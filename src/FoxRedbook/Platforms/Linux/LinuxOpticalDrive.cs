using System.Buffers;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using FoxRedbook.Platforms.Common;

namespace FoxRedbook.Platforms.Linux;

/// <summary>
/// Linux implementation of <see cref="IOpticalDrive"/> using the kernel's
/// SCSI generic (sg) passthrough interface via <c>ioctl(fd, SG_IO, ...)</c>.
/// Opens a block device path like <c>/dev/sr0</c>, issues INQUIRY / READ TOC /
/// READ CD commands directly, and returns parsed responses.
/// </summary>
/// <remarks>
/// <para>
/// Indirect I/O mode is used (flags = 0, no SG_FLAG_DIRECT_IO) so the kernel
/// handles buffer alignment for us. At CD read speeds the memcpy overhead is
/// negligible compared to the drive's mechanical limits.
/// </para>
/// <para>
/// The file descriptor is opened with <c>O_RDONLY | O_NONBLOCK</c> so the
/// open call returns immediately instead of blocking on drive spin-up.
/// Subsequent SCSI commands will return sense data (MediaNotPresent or
/// similar) when the drive is not ready, which we map to the typed
/// exception hierarchy from step 1.
/// </para>
/// </remarks>
[SupportedOSPlatform("linux")]
public sealed class LinuxOpticalDrive : IOpticalDrive
{
    private const uint DefaultTimeoutMs = 30_000;
    private const int SenseBufferSize = 18; // fixed-format sense data

    private readonly SafeFileDescriptorHandle _fd;
    private DriveInquiry? _cachedInquiry;
    private bool _disposed;

    /// <summary>
    /// Opens the given block device (e.g., <c>/dev/sr0</c>) and returns an
    /// <see cref="IOpticalDrive"/> backed by SCSI generic passthrough.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="devicePath"/> is null.</exception>
    /// <exception cref="OpticalDriveException">
    /// The device could not be opened (not found, permission denied, etc.).
    /// The underlying errno is included in the message.
    /// </exception>
    public LinuxOpticalDrive(string devicePath)
    {
        ArgumentNullException.ThrowIfNull(devicePath);

        int flags = SgIoNative.O_RDONLY | SgIoNative.O_NONBLOCK;
        int fd = SgIoNative.Open(devicePath, flags);

        if (fd < 0)
        {
            int errno = Marshal.GetLastPInvokeError();
            throw new OpticalDriveException(
                $"Failed to open '{devicePath}': errno {errno}.");
        }

        _fd = new SafeFileDescriptorHandle(fd);
    }

    /// <inheritdoc />
    public DriveInquiry Inquiry
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _cachedInquiry ??= QueryInquiry();
        }
    }

    /// <inheritdoc />
    public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] cdb = new byte[10];
        ScsiCommands.BuildReadToc(cdb);

        byte[] response = ArrayPool<byte>.Shared.Rent(ScsiCommands.ReadTocMaxAllocationLength);

        try
        {
            ExecuteScsiCommand(
                cdb,
                response.AsSpan(0, ScsiCommands.ReadTocMaxAllocationLength),
                SgIoNative.SG_DXFER_FROM_DEV);

            TableOfContents toc = ScsiCommands.ParseReadTocResponse(
                response.AsSpan(0, ScsiCommands.ReadTocMaxAllocationLength));

            return Task.FromResult(toc);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    /// <inheritdoc />
    public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] cdb = new byte[10];
        CdTextCommands.BuildReadCdText(cdb);

        // 65 KB is more than enough — the theoretical max is 36 KB.
        const int CdTextBufferSize = 65536;
        byte[] response = ArrayPool<byte>.Shared.Rent(CdTextBufferSize);

        try
        {
            try
            {
                ExecuteScsiCommand(
                    cdb,
                    response.AsSpan(0, CdTextBufferSize),
                    SgIoNative.SG_DXFER_FROM_DEV);
            }
            catch (OpticalDriveException)
            {
                // Drive returned an error — most commonly "format 5 not supported"
                // on discs without CD-Text. Surface as "no CD-Text present."
                return Task.FromResult<CdText?>(null);
            }

            CdText? cdText = CdTextCommands.ParseCdText(response.AsSpan(0, CdTextBufferSize));
            return Task.FromResult(cdText);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(response);
        }
    }

    /// <inheritdoc />
    public Task<int> ReadSectorsAsync(
        long lba,
        int count,
        Memory<byte> buffer,
        ReadOptions flags = ReadOptions.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        int requiredSize = CdConstants.GetReadBufferSize(flags, count);

        if (buffer.Length < requiredSize)
        {
            throw new ArgumentException(
                $"Buffer too small: {buffer.Length} bytes provided, {requiredSize} required.",
                nameof(buffer));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(lba);

        if (count <= 0)
        {
            return Task.FromResult(0);
        }

        byte[] cdb = new byte[12];
        ScsiCommands.BuildReadCd(cdb, lba, count, flags);

        ExecuteScsiCommand(cdb, buffer.Span.Slice(0, requiredSize), SgIoNative.SG_DXFER_FROM_DEV);

        return Task.FromResult(count);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _fd.Dispose();
            _disposed = true;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Internals ──────────────────────────────────────────────

    private DriveInquiry QueryInquiry()
    {
        byte[] cdb = new byte[6];
        ScsiCommands.BuildInquiry(cdb);

        byte[] response = new byte[ScsiCommands.InquiryResponseLength];
        ExecuteScsiCommand(cdb, response.AsSpan(), SgIoNative.SG_DXFER_FROM_DEV);

        return ScsiCommands.ParseInquiry(response);
    }

    /// <summary>
    /// Executes a single SCSI command via SG_IO. Pins the caller's CDB,
    /// data buffer, and sense buffer; issues the ioctl; and either returns
    /// cleanly or throws an <see cref="OpticalDriveException"/> derived
    /// from the sense data or kernel error fields.
    /// </summary>
    private unsafe void ExecuteScsiCommand(
        ReadOnlySpan<byte> cdb,
        Span<byte> dataBuffer,
        int dxferDirection)
    {
        Span<byte> senseBuffer = stackalloc byte[SenseBufferSize];

        fixed (byte* cdbPtr = cdb)
        fixed (byte* dataPtr = dataBuffer)
        fixed (byte* sensePtr = senseBuffer)
        {
            var hdr = new SgIoHdr
            {
                InterfaceId = SgIoNative.SG_INTERFACE_ID_ORIG,
                DxferDirection = dxferDirection,
                CmdLen = (byte)cdb.Length,
                MxSbLen = SenseBufferSize,
                IovecCount = 0,
                DxferLen = (uint)dataBuffer.Length,
                Dxferp = (IntPtr)dataPtr,
                Cmdp = (IntPtr)cdbPtr,
                Sbp = (IntPtr)sensePtr,
                Timeout = DefaultTimeoutMs,
                Flags = 0, // indirect I/O, no alignment requirement
            };

            int result = SgIoNative.Ioctl(_fd.FileDescriptor, SgIoNative.SG_IO, &hdr);

            if (result < 0)
            {
                int errno = Marshal.GetLastPInvokeError();
                throw new OpticalDriveException(
                    $"SG_IO ioctl failed with errno {errno}.");
            }

            // Success requires status/host_status/driver_status/sb_len_wr all zero.
            bool success = hdr.Status == 0
                && hdr.HostStatus == 0
                && hdr.DriverStatus == 0
                && hdr.SbLenWr == 0;

            if (!success)
            {
                if (hdr.SbLenWr > 0)
                {
                    int senseLen = Math.Min((int)hdr.SbLenWr, SenseBufferSize);
                    throw ScsiCommands.MapSenseData(senseBuffer.Slice(0, senseLen));
                }

                throw new OpticalDriveException(
                    $"SCSI command failed: status=0x{hdr.Status:X2}, " +
                    $"host_status=0x{hdr.HostStatus:X4}, driver_status=0x{hdr.DriverStatus:X4}.");
            }
        }
    }
}
