using System.Buffers;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using FoxRedbook.Platforms.Common;
using Microsoft.Win32.SafeHandles;

namespace FoxRedbook.Platforms.Windows;

/// <summary>
/// Windows implementation of <see cref="IOpticalDrive"/> using
/// <c>IOCTL_SCSI_PASS_THROUGH_DIRECT</c> via <c>DeviceIoControl</c>.
/// Opens an optical drive device path like <c>D:</c> or <c>\\.\CdRom0</c>,
/// issues INQUIRY / READ TOC / READ CD commands directly, and returns
/// parsed responses.
/// </summary>
/// <remarks>
/// <para>
/// The CDB builders, response parsers, and sense-data mapping all come
/// from <see cref="ScsiCommands"/>, which is filed under the Linux
/// platform folder but is pure-function and platform-agnostic. No
/// code duplication between the Windows and Linux backends.
/// </para>
/// <para>
/// <b>LOAD-BEARING: the synchronous DeviceIoControl design is intentional.</b>
/// This class uses a synchronous <c>DeviceIoControl</c> call without
/// <c>FILE_FLAG_OVERLAPPED</c>, just like the Linux backend uses
/// synchronous <c>ioctl(SG_IO)</c>. The async shape at the
/// <see cref="IOpticalDrive.ReadSectorsAsync"/> boundary is implemented
/// via <see cref="Task.FromResult{TResult}"/>. Switching to true
/// asynchronous I/O with overlapped completion ports would require
/// re-opening the device with <c>FILE_FLAG_OVERLAPPED</c>, allocating
/// an <c>OVERLAPPED</c> structure per call with an event handle or
/// threadpool completion binding, threading that through the SafeHandle
/// lifecycle, and rewriting error handling to account for both the
/// synchronous error path (BOOL return) and the pending-completion
/// path (ERROR_IO_PENDING). It is a multi-day rewrite with no benefit
/// at CD read rates (1-7 MB/s, sub-millisecond kernel latency per
/// ioctl). Do not "improve" this without a concrete justification.
/// </para>
/// </remarks>
[SupportedOSPlatform("windows")]
public sealed class WindowsOpticalDrive : IOpticalDrive, IScsiTransport
{
    private const uint DefaultTimeoutSeconds = 30;

    private readonly SafeFileHandle _handle;
    private DriveInquiry? _cachedInquiry;
    private bool _disposed;

    /// <summary>
    /// Opens the given optical drive and returns an <see cref="IOpticalDrive"/>
    /// backed by <c>IOCTL_SCSI_PASS_THROUGH_DIRECT</c>.
    /// </summary>
    /// <param name="devicePath">
    /// Drive letter (<c>D:</c>), device-namespace drive letter (<c>\\.\D:</c>),
    /// or device object name (<c>\\.\CdRom0</c>). Normalized internally.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="devicePath"/> is null.</exception>
    /// <exception cref="ArgumentException">Path is not a recognized form.</exception>
    /// <exception cref="OpticalDriveException">
    /// CreateFile failed — the Win32 error code is included in the message.
    /// </exception>
    public WindowsOpticalDrive(string devicePath)
    {
        ArgumentNullException.ThrowIfNull(devicePath);

        string normalized = WindowsDevicePath.Normalize(devicePath);

        // GENERIC_READ alone is NOT sufficient. IOCTL_SCSI_PASS_THROUGH_DIRECT's
        // CTL_CODE includes FILE_READ_ACCESS | FILE_WRITE_ACCESS, and the
        // Windows I/O manager requires the file handle to satisfy both bits.
        // Opening with only GENERIC_READ produces ERROR_ACCESS_DENIED on
        // every DeviceIoControl call — a gotcha that's easy to miss until
        // the first hardware test fails with a misleading error.
        uint access = Win32Native.GENERIC_READ | Win32Native.GENERIC_WRITE;
        uint share = Win32Native.FILE_SHARE_READ | Win32Native.FILE_SHARE_WRITE;

        SafeFileHandle handle = Win32Native.CreateFile(
            normalized,
            access,
            share,
            lpSecurityAttributes: 0,
            Win32Native.OPEN_EXISTING,
            dwFlagsAndAttributes: 0,
            hTemplateFile: 0);

        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new OpticalDriveException(
                $"Failed to open '{normalized}': Win32 error {error}.");
        }

        _handle = handle;
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
                Win32Native.SCSI_IOCTL_DATA_IN);

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

        const int CdTextBufferSize = 65536;
        byte[] response = ArrayPool<byte>.Shared.Rent(CdTextBufferSize);

        try
        {
            try
            {
                ExecuteScsiCommand(
                    cdb,
                    response.AsSpan(0, CdTextBufferSize),
                    Win32Native.SCSI_IOCTL_DATA_IN);
            }
            catch (OpticalDriveException)
            {
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

        ExecuteScsiCommand(cdb, buffer.Span.Slice(0, requiredSize), Win32Native.SCSI_IOCTL_DATA_IN);

        return Task.FromResult(count);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _handle.Dispose();
            _disposed = true;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        byte dataIn = direction switch
        {
            ScsiDirection.None => Win32Native.SCSI_IOCTL_DATA_UNSPECIFIED,
            ScsiDirection.In => Win32Native.SCSI_IOCTL_DATA_IN,
            ScsiDirection.Out => Win32Native.SCSI_IOCTL_DATA_OUT,
            _ => throw new ArgumentOutOfRangeException(nameof(direction)),
        };

        ExecuteScsiCommand(cdb, buffer, dataIn);
    }

    // ── Internals ──────────────────────────────────────────────

    private DriveInquiry QueryInquiry()
    {
        byte[] cdb = new byte[6];
        ScsiCommands.BuildInquiry(cdb);

        byte[] response = new byte[ScsiCommands.InquiryResponseLength];
        ExecuteScsiCommand(cdb, response.AsSpan(), Win32Native.SCSI_IOCTL_DATA_IN);

        return ScsiCommands.ParseInquiry(response);
    }

    /// <summary>
    /// Executes a single SCSI command via IOCTL_SCSI_PASS_THROUGH_DIRECT.
    /// Builds the wrapped-buffer struct, pins the caller's data buffer,
    /// issues the ioctl, and either returns cleanly or throws an
    /// <see cref="OpticalDriveException"/> derived from the sense data
    /// or Win32 error.
    /// </summary>
    private unsafe void ExecuteScsiCommand(
        ReadOnlySpan<byte> cdb,
        Span<byte> dataBuffer,
        byte dataIn)
    {
        var wrapper = new ScsiPassThroughDirectWithBuffer();
        wrapper.Spt.Length = (ushort)Marshal.SizeOf<ScsiPassThroughDirect>();
        wrapper.Spt.PathId = 0;
        wrapper.Spt.TargetId = 0;
        wrapper.Spt.Lun = 0;
        wrapper.Spt.CdbLength = (byte)cdb.Length;
        wrapper.Spt.SenseInfoLength = 32; // matches SenseBuffer inline array size
        wrapper.Spt.DataIn = dataIn;
        wrapper.Spt.DataTransferLength = (uint)dataBuffer.Length;
        wrapper.Spt.TimeOutValue = DefaultTimeoutSeconds;
        wrapper.Spt.SenseInfoOffset = (uint)Marshal.OffsetOf<ScsiPassThroughDirectWithBuffer>(nameof(ScsiPassThroughDirectWithBuffer.SenseBuf));

        // Copy the CDB into the inline array.
        for (int i = 0; i < cdb.Length; i++)
        {
            wrapper.Spt.Cdb[i] = cdb[i];
        }

        fixed (byte* dataPtr = dataBuffer)
        {
            wrapper.Spt.DataBuffer = (nint)dataPtr;

            uint bytesReturned;
            uint wrapperSize = (uint)Marshal.SizeOf<ScsiPassThroughDirectWithBuffer>();

            bool success = Win32Native.DeviceIoControl(
                _handle,
                Win32Native.IOCTL_SCSI_PASS_THROUGH_DIRECT,
                &wrapper,
                wrapperSize,
                &wrapper,
                wrapperSize,
                out bytesReturned,
                lpOverlapped: 0);

            if (!success)
            {
                int error = Marshal.GetLastPInvokeError();

                // ERROR_NOT_READY from DeviceIoControl itself (before the
                // device even responds with sense data) means no media is
                // in the drive. Map directly without parsing sense.
                if (error == Win32Native.ERROR_NOT_READY)
                {
                    throw new MediaNotPresentException(
                        "No media in drive (DeviceIoControl returned ERROR_NOT_READY).");
                }

                if (error == Win32Native.ERROR_ACCESS_DENIED)
                {
                    throw new OpticalDriveException(
                        "DeviceIoControl returned ERROR_ACCESS_DENIED. " +
                        "The device handle must be opened with GENERIC_READ | GENERIC_WRITE " +
                        "to satisfy the IOCTL's access bits.");
                }

                throw new OpticalDriveException(
                    $"DeviceIoControl failed with Win32 error {error}.");
            }

            // Success return value, but the device may still have returned
            // a CHECK CONDITION with sense data. ScsiStatus == 0x02 signals
            // this, and the sense buffer in the wrapper contains the details.
            if (wrapper.Spt.ScsiStatus != 0)
            {
                // The inline array's ReadOnlySpan view gives us direct access
                // to the sense bytes without another copy.
                ReadOnlySpan<byte> senseSpan = wrapper.SenseBuf;
                throw ScsiCommands.MapSenseData(senseSpan);
            }
        }
    }
}
