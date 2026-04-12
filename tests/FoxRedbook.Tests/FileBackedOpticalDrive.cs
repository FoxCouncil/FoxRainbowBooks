using Microsoft.Win32.SafeHandles;

namespace FoxRedbook.Tests;

/// <summary>
/// An <see cref="IOpticalDrive"/> implementation backed by a raw binary file on disk.
/// Sectors are laid out contiguously: sector at LBA N starts at byte offset
/// <c>N × <see cref="CdConstants.SectorSize"/></c>.
/// </summary>
/// <remarks>
/// <para>
/// This is a test fixture, not a shipping feature. It uses <see cref="RandomAccess"/>
/// for thread-safe positional reads — concurrent <see cref="ReadSectorsAsync"/> calls
/// are safe without locking.
/// </para>
/// <para>
/// Error simulation is driven by an optional <see cref="ReadErrorProfile"/>.
/// Fault application order per sector:
/// <list type="number">
///   <item>Transient faults — throw before touching the buffer.</item>
///   <item>Jitter faults — redirect which file offset is read.</item>
///   <item>Bit flip faults — corrupt bytes in the returned audio data.</item>
///   <item>C2 error faults — set bits in the C2 error pointer block.</item>
/// </list>
/// Per-sector attempt counts are tracked internally. Use <see cref="ResetAttemptCounts"/>
/// to reset for multi-phase tests.
/// </para>
/// </remarks>
public sealed class FileBackedOpticalDrive : IOpticalDrive
{
    private readonly SafeFileHandle _handle;
    private readonly TableOfContents _toc;
    private readonly ReadErrorProfile? _errorProfile;
    private readonly CdText? _cdText;
    private readonly Dictionary<long, int> _attemptCounts = new();
    private bool _disposed;

    /// <summary>
    /// Opens a binary file as a virtual optical drive.
    /// </summary>
    /// <param name="filePath">
    /// Path to a raw binary file containing contiguous sector data starting at LBA 0.
    /// Must be at least <c>toc.LeadOutLba × 2352</c> bytes.
    /// </param>
    /// <param name="toc">Pre-built Table of Contents describing the disc layout.</param>
    /// <param name="inquiry">
    /// Drive identification. Defaults to Vendor="TEST", Product="FileBackedDrive", Revision="1.0".
    /// </param>
    /// <param name="errorProfile">
    /// Optional error simulation profile. When null, all reads return clean data.
    /// </param>
    /// <param name="cdText">
    /// Optional CD-Text payload. When null, <see cref="ReadCdTextAsync"/> returns null
    /// (simulating a disc with no CD-Text data).
    /// </param>
    public FileBackedOpticalDrive(
        string filePath,
        TableOfContents toc,
        DriveInquiry? inquiry = null,
        ReadErrorProfile? errorProfile = null,
        CdText? cdText = null)
    {
        ArgumentNullException.ThrowIfNull(filePath);
        ArgumentNullException.ThrowIfNull(toc);

        _handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _toc = toc;
        _errorProfile = errorProfile;
        _cdText = cdText;
        Inquiry = inquiry ?? new DriveInquiry
        {
            Vendor = "TEST",
            Product = "FileBackedDrive",
            Revision = "1.0",
        };

        long requiredBytes = toc.LeadOutLba * CdConstants.SectorSize;
        long actualLength = RandomAccess.GetLength(_handle);

        if (actualLength < requiredBytes)
        {
            _handle.Dispose();
            throw new ArgumentException(
                $"Backing file is {actualLength} bytes but TOC requires at least {requiredBytes} bytes ({toc.LeadOutLba} sectors).",
                nameof(filePath));
        }
    }

    /// <inheritdoc />
    public DriveInquiry Inquiry { get; }

    /// <inheritdoc />
    public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_toc);
    }

    /// <inheritdoc />
    public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_cdText);
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
                $"Buffer too small: {buffer.Length} bytes provided, {requiredSize} required for {count} sectors with flags {flags}.",
                nameof(buffer));
        }

        if (lba < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(lba), lba, "LBA must be non-negative.");
        }

        int availableSectors = (int)Math.Max(0, Math.Min(count, _toc.LeadOutLba - lba));

        if (availableSectors <= 0)
        {
            return Task.FromResult(0);
        }

        bool includeC2 = (flags & ReadOptions.C2ErrorPointers) != 0;
        bool includeSub = (flags & ReadOptions.SubchannelData) != 0;
        int perSectorSize = CdConstants.SectorSize
            + (includeC2 ? CdConstants.C2ErrorPointerSize : 0)
            + (includeSub ? CdConstants.SubchannelSize : 0);

        // Phase 1: Increment attempt counts for all sectors in the range
        for (int i = 0; i < availableSectors; i++)
        {
            long currentLba = lba + i;
            _attemptCounts.TryGetValue(currentLba, out int current);
            _attemptCounts[currentLba] = current + 1;
        }

        // Phase 2: Check transient faults — throw before touching the buffer
        if (_errorProfile is not null)
        {
            for (int i = 0; i < availableSectors; i++)
            {
                long currentLba = lba + i;
                int attempt = _attemptCounts[currentLba];
                CheckTransientFaults(currentLba, attempt);
            }
        }

        // Phase 3: Read and process each sector
        Span<byte> output = buffer.Span;

        for (int i = 0; i < availableSectors; i++)
        {
            long currentLba = lba + i;
            int attempt = _attemptCounts[currentLba];
            int sectorStart = i * perSectorSize;

            Span<byte> audioSlice = output.Slice(sectorStart, CdConstants.SectorSize);

            // Determine the byte offset in the backing file (jitter may shift it)
            long fileOffset = currentLba * CdConstants.SectorSize;

            if (_errorProfile is not null)
            {
                fileOffset = ApplyJitter(currentLba, attempt, fileOffset);
            }

            // Read audio data — clear first so short reads (from jitter near
            // file edges) leave zeros rather than stale data
            audioSlice.Clear();

            if (fileOffset >= 0)
            {
                RandomAccess.Read(_handle, audioSlice, fileOffset);
            }

            // Apply bit flips to the returned data (post-jitter)
            if (_errorProfile is not null)
            {
                ApplyBitFlips(currentLba, attempt, audioSlice);
            }

            // Generate C2 error pointers if requested
            if (includeC2)
            {
                Span<byte> c2Slice = output.Slice(
                    sectorStart + CdConstants.SectorSize,
                    CdConstants.C2ErrorPointerSize);
                c2Slice.Clear();

                if (_errorProfile is not null)
                {
                    ApplyC2Errors(currentLba, c2Slice);
                }
            }

            // Subchannel data — all zeros, not simulated
            if (includeSub)
            {
                int subOffset = sectorStart + CdConstants.SectorSize
                    + (includeC2 ? CdConstants.C2ErrorPointerSize : 0);
                output.Slice(subOffset, CdConstants.SubchannelSize).Clear();
            }
        }

        return Task.FromResult(availableSectors);
    }

    /// <summary>
    /// Resets per-sector attempt counts to zero. Call this between test phases
    /// to re-arm faults without reconstructing the drive.
    /// </summary>
    public void ResetAttemptCounts()
    {
        _attemptCounts.Clear();
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

    private void CheckTransientFaults(long lba, int attempt)
    {
        foreach (var fault in _errorProfile!.TransientFailures)
        {
            if (lba >= fault.StartLba && lba < fault.EndLba && attempt <= fault.FailureCount)
            {
                throw new OpticalDriveException(
                    $"Simulated transient read error at LBA {lba} (attempt {attempt} of {fault.FailureCount}).");
            }
        }
    }

    private long ApplyJitter(long lba, int attempt, long fileOffset)
    {
        foreach (var fault in _errorProfile!.JitterFaults)
        {
            if (lba >= fault.StartLba && lba < fault.EndLba && attempt <= fault.JitterReads)
            {
                return fileOffset + (long)fault.SampleFrameShift * CdConstants.BytesPerSampleFrame;
            }
        }

        return fileOffset;
    }

    private void ApplyBitFlips(long lba, int attempt, Span<byte> audioData)
    {
        foreach (var fault in _errorProfile!.BitFlips)
        {
            if (lba >= fault.StartLba && lba < fault.EndLba && attempt <= fault.CorruptReads)
            {
                audioData[fault.ByteOffset] ^= fault.XorMask;
            }
        }
    }

    private void ApplyC2Errors(long lba, Span<byte> c2Data)
    {
        foreach (var fault in _errorProfile!.C2Errors)
        {
            if (lba >= fault.StartLba && lba < fault.EndLba)
            {
                int end = Math.Min(fault.ByteOffset + fault.ByteCount, CdConstants.SectorSize);

                for (int b = fault.ByteOffset; b < end; b++)
                {
                    c2Data[b / 8] |= (byte)(1 << (b % 8));
                }
            }
        }
    }
}
