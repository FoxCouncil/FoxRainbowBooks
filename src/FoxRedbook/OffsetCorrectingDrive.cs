using System.Buffers;

namespace FoxRedbook;

/// <summary>
/// Decorator that transparently applies sample-level read offset correction
/// to an underlying <see cref="IOpticalDrive"/>. Every drive model has a fixed
/// offset (in sample frames) that shifts audio data relative to the nominal LBA.
/// This decorator compensates by adjusting the read position and shifting the
/// returned data.
/// </summary>
/// <remarks>
/// <para>
/// The offset is measured in sample frames (4 bytes each: 2 channels × 16-bit).
/// A positive offset means the drive reads ahead of the true disc position, so we
/// need to shift our reads backward (to earlier LBAs) to compensate. A negative
/// offset means the drive reads behind, so we shift forward.
/// </para>
/// <para>
/// At disc edges (before LBA 0 or past lead-out), the shifted region that falls
/// outside the readable area is zero-padded. This matches the physical reality:
/// the pregap before track 1 and the silence after the last track are typically
/// digital silence, and a few samples of zeros at the boundary are correct behavior.
/// </para>
/// </remarks>
public sealed class OffsetCorrectingDrive : IOpticalDrive
{
    private readonly IOpticalDrive _inner;
    private readonly int _offsetSamples;
    private readonly int _offsetBytes;
    private bool _disposed;

    /// <summary>
    /// Wraps an existing drive with offset correction.
    /// </summary>
    /// <param name="inner">The underlying drive to wrap. Disposed when this instance is disposed.</param>
    /// <param name="offsetSamples">
    /// Offset in sample frames (4 bytes each). Positive means the drive reads ahead
    /// of the true position (reads are shifted backward); negative means behind
    /// (shifted forward).
    /// </param>
    public OffsetCorrectingDrive(IOpticalDrive inner, int offsetSamples)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _offsetSamples = offsetSamples;
        _offsetBytes = offsetSamples * CdConstants.BytesPerSampleFrame;
    }

    /// <inheritdoc />
    public DriveInquiry Inquiry => _inner.Inquiry;

    /// <inheritdoc />
    public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.ReadTocAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.ReadCdTextAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> ReadSectorsAsync(
        long lba,
        int count,
        Memory<byte> buffer,
        ReadOptions flags = ReadOptions.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int requiredSize = CdConstants.GetReadBufferSize(flags, count);

        if (buffer.Length < requiredSize)
        {
            throw new ArgumentException(
                $"Buffer too small: {buffer.Length} bytes provided, {requiredSize} required for {count} sectors with flags {flags}.",
                nameof(buffer));
        }

        // No offset — pass through directly
        if (_offsetSamples == 0)
        {
            return await _inner.ReadSectorsAsync(lba, count, buffer, flags, cancellationToken).ConfigureAwait(false);
        }

        // Calculate how the offset shifts the byte range we need.
        // We're correcting the drive's offset, so we read from an earlier/later
        // position and then shift the data.
        //
        // If offset is +6 samples (+24 bytes), the drive reads 24 bytes ahead.
        // To get the correct data for our requested LBA, we need to read starting
        // 24 bytes earlier, then take the data from byte 24 onward.

        int audioPerSector = CdConstants.SectorSize;
        int totalAudioBytes = count * audioPerSector;

        // How many extra bytes we need from the shifted read
        int absOffsetBytes = Math.Abs(_offsetBytes);

        // We need to read enough sectors to cover the requested range plus the offset
        int extraSectorsNeeded = (absOffsetBytes + audioPerSector - 1) / audioPerSector;
        int readCount = count + extraSectorsNeeded;

        // The LBA to start reading from (shifted by offset)
        long readLba;
        int skipBytes;

        if (_offsetSamples > 0)
        {
            // Drive reads ahead: read from earlier LBAs, skip the first offsetBytes
            readLba = lba - extraSectorsNeeded;
            skipBytes = extraSectorsNeeded * audioPerSector - _offsetBytes;
        }
        else
        {
            // Drive reads behind: read from later LBAs, skip from the start
            readLba = lba;
            readCount = count + extraSectorsNeeded;
            skipBytes = -_offsetBytes;
        }

        // Allocate a temporary read buffer for the extended range.
        // Only audio data — we don't offset-correct C2 or subchannel.
        int readBufferSize = CdConstants.GetReadBufferSize(flags, readCount);
        byte[] tempBuffer = ArrayPool<byte>.Shared.Rent(readBufferSize);

        try
        {
            // Handle reads that would go before LBA 0
            int leadingZeroSectors = 0;
            long actualReadLba = readLba;
            int actualReadCount = readCount;

            if (readLba < 0)
            {
                leadingZeroSectors = (int)Math.Min(-readLba, readCount);
                actualReadLba = 0;
                actualReadCount = readCount - leadingZeroSectors;
            }

            int perSectorSize = CdConstants.GetReadBufferSize(flags, 1);

            // Zero-fill the leading sectors that are before LBA 0
            if (leadingZeroSectors > 0)
            {
                tempBuffer.AsSpan(0, leadingZeroSectors * perSectorSize).Clear();
            }

            // Read the available sectors from the drive
            int sectorsRead = 0;

            if (actualReadCount > 0)
            {
                int readOffset = leadingZeroSectors * perSectorSize;
                int readSize = CdConstants.GetReadBufferSize(flags, actualReadCount);

                sectorsRead = await _inner.ReadSectorsAsync(
                    actualReadLba, actualReadCount,
                    tempBuffer.AsMemory(readOffset, readSize),
                    flags, cancellationToken).ConfigureAwait(false);

                // Zero-fill any sectors the drive couldn't provide (past lead-out)
                int shortfall = actualReadCount - sectorsRead;

                if (shortfall > 0)
                {
                    int zeroStart = readOffset + sectorsRead * perSectorSize;
                    int zeroLength = shortfall * perSectorSize;
                    tempBuffer.AsSpan(zeroStart, zeroLength).Clear();
                }
            }

            int totalSectorsAvailable = leadingZeroSectors + Math.Max(sectorsRead, actualReadCount);

            // Now extract the offset-corrected data for each requested sector.
            // For audio-only reads, we can do a bulk copy. For reads with C2/subchannel,
            // we need to handle per-sector layout: [audio | C2 | subchannel].
            bool hasExtraData = (flags & (ReadOptions.C2ErrorPointers | ReadOptions.SubchannelData)) != 0;
            Span<byte> output = buffer.Span;

            if (!hasExtraData)
            {
                // Simple case: audio only. Bulk-copy with byte offset.
                int availableAudio = totalSectorsAvailable * audioPerSector;
                int copyLength = Math.Min(totalAudioBytes, availableAudio - skipBytes);

                if (copyLength > 0)
                {
                    tempBuffer.AsSpan(skipBytes, copyLength).CopyTo(output);
                }

                // Zero-pad if we couldn't fill the entire request
                if (copyLength < totalAudioBytes)
                {
                    output.Slice(copyLength, totalAudioBytes - copyLength).Clear();
                }
            }
            else
            {
                // Complex case: per-sector layout with C2/subchannel.
                // Offset correction only applies to audio data. C2 and subchannel
                // correspond to the originally-requested LBAs (the drive reports
                // C2 errors for the sectors it actually read, not our shifted view).
                // For simplicity, we zero-fill C2 and subchannel in the offset case.
                int c2Size = (flags & ReadOptions.C2ErrorPointers) != 0 ? CdConstants.C2ErrorPointerSize : 0;
                int subSize = (flags & ReadOptions.SubchannelData) != 0 ? CdConstants.SubchannelSize : 0;

                for (int i = 0; i < count; i++)
                {
                    int outStart = i * perSectorSize;
                    int srcAudioStart = skipBytes + i * audioPerSector;

                    // Copy offset-corrected audio
                    if (srcAudioStart >= 0 && srcAudioStart + audioPerSector <= totalSectorsAvailable * audioPerSector)
                    {
                        tempBuffer.AsSpan(srcAudioStart, audioPerSector).CopyTo(output.Slice(outStart));
                    }
                    else
                    {
                        output.Slice(outStart, audioPerSector).Clear();
                    }

                    // Zero-fill C2 and subchannel
                    int extraStart = outStart + audioPerSector;

                    if (c2Size > 0)
                    {
                        output.Slice(extraStart, c2Size).Clear();
                        extraStart += c2Size;
                    }

                    if (subSize > 0)
                    {
                        output.Slice(extraStart, subSize).Clear();
                    }
                }
            }

            return count;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _inner.Dispose();
            _disposed = true;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            return _inner.DisposeAsync();
        }

        return ValueTask.CompletedTask;
    }
}
