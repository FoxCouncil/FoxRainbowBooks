namespace FoxOrangebook.FileSystem;

/// <summary>
/// A placed region of the image: a run of logical sectors at a known LBA whose
/// content is either an in-memory byte array (descriptors, path tables,
/// directory extents) or a lazily-opened file source (file data). Content
/// shorter than the sector run is zero-padded to the sector boundary.
/// </summary>
internal readonly struct PlacedRegion
{
    public PlacedRegion(long lba, byte[] bytes)
    {
        Lba = lba;
        Bytes = bytes;
        Source = null;
        DataLength = bytes.Length;
    }

    public PlacedRegion(long lba, IContentSource source)
    {
        Lba = lba;
        Bytes = null;
        Source = source;
        DataLength = source.Length;
    }

    public long Lba { get; }
    public long DataLength { get; }
    public byte[]? Bytes { get; }
    public IContentSource? Source { get; }

    public long SectorCount => IsoConstants.SectorsFor(DataLength);
}

/// <summary>
/// A finished data-disc filesystem image: a contiguous run of 2048-byte logical
/// sectors. Write it to a <c>.iso</c> file with <see cref="WriteToFileAsync"/>,
/// or stream the raw sectors with <see cref="OpenRead"/> (for example, to feed
/// a data-disc burn).
/// </summary>
public sealed class DiscImage
{
    private readonly Segment[] _segments;

    internal DiscImage(long sectorCount, List<PlacedRegion> regions)
    {
        SectorCount = sectorCount;
        _segments = BuildSegments(sectorCount, regions);
    }

    /// <summary>Size of one logical sector in bytes (always 2048).</summary>
    public const int LogicalSectorSize = IsoConstants.LogicalSectorSize;

    /// <summary>Total number of 2048-byte logical sectors in the image.</summary>
    public long SectorCount { get; }

    /// <summary>Total length of the image in bytes.</summary>
    public long ByteLength => SectorCount * LogicalSectorSize;

    /// <summary>
    /// Opens a seekable, read-only stream over the entire image. Multiple
    /// independent streams may be opened concurrently.
    /// </summary>
    public Stream OpenRead()
    {
        return new DiscImageStream(_segments, ByteLength);
    }

    /// <summary>Writes the entire image to <paramref name="output"/> sequentially.</summary>
    public void WriteTo(Stream output)
    {
        ArgumentNullException.ThrowIfNull(output);

        Span<byte> zero = stackalloc byte[IsoConstants.LogicalSectorSize];
        zero.Clear();

        foreach (var seg in _segments)
        {
            if (seg.Source is null && seg.Bytes is null)
            {
                WriteZeros(output, seg.Length, zero);
                continue;
            }

            if (seg.Bytes is not null)
            {
                output.Write(seg.Bytes);
                WriteZeros(output, seg.Length - seg.Bytes.Length, zero);
                continue;
            }

            using var src = seg.Source!.Open();
            src.CopyTo(output);
            WriteZeros(output, seg.Length - seg.DataLength, zero);
        }
    }

    /// <summary>Writes the entire image to a file at <paramref name="path"/> (typically a <c>.iso</c>).</summary>
    public async Task WriteToFileAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);

        var output = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16, useAsync: true);

        await using (output.ConfigureAwait(false))
        {
            await WriteToAsync(output, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Writes the entire image to <paramref name="output"/> sequentially (asynchronously).</summary>
    public async Task WriteToAsync(Stream output, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(output);

        var zero = new byte[IsoConstants.LogicalSectorSize];

        foreach (var seg in _segments)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (seg.Bytes is not null)
            {
                await output.WriteAsync(seg.Bytes, cancellationToken).ConfigureAwait(false);
                await WriteZerosAsync(output, seg.Length - seg.Bytes.Length, zero, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (seg.Source is not null)
            {
                var src = seg.Source.Open();

                await using (src.ConfigureAwait(false))
                {
                    await src.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
                }

                await WriteZerosAsync(output, seg.Length - seg.DataLength, zero, cancellationToken).ConfigureAwait(false);
                continue;
            }

            await WriteZerosAsync(output, seg.Length, zero, cancellationToken).ConfigureAwait(false);
        }
    }

    // ── Segment table ────────────────────────────────────────────

    private static Segment[] BuildSegments(long sectorCount, List<PlacedRegion> regions)
    {
        var placed = regions.FindAll(r => r.DataLength > 0);
        placed.Sort((a, b) => a.Lba.CompareTo(b.Lba));

        var segments = new List<Segment>(placed.Count * 2 + 2);
        long cursorSector = 0;

        foreach (var region in placed)
        {
            if (region.Lba < cursorSector)
            {
                throw new InvalidOperationException($"Image layout error: region at LBA {region.Lba} overlaps the sector at {cursorSector}.");
            }

            if (region.Lba > cursorSector)
            {
                segments.Add(Segment.Zero((region.Lba - cursorSector) * IsoConstants.LogicalSectorSize));
            }

            long total = region.SectorCount * IsoConstants.LogicalSectorSize;
            segments.Add(region.Bytes is not null ? Segment.FromBytes(region.Bytes, total) : Segment.FromSource(region.Source!, region.DataLength, total));
            cursorSector = region.Lba + region.SectorCount;
        }

        long tail = sectorCount - cursorSector;

        if (tail > 0)
        {
            segments.Add(Segment.Zero(tail * IsoConstants.LogicalSectorSize));
        }

        return segments.ToArray();
    }

    private static void WriteZeros(Stream output, long count, Span<byte> zero)
    {
        while (count > 0)
        {
            int chunk = (int)Math.Min(count, zero.Length);
            output.Write(zero.Slice(0, chunk));
            count -= chunk;
        }
    }

    private static async Task WriteZerosAsync(Stream output, long count, byte[] zero, CancellationToken cancellationToken)
    {
        while (count > 0)
        {
            int chunk = (int)Math.Min(count, zero.Length);
            await output.WriteAsync(zero.AsMemory(0, chunk), cancellationToken).ConfigureAwait(false);
            count -= chunk;
        }
    }
}

/// <summary>
/// One contiguous span of the image: in-memory bytes, a file source, or a zero
/// gap. <see cref="DataLength"/> is the real content length; bytes from
/// <see cref="DataLength"/> up to <see cref="Length"/> are zero padding.
/// </summary>
internal sealed class Segment
{
    public required long Length { get; init; }
    public long DataLength { get; init; }
    public byte[]? Bytes { get; init; }
    public IContentSource? Source { get; init; }

    public static Segment Zero(long length) => new() { Length = length, DataLength = 0 };

    public static Segment FromBytes(byte[] bytes, long total) => new() { Length = total, DataLength = bytes.Length, Bytes = bytes };

    public static Segment FromSource(IContentSource source, long dataLength, long total) => new() { Length = total, DataLength = dataLength, Source = source };
}
