namespace FoxRedbook;

/// <summary>
/// Abstraction over a physical or virtual optical drive capable of reading CD-DA sectors.
/// </summary>
/// <remarks>
/// <para>
/// Platform backends (<c>FoxRedbook.Windows</c>, <c>FoxRedbook.Linux</c>, <c>FoxRedbook.MacOS</c>)
/// implement this interface using OS-specific SCSI passthrough mechanisms.
/// <c>FileBackedOpticalDrive</c> in the test project provides a test fixture implementation
/// backed by a .bin file.
/// </para>
/// <para>
/// The verification engine consumes this interface and knows nothing about the underlying OS.
/// All buffer management for <see cref="ReadSectorsAsync"/> is the caller's responsibility —
/// use <see cref="System.Buffers.ArrayPool{T}"/> in hot paths.
/// </para>
/// </remarks>
public interface IOpticalDrive : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Drive identification from the SCSI INQUIRY response.
    /// Available after the drive is opened; does not require disc access.
    /// </summary>
    DriveInquiry Inquiry { get; }

    /// <summary>
    /// Reads the Table of Contents from the disc in the drive.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The parsed TOC describing all tracks on the disc.</returns>
    /// <exception cref="InvalidOperationException">No disc is present in the drive.</exception>
    Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one or more contiguous raw CD-DA sectors from the disc.
    /// </summary>
    /// <param name="lba">Logical Block Address of the first sector to read.</param>
    /// <param name="count">Number of contiguous sectors to read.</param>
    /// <param name="buffer">
    /// Caller-provided buffer to receive the data. Must be large enough to hold
    /// <paramref name="count"/> sectors at the per-sector size implied by <paramref name="flags"/>:
    /// <list type="bullet">
    ///   <item><see cref="ReadOptions.None"/>: <c>count × 2,352</c> bytes</item>
    ///   <item><see cref="ReadOptions.C2ErrorPointers"/>: <c>count × 2,646</c> bytes (2,352 + 294)</item>
    ///   <item><see cref="ReadOptions.SubchannelData"/>: <c>count × 2,448</c> bytes (2,352 + 96)</item>
    ///   <item>Both flags: <c>count × 2,742</c> bytes (2,352 + 294 + 96)</item>
    /// </list>
    /// </param>
    /// <param name="flags">Controls whether C2 error pointers and/or subchannel data are requested.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The number of sectors actually read (may be less than <paramref name="count"/> near the end of a track).</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="buffer"/> is smaller than <see cref="CdConstants.GetReadBufferSize"/>
    /// requires for the given <paramref name="flags"/> and <paramref name="count"/>.
    /// </exception>
    Task<int> ReadSectorsAsync(long lba, int count, Memory<byte> buffer, ReadOptions flags = ReadOptions.None, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads CD-Text metadata from the disc's lead-in area via
    /// <c>READ TOC</c> format 5. CD-Text is optional on commercial CDs;
    /// roughly 10–20% of pressed discs include it. Returns
    /// <see langword="null"/> when the disc has no CD-Text data.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// Parsed CD-Text for the disc's first language block, or
    /// <see langword="null"/> if the disc has no CD-Text.
    /// </returns>
    /// <exception cref="InvalidOperationException">No disc is present in the drive.</exception>
    Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default);
}
