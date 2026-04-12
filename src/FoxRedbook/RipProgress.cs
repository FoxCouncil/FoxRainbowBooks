namespace FoxRedbook;

/// <summary>
/// Progress snapshot reported during ripping via <see cref="IProgress{T}"/>.
/// </summary>
public readonly record struct RipProgress
{
    /// <summary>
    /// LBA of the sector currently being processed.
    /// </summary>
    public required long Lba { get; init; }

    /// <summary>
    /// Total number of sectors in the current track.
    /// </summary>
    public required int TotalSectors { get; init; }

    /// <summary>
    /// Number of re-reads the current sector has required so far.
    /// </summary>
    public required int RetryCount { get; init; }

    /// <summary>
    /// What the verification engine just did for this sector.
    /// </summary>
    public required SectorStatus Status { get; init; }
}
