namespace FoxRedbook;

/// <summary>
/// Describes a single track from a CD's Table of Contents.
/// </summary>
/// <remarks>
/// <see cref="StartLba"/> is the first sector belonging to this track.
/// <see cref="SectorCount"/> is computed from the TOC (next track's start LBA minus this one,
/// or lead-out LBA for the last track). The last sector of the track is at
/// <c>StartLba + SectorCount - 1</c>.
/// </remarks>
public readonly record struct TrackInfo
{
    /// <summary>
    /// Track number (1–99). Matches the TOC point/track number field.
    /// </summary>
    public required int Number { get; init; }

    /// <summary>
    /// Logical Block Address of the first sector in this track.
    /// </summary>
    public required long StartLba { get; init; }

    /// <summary>
    /// Total number of sectors in this track.
    /// </summary>
    public required int SectorCount { get; init; }

    /// <summary>
    /// Whether this is an audio or data track.
    /// </summary>
    public required TrackType Type { get; init; }

    /// <summary>
    /// Raw control flags from the TOC Q subchannel (pre-emphasis, copy, four-channel).
    /// </summary>
    public required TrackControl Control { get; init; }

    /// <summary>
    /// LBA one past the last sector of this track (<c>StartLba + SectorCount</c>).
    /// Equal to the next track's <see cref="StartLba"/>, or the lead-out LBA for the last track.
    /// </summary>
    public long EndLba => StartLba + SectorCount;
}
