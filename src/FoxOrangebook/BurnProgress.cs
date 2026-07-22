namespace FoxOrangebook;

/// <summary>
/// Progress report emitted during a burn session.
/// </summary>
public readonly record struct BurnProgress
{
    /// <summary>Track number currently being written.</summary>
    public required int TrackNumber { get; init; }

    /// <summary>Total sectors in the current track, including its pregap.</summary>
    public required int TrackSectors { get; init; }

    /// <summary>Sectors written so far in the current track, including pregap silence.</summary>
    public required int SectorsWritten { get; init; }

    /// <summary>Total sectors to be written across the whole disc, including all pregap regions.</summary>
    public required long TotalDiscSectors { get; init; }

    /// <summary>Total sectors written so far, including pregap silence.</summary>
    public required long TotalSectorsWritten { get; init; }
}
