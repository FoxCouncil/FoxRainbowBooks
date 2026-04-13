namespace FoxOrangebook;

/// <summary>
/// Progress report emitted during a burn session.
/// </summary>
public readonly record struct BurnProgress
{
    /// <summary>Track number currently being written.</summary>
    public required int TrackNumber { get; init; }

    /// <summary>Total sectors in the current track.</summary>
    public required int TrackSectors { get; init; }

    /// <summary>Sectors written so far in the current track.</summary>
    public required int SectorsWritten { get; init; }

    /// <summary>Total sectors across all tracks on the disc.</summary>
    public required long TotalDiscSectors { get; init; }

    /// <summary>Total sectors written across all tracks so far.</summary>
    public required long TotalSectorsWritten { get; init; }
}
