namespace FoxOrangebook;

/// <summary>
/// Parsed response from READ DISC INFORMATION (0x51). Contains the
/// disc's current state and whether it can accept writes.
/// </summary>
public readonly record struct DiscInfo
{
    /// <summary>Current disc status.</summary>
    public required DiscStatus Status { get; init; }

    /// <summary>Whether the disc is erasable (CD-RW).</summary>
    public required bool Erasable { get; init; }

    /// <summary>Number of the first track on the disc.</summary>
    public required byte FirstTrack { get; init; }

    /// <summary>Number of the last track on the disc.</summary>
    public required byte LastTrack { get; init; }

    /// <summary>
    /// Writable capacity of the disc in sectors, derived from the Last
    /// Possible Lead-Out Start Address (READ DISC INFORMATION bytes 20–23)
    /// with the 150-sector mandatory pregap subtracted. Null when the
    /// drive does not report the field (all-0xFF). An 80-minute blank
    /// reports roughly 359,849 sectors.
    /// </summary>
    public long? CapacitySectors { get; init; }
}
