namespace FoxOrangebook;

/// <summary>
/// Status of the disc in the drive, parsed from READ DISC INFORMATION.
/// </summary>
public enum DiscStatus
{
    /// <summary>Disc is blank — ready for a fresh burn.</summary>
    Blank = 0x00,

    /// <summary>Disc has content but can accept additional sessions.</summary>
    Appendable = 0x01,

    /// <summary>Disc is finalized — no further writing is possible.</summary>
    Complete = 0x02,
}
