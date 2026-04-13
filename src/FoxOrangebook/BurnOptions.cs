namespace FoxOrangebook;

/// <summary>
/// Options for a burn session.
/// </summary>
public sealed record BurnOptions
{
    /// <summary>
    /// If true, performs a simulated burn (laser off). The drive goes
    /// through the full write sequence without actually marking the disc.
    /// Useful for verifying that the drive accepts the cue sheet and
    /// data rate before committing to a real burn.
    /// </summary>
    public bool TestWrite { get; init; }

    /// <summary>
    /// If true, enables buffer underrun protection (BURN-Free / SafeBurn).
    /// Most modern drives support this. When the host can't feed data fast
    /// enough, the drive pauses the laser and resumes seamlessly rather
    /// than producing a coaster.
    /// </summary>
    public bool BufferUnderrunProtection { get; init; } = true;

    /// <summary>
    /// Number of sectors to send per WRITE (10) command. Larger values
    /// reduce command overhead but require more memory. 32 sectors =
    /// 75,264 bytes per command — a good balance.
    /// </summary>
    public int SectorsPerWrite { get; init; } = 32;

    /// <summary>
    /// Disc title for cue sheet and CD-Text. Optional.
    /// </summary>
    public string? DiscTitle { get; init; }

    /// <summary>
    /// Disc performer for cue sheet and CD-Text. Optional.
    /// </summary>
    public string? DiscPerformer { get; init; }
}
