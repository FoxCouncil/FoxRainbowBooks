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
    /// 75,264 bytes per command — a good balance. On transports with a
    /// 64 KB transfer cap (typical USB), keep this at 27 or below
    /// (27 × 2,352 = 63,504 bytes).
    /// </summary>
    public int SectorsPerWrite { get; init; } = 32;

    /// <summary>
    /// Write speed in kB/s, sent to the drive via SET CD SPEED before
    /// power calibration. 176 kB/s = 1x audio speed; common values are
    /// 706 (4x), 1,412 (8x), 2,824 (16x). Null (the default) leaves the
    /// drive at its maximum speed. Values are clamped to the valid range;
    /// the drive rounds to its nearest supported speed.
    /// </summary>
    public int? WriteSpeedKBps { get; init; }

    /// <summary>
    /// Disc title for cue sheet and CD-Text. Optional.
    /// </summary>
    public string? DiscTitle { get; init; }

    /// <summary>
    /// Disc performer for cue sheet and CD-Text. Optional.
    /// </summary>
    public string? DiscPerformer { get; init; }
}
