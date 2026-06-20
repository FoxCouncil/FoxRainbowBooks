namespace FoxOrangebook;

/// <summary>
/// Options for a data-disc (Mode 1 / 2048-byte) burn session.
/// </summary>
public sealed record DataBurnOptions
{
    /// <summary>
    /// If true, performs a simulated burn (laser off). The drive runs the full
    /// write sequence without marking the disc — useful for verifying data rate.
    /// </summary>
    public bool TestWrite { get; init; }

    /// <summary>
    /// If true, enables buffer underrun protection (BURN-Free / SafeBurn).
    /// </summary>
    public bool BufferUnderrunProtection { get; init; } = true;

    /// <summary>
    /// Number of 2048-byte sectors to send per WRITE (10) command. 16 sectors =
    /// 32,768 bytes per command — a good balance of throughput and memory.
    /// </summary>
    public int SectorsPerWrite { get; init; } = 16;
}
