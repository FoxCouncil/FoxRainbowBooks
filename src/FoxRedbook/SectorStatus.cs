namespace FoxRedbook;

/// <summary>
/// Flags describing what the verification engine did to produce a given sector.
/// Multiple flags may be set on a single sector.
/// </summary>
[Flags]
public enum SectorStatus
{
    /// <summary>
    /// Sector read cleanly on the first attempt with no corrections needed.
    /// </summary>
    None = 0,

    /// <summary>
    /// A read operation was issued to the drive for this sector.
    /// </summary>
    Read = 1 << 0,

    /// <summary>
    /// Sector was verified against a second read (overlap or re-read matched).
    /// </summary>
    Verified = 1 << 1,

    /// <summary>
    /// Dynamic overlap adjustment was applied to correct jitter between reads.
    /// </summary>
    JitterCorrected = 1 << 2,

    /// <summary>
    /// Dropped bytes were detected and reconstructed from overlapping reads.
    /// </summary>
    DroppedBytesFixed = 1 << 3,

    /// <summary>
    /// Duplicated bytes were detected and removed from overlapping reads.
    /// </summary>
    DuplicatedBytesFixed = 1 << 4,

    /// <summary>
    /// A scratch or defect was detected and the sector was reconstructed
    /// from multiple re-reads.
    /// </summary>
    ScratchRepaired = 1 << 5,

    /// <summary>
    /// Maximum re-read count was exhausted without achieving a confident match.
    /// The best available data was used. Consider this sector suspect.
    /// </summary>
    Skipped = 1 << 6,

    /// <summary>
    /// Timing drift between the drive and expected sector boundaries was
    /// compensated for across multiple reads.
    /// </summary>
    DriftCorrected = 1 << 7,

    /// <summary>
    /// The drive reported a read error (sense key) for this sector on at
    /// least one attempt.
    /// </summary>
    ReadError = 1 << 8,
}
