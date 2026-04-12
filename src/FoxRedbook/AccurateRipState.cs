namespace FoxRedbook;

/// <summary>
/// Running state for an incremental AccurateRip checksum computation.
/// Holds the low and high 32-bit accumulators, the current position
/// counter, and the skip boundaries for a single track.
/// </summary>
/// <remarks>
/// Mutable reference type (sealed class, not struct) to avoid the
/// silent-mutation-on-copy footgun. The state is updated in place
/// by <see cref="AccurateRipChecksum.Update"/> as each sector of
/// the track is ripped.
/// </remarks>
internal sealed class AccurateRipState
{
    /// <summary>
    /// Low 32 bits of the running product accumulator. Equals the v1
    /// checksum when the track is fully consumed.
    /// </summary>
    internal uint CsumLo { get; set; }

    /// <summary>
    /// High 32 bits of the running product accumulator. v2 is computed
    /// as <c>CsumLo + CsumHi</c>, folding the 64-bit product's upper half
    /// back into the 32-bit result.
    /// </summary>
    internal uint CsumHi { get; set; }

    /// <summary>
    /// 1-based position counter for the next DWORD to be processed.
    /// Increments regardless of whether the sample falls inside the
    /// skip boundaries — the multiplier is tied to absolute track
    /// position, not to the number of included samples.
    /// </summary>
    internal uint CurrentMulti { get; set; }

    /// <summary>
    /// Inclusive lower bound for the position counter. Positions below
    /// this are excluded from the checksum.
    /// </summary>
    internal uint CheckFrom { get; set; }

    /// <summary>
    /// Inclusive upper bound for the position counter. Positions above
    /// this are excluded from the checksum.
    /// </summary>
    internal uint CheckTo { get; set; }

    /// <summary>
    /// Set to <see langword="true"/> when the last sector of the track
    /// has been processed. Prevents accessing checksums on partial rips.
    /// </summary>
    internal bool IsFinalized { get; set; }

    /// <summary>
    /// AccurateRip v1 checksum. Equal to the low 32 bits of the
    /// accumulated position-weighted sum.
    /// </summary>
    internal uint V1 => CsumLo;

    /// <summary>
    /// AccurateRip v2 checksum. Equal to v1 plus the high 32 bits of the
    /// accumulated product — folds the overflow of the 32-bit multiplication
    /// back into the result so that every bit of every sample contributes.
    /// </summary>
    internal uint V2 => CsumLo + CsumHi;
}
