namespace FoxRedbook;

/// <summary>
/// Internal constants used by the verification engine. These are tuning
/// parameters derived from CD-DA physical constraints and empirical
/// testing of drive behavior.
/// </summary>
internal static class WiggleConstants
{
    /// <summary>
    /// Number of 16-bit words (samples) per CD-DA sector.
    /// A sector is 2,352 bytes = 1,176 sixteen-bit words (588 L + 588 R interleaved).
    /// </summary>
    internal const int WordsPerSector = CdConstants.SectorSize / sizeof(short);

    /// <summary>
    /// Minimum matching run length (in 16-bit words) to trust an overlap match
    /// between two cache blocks. Shorter matches could be coincidental — two
    /// independent audio streams can share short identical subsequences by chance.
    /// 64 words = 256 bytes = ~1.45 ms of audio, long enough that random
    /// agreement is astronomically unlikely.
    /// </summary>
    internal const int MinWordsSearch = 64;

    /// <summary>
    /// Minimum overlap required between read requests for jitter detection.
    /// Must be at least as large as <see cref="MinWordsSearch"/> so that
    /// overlapping reads always have enough common data to find a match.
    /// </summary>
    internal const int MinWordsOverlap = 64;

    /// <summary>
    /// Minimum agreeing samples to resolve a rift (dropped/duplicated bytes).
    /// Smaller than <see cref="MinWordsSearch"/> because rift analysis operates
    /// on data that's already been verified — we just need enough agreement
    /// to distinguish a real gap from noise.
    /// </summary>
    internal const int MinWordsRift = 16;

    /// <summary>
    /// Maximum dynamic overlap in sectors. Bounds the jitter search window to
    /// prevent pathological cases from scanning the entire disc.
    /// </summary>
    internal const int MaxSectorOverlap = 32;

    /// <summary>
    /// Minimum dynamic overlap in samples. The search window never shrinks
    /// below this even when jitter measurements show very little variation,
    /// because a drive can develop jitter at any time.
    /// </summary>
    internal const int MinSectorEpsilon = 128;

    /// <summary>
    /// <para>
    /// Number of samples trimmed from each end of a verified region before
    /// extracting it as a fragment. Equal to <c>MinWordsOverlap / 2 - 1</c> (31).
    /// </para>
    /// <para>
    /// This trim is load-bearing for gap detection. Two fragments extracted from
    /// the same verification run overlap by at least <c>MinWordsOverlap - 2 * OverlapAdj = 2</c>
    /// samples, proving they're contiguous. Fragments from different runs have a
    /// detectable gap of up to <c>2 * OverlapAdj = 62</c> unverified samples between
    /// their trimmed edges. Stage 2 uses this gap/overlap distinction to decide
    /// whether rift analysis is needed. Without the trim, fragments from independent
    /// runs would appear seamlessly adjacent, and boundary errors would go undetected.
    /// </para>
    /// </summary>
    internal const int OverlapAdj = MinWordsOverlap / 2 - 1;

    /// <summary>
    /// <para>
    /// Stride for Stage 1 sample scanning. Instead of comparing every sample
    /// position when searching for matches between cache blocks, we check every
    /// 23rd position.
    /// </para>
    /// <para>
    /// 23 is chosen because it's an odd prime that doesn't divide evenly into
    /// any CD-DA structural boundary (sector = 1,176 words, frame = 6 words).
    /// A non-prime or even stride could systematically skip the same positions
    /// relative to sector boundaries on every scan, creating blind spots.
    /// An odd prime guarantees that successive scans with different block
    /// alignments sample different positions, eventually covering every offset.
    /// </para>
    /// </summary>
    internal const int SampleStride = 23;

    /// <summary>
    /// Number of jitter measurements before recalculating the dynamic overlap
    /// window. Too few measurements produce noisy estimates; too many make the
    /// window slow to adapt to changing drive behavior (e.g., moving from a
    /// clean region to a scratched one).
    /// </summary>
    internal const int JitterMeasurementInterval = 10;

    /// <summary>
    /// How often (in retry iterations) to widen the overlap window when the
    /// engine isn't making progress. Every N retries, the window grows by 1.5×.
    /// </summary>
    internal const int RetryBackoffInterval = 5;

    /// <summary>
    /// Number of consecutive silence samples required to trigger silence-mode
    /// matching. Normal jitter detection fails on silence because all-zero
    /// samples match everywhere.
    /// </summary>
    internal const int MinSilenceBoundary = 1024;

    /// <summary>
    /// Half-width of the edge region marked on each side of a read-request
    /// boundary. Samples in this region get <see cref="SampleFlags.Edge"/> set
    /// because jitter and dropped samples concentrate at the points where
    /// the drive firmware switches between adjacent read commands.
    /// </summary>
    internal const int EdgeHalfWidth = MinWordsOverlap / 2;
}
