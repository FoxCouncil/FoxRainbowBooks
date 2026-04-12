namespace FoxRedbook;

/// <summary>
/// Per-sample state tracked during cross-verification.
/// </summary>
[Flags]
internal enum SampleFlags : byte
{
    /// <summary>No flags set — sample has not been processed.</summary>
    None = 0,

    /// <summary>
    /// Sample value matched across two independent reads at the same
    /// absolute position. Near-certain to be correct — uncorrelated read
    /// errors producing identical 16-bit values is vanishingly unlikely.
    /// </summary>
    Verified = 1 << 0,

    /// <summary>
    /// Sample is near a read-request boundary. Drive firmware introduces
    /// the most jitter and dropped samples at the point where one read
    /// command ends and the next begins, because the laser repositioning
    /// isn't perfectly repeatable. Matches cannot cross an Edge boundary
    /// in both blocks simultaneously — that would let a false alignment
    /// bridge two unreliable regions.
    /// </summary>
    Edge = 1 << 1,

    /// <summary>
    /// Drive returned a short read; this sample position was not filled
    /// with real data. The verification engine zero-fills these positions
    /// and never treats them as verified.
    /// </summary>
    Unread = 1 << 2,
}
