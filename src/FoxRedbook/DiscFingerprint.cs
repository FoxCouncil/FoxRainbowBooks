namespace FoxRedbook;

/// <summary>
/// Pure-function computation of disc fingerprints (MusicBrainz, freedb,
/// AccurateRip) from a <see cref="TableOfContents"/>. No drive I/O, no
/// network access — everything is derived from the TOC alone.
/// </summary>
public static class DiscFingerprint
{
    /// <summary>
    /// Computes the full <see cref="DiscInfo"/> fingerprint for the given TOC.
    /// </summary>
    /// <param name="toc">The parsed table of contents.</param>
    /// <returns>
    /// A <see cref="DiscInfo"/> containing the TOC and all three database
    /// identifiers (MusicBrainz, freedb, and both AccurateRip IDs).
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="toc"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The TOC contains no tracks, or no audio tracks (required for the
    /// MusicBrainz and AccurateRip computations).
    /// </exception>
    public static DiscInfo Compute(TableOfContents toc)
    {
        ArgumentNullException.ThrowIfNull(toc);

        string mbId = MusicBrainzDiscId.Compute(toc);
        uint freedbId = FreedbDiscId.Compute(toc);
        (uint arId1, uint arId2) = AccurateRipDiscIds.Compute(toc);

        return new DiscInfo
        {
            Toc = toc,
            MusicBrainzDiscId = mbId,
            FreedbDiscId = freedbId,
            AccurateRipId1 = arId1,
            AccurateRipId2 = arId2,
        };
    }
}
