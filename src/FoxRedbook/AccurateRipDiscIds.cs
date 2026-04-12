namespace FoxRedbook;

/// <summary>
/// Computes the two 32-bit AccurateRip disc IDs from a <see cref="TableOfContents"/>.
/// These are used to construct the AccurateRip database URL when checking
/// rip verification results; they identify the disc, not the audio data.
/// </summary>
internal static class AccurateRipDiscIds
{
    /// <summary>
    /// Computes the AccurateRip disc ID pair (ID1, ID2) for the given TOC.
    /// Only audio tracks contribute to the sums; data tracks are excluded.
    /// </summary>
    /// <remarks>
    /// Verified against whipper's Bloc Party Silent Alarm test vector,
    /// which produces ID1 = 0x001af2de and ID2 = 0x0105994e from a
    /// 13-track audio TOC with raw leadout LBA 243216. Hand-walked the
    /// weighted product sum to confirm the per-track formula before
    /// implementation.
    /// </remarks>
    internal static (uint Id1, uint Id2) Compute(TableOfContents toc)
    {
        ArgumentNullException.ThrowIfNull(toc);

        uint id1 = 0;
        uint id2 = 0;
        int audioCount = 0;

        foreach (var track in toc.Tracks)
        {
            if (track.Type != TrackType.Audio)
            {
                continue;
            }

            long lba = track.StartLba;

            // LOAD-BEARING: the max(lba, 1) guard is NOT a redundant
            // safety check. If track 1 starts at LBA 0 (a standard disc
            // with no pregap audio before the first track), the raw LBA
            // is 0 and the weighted contribution would be 0 * 1 = 0,
            // silently removing that track from the ID2 sum entirely.
            // Every canonical AccurateRip implementation (whipper,
            // morituri, libarcstk) uses the same guard, and the
            // published AccurateRip database was built against it —
            // removing it produces IDs that 404 against the live
            // database on every disc with a zero-LBA first track.
            long lbaForId2 = Math.Max(lba, 1);

            audioCount++;
            id1 = unchecked(id1 + (uint)lba);
            id2 = unchecked(id2 + ((uint)lbaForId2 * (uint)track.Number));
        }

        if (audioCount == 0)
        {
            throw new InvalidOperationException("Cannot compute AccurateRip disc IDs for a TOC with no audio tracks.");
        }

        // Lead-out contribution: the "offset one past the last track" is
        // added to ID1 directly, and multiplied by (audioCount + 1) for ID2.
        // For a disc with no data track, this equals TOC.LeadOutLba.
        // For mixed-mode discs, whipper uses the raw last-track-end regardless
        // of whether that track is audio or data — we do the same so our
        // IDs match whipper/dBpoweramp/EAC on mixed-mode discs.
        uint leadOut = (uint)toc.LeadOutLba;
        id1 = unchecked(id1 + leadOut);
        id2 = unchecked(id2 + leadOut * (uint)(audioCount + 1));

        return (id1, id2);
    }
}
