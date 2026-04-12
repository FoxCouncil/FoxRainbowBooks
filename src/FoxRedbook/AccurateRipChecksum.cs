namespace FoxRedbook;

/// <summary>
/// Computes the AccurateRip "checksum" values — v1 and v2 — that consumers
/// use to cross-check their rips against a community database of known-good
/// values. Despite the ubiquitous "CRC" terminology in the public ecosystem,
/// this is not a cyclic redundancy check. It is a position-weighted sum of
/// 32-bit sample pairs. The internal naming reflects the actual algorithm;
/// the public <c>RipSession</c> API keeps the "CRC" terminology because that
/// is what database consumers search for.
/// </summary>
/// <remarks>
/// <para>
/// Both versions operate on the track's audio data as a sequence of 32-bit
/// unsigned sample pairs (one DWORD = one stereo frame = 4 bytes). Each
/// DWORD is multiplied by a 1-based position counter; v1 keeps only the
/// low 32 bits of the product, v2 folds both halves of the 64-bit product
/// back into a 32-bit accumulator so that every bit of every sample
/// influences the final value.
/// </para>
/// <para>
/// The first 5 sectors of track 1 and the last 5 sectors of the final track
/// are excluded from the computation, absorbing boundary uncertainty from
/// drive read offsets and pregap/leadout handling differences between
/// rippers. See <see cref="SkipDwords"/> for the exact boundary behavior.
/// </para>
/// </remarks>
internal static class AccurateRipChecksum
{
    /// <summary>
    /// Number of 32-bit sample pairs excluded from the checksum at the start
    /// of track 1 and the end of the final track. Equals 5 sectors × 588
    /// DWORDs per sector = 2,940.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The effective skip at the start of track 1 is 2,939 DWORDs, not 2,940,
    /// because the multiplier position counter starts at 1 and the boundary
    /// comparison is inclusive: position <c>multi == 2940</c> is the first
    /// position INCLUDED in the sum. The corresponding end-of-last-track
    /// skip is exactly 2,940 positions because the upper bound check uses
    /// <c>multi &lt;= checkTo</c> against the pre-decremented bound.
    /// </para>
    /// <para>
    /// This off-by-one asymmetry is a consequence of the original algorithm's
    /// 1-based position counter and must be preserved exactly for bit-for-bit
    /// compatibility with the public database.
    /// </para>
    /// </remarks>
    internal const uint SkipDwords = 2940;

    /// <summary>
    /// Creates a new per-track state initialized with the appropriate
    /// skip boundaries for the given track position on the disc.
    /// </summary>
    /// <param name="trackNumber">1-based track number on the disc.</param>
    /// <param name="totalTracks">Total audio track count on the disc.</param>
    /// <param name="totalDwords">
    /// Total number of 32-bit sample pairs in the track. Equals
    /// <c>sectorCount × 588</c>.
    /// </param>
    internal static AccurateRipState CreateState(int trackNumber, int totalTracks, uint totalDwords)
    {
        uint checkFrom = 0;
        uint checkTo = totalDwords;

        if (trackNumber == 1)
        {
            checkFrom = SkipDwords;
        }

        if (trackNumber == totalTracks)
        {
            // Guard against underflow for tracks smaller than the skip window
            checkTo = totalDwords > SkipDwords ? totalDwords - SkipDwords : 0;
        }

        return new AccurateRipState
        {
            CheckFrom = checkFrom,
            CheckTo = checkTo,
            CurrentMulti = 1,
        };
    }

    /// <summary>
    /// Processes a span of 32-bit sample pairs, updating the running state.
    /// Both v1 and v2 accumulators are updated in a single pass — v1 reads
    /// the low 32 bits of each 64-bit product, v2 reads both halves.
    /// </summary>
    /// <param name="state">Per-track state to update.</param>
    /// <param name="dwords">Sample pairs to process.</param>
    internal static void Update(AccurateRipState state, ReadOnlySpan<uint> dwords)
    {
        uint multi = state.CurrentMulti;
        uint checkFrom = state.CheckFrom;
        uint checkTo = state.CheckTo;
        uint csumLo = state.CsumLo;
        uint csumHi = state.CsumHi;

        for (int i = 0; i < dwords.Length; i++)
        {
            if (multi >= checkFrom && multi <= checkTo)
            {
                ulong product = (ulong)dwords[i] * multi;
                csumLo += (uint)product;
                csumHi += (uint)(product >> 32);
            }

            multi++;
        }

        state.CurrentMulti = multi;
        state.CsumLo = csumLo;
        state.CsumHi = csumHi;
    }

    /// <summary>
    /// Computes v1 and v2 in one pass for a complete track. Convenience
    /// wrapper for tests and one-shot use; production use flows through
    /// <see cref="CreateState"/> + repeated <see cref="Update"/> calls.
    /// </summary>
    /// <param name="pcm">Complete track audio as 32-bit sample pairs.</param>
    /// <param name="trackNumber">1-based track number.</param>
    /// <param name="totalTracks">Total audio tracks on the disc.</param>
    /// <param name="v1">Output v1 checksum.</param>
    /// <param name="v2">Output v2 checksum.</param>
    internal static void Compute(
        ReadOnlySpan<uint> pcm,
        int trackNumber,
        int totalTracks,
        out uint v1,
        out uint v2)
    {
        var state = CreateState(trackNumber, totalTracks, (uint)pcm.Length);
        Update(state, pcm);
        v1 = state.V1;
        v2 = state.V2;
    }
}
