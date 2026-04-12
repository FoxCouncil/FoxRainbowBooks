namespace FoxRedbook.Tests;

public sealed class AccurateRipChecksumTests
{
    // ── Trivial hand-computed cases ──────────────────────────────

    [Fact]
    public void AllZeros_Track2Of3_ReturnsZero()
    {
        uint[] pcm = new uint[588];
        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        Assert.Equal(0u, v1);
        Assert.Equal(0u, v2);
    }

    [Fact]
    public void SingleSamplePositionOne_ReturnsValue()
    {
        uint[] pcm = new uint[588];
        pcm[0] = 1;

        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        // multi=1 at i=0, product = 1 * 1 = 1, csumLo=1, csumHi=0
        Assert.Equal(1u, v1);
        Assert.Equal(1u, v2);
    }

    [Fact]
    public void SingleMaxValuePositionOne_ReturnsMaxValue()
    {
        uint[] pcm = new uint[588];
        pcm[0] = 0xFFFFFFFFu;

        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        // multi=1, product = 0xFFFFFFFF * 1 = 0xFFFFFFFF (no high bits)
        // csumLo = 0xFFFFFFFF, csumHi = 0
        Assert.Equal(0xFFFFFFFFu, v1);
        Assert.Equal(0xFFFFFFFFu, v2);
    }

    [Fact]
    public void TwoMaxValueSamples_LowWrapsHighFolds()
    {
        uint[] pcm = new uint[588];
        pcm[0] = 0xFFFFFFFFu;
        pcm[1] = 0xFFFFFFFFu;

        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        // multi=1: product = 0xFFFFFFFF, csumLo=0xFFFFFFFF, csumHi=0
        // multi=2: product = 0x1FFFFFFFE, csumLo += 0xFFFFFFFE → wraps to 0xFFFFFFFD, csumHi += 1 → 1
        // v1 = 0xFFFFFFFD
        // v2 = 0xFFFFFFFD + 1 = 0xFFFFFFFE
        Assert.Equal(0xFFFFFFFDu, v1);
        Assert.Equal(0xFFFFFFFEu, v2);
    }

    // ── Skip boundary off-by-one ─────────────────────────────────

    [Fact]
    public void Track1Of3_Position2939IsSkipped_Position2940IsIncluded()
    {
        // 10 sectors = 5880 DWORDs. Track 1 of 3 → checkFrom = 2940.
        // Position 2939 (at index 2938) is skipped because multi=2939 < 2940.
        // Position 2940 (at index 2939) is included because multi=2940 >= 2940.
        uint[] pcm = new uint[10 * 588];
        pcm[2938] = 1;  // should be excluded
        pcm[2939] = 1;  // should be included

        AccurateRipChecksum.Compute(pcm, 1, 3, out uint v1, out uint v2);

        // Only pcm[2939] contributes: 2940 * 1 = 2940
        Assert.Equal(2940u, v1);
        Assert.Equal(2940u, v2);
    }

    [Fact]
    public void LastTrackOf3_EndSkipBoundary()
    {
        // 10 sectors = 5880 DWORDs. Track 3 of 3 → checkTo = 5880 - 2940 = 2940.
        // Position 2940 (multi=2940, at i=2939) is the last included position.
        // Position 2941 (multi=2941, at i=2940) is skipped.
        uint[] pcm = new uint[10 * 588];
        pcm[2939] = 1;  // should be included
        pcm[2940] = 1;  // should be excluded

        AccurateRipChecksum.Compute(pcm, 3, 3, out uint v1, out uint v2);

        // Only pcm[2939] contributes: 2940 * 1 = 2940
        Assert.Equal(2940u, v1);
        Assert.Equal(2940u, v2);
    }

    [Fact]
    public void SingleTrackOf1_BothSkipsApply()
    {
        // 10 sectors, single-track disc. Both start and end skips apply.
        // checkFrom = 2940, checkTo = 5880 - 2940 = 2940.
        // Only position multi=2940 (i=2939) is included.
        uint[] pcm = new uint[10 * 588];
        pcm[2939] = 5;  // the one included position

        AccurateRipChecksum.Compute(pcm, 1, 1, out uint v1, out uint v2);

        // 2940 * 5 = 14700
        Assert.Equal(14700u, v1);
        Assert.Equal(14700u, v2);
    }

    [Fact]
    public void Track1Of3_ShortTrack_SkipWindowExcludesAll()
    {
        // 5 sectors = 2940 DWORDs. Track 1 of 3 → checkFrom = 2940, checkTo = 2940.
        // Only position multi=2940 (i=2939) is included.
        uint[] pcm = new uint[5 * 588];
        pcm[2939] = 7;

        AccurateRipChecksum.Compute(pcm, 1, 3, out uint v1, out uint v2);

        Assert.Equal(2940u * 7u, v1);
        Assert.Equal(2940u * 7u, v2);
    }

    [Fact]
    public void Track1Of1_VeryShortTrack_ExcludesEverything()
    {
        // 5 sectors, single track. checkFrom = 2940, checkTo = max(0, 2940 - 2940) = 0.
        // checkFrom > checkTo, no samples included.
        uint[] pcm = new uint[5 * 588];

        for (int i = 0; i < pcm.Length; i++)
        {
            pcm[i] = (uint)i;
        }

        AccurateRipChecksum.Compute(pcm, 1, 1, out uint v1, out uint v2);

        Assert.Equal(0u, v1);
        Assert.Equal(0u, v2);
    }

    // ── Externally-verified nontrivial patterns ──────────────────
    //
    // The expected values below were produced by a separate reference
    // implementation of the same algorithm, compiled from the documented
    // C source and run over the same input patterns. They lock this port
    // against subtle differences from the canonical algorithm.

    [Fact]
    public void FiveSectors_LcgPattern_Track2Of3_MatchesReference()
    {
        uint[] pcm = BuildLcgPattern(5 * 588);
        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        Assert.Equal(0xEDBE2C4Eu, v1);
        Assert.Equal(0xEDDFB8E9u, v2);
    }

    [Fact]
    public void FiveSectors_LcgPattern_Track1Of3_StartSkip_MatchesReference()
    {
        uint[] pcm = BuildLcgPattern(5 * 588);
        AccurateRipChecksum.Compute(pcm, 1, 3, out uint v1, out uint v2);

        Assert.Equal(0x4204C850u, v1);
        Assert.Equal(0x4204D304u, v2);
    }

    [Fact]
    public void FiveSectors_LcgPattern_Track3Of3_EndSkip_MatchesReference()
    {
        // 5 sectors = 2940 DWORDs. Track 3 of 3 → checkTo = 0.
        // checkFrom = 0, but multi starts at 1, so 1 > 0 → no samples included.
        uint[] pcm = BuildLcgPattern(5 * 588);
        AccurateRipChecksum.Compute(pcm, 3, 3, out uint v1, out uint v2);

        Assert.Equal(0u, v1);
        Assert.Equal(0u, v2);
    }

    [Fact]
    public void TenSectors_AllMaxValue_Track2Of3_MatchesReference()
    {
        // Stress the 64-bit product path: every sample is 0xFFFFFFFF,
        // so every product overflows 32 bits and contributes to csumHi.
        uint[] pcm = new uint[10 * 588];
        Array.Fill(pcm, 0xFFFFFFFFu);

        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1, out uint v2);

        Assert.Equal(0xFEF82C64u, v1);
        Assert.Equal(0xFFFFE908u, v2);
    }

    // ── Incremental update equivalence ───────────────────────────

    [Fact]
    public void IncrementalUpdate_MatchesOneShotCompute()
    {
        uint[] pcm = BuildLcgPattern(5 * 588);

        // One-shot
        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1OneShot, out uint v2OneShot);

        // Incremental, sector by sector
        var state = AccurateRipChecksum.CreateState(2, 3, (uint)pcm.Length);

        for (int s = 0; s < 5; s++)
        {
            AccurateRipChecksum.Update(state, pcm.AsSpan(s * 588, 588));
        }

        Assert.Equal(v1OneShot, state.V1);
        Assert.Equal(v2OneShot, state.V2);
    }

    [Fact]
    public void IncrementalUpdate_ByteLevelChunks_MatchesOneShot()
    {
        uint[] pcm = BuildLcgPattern(3 * 588);

        AccurateRipChecksum.Compute(pcm, 2, 3, out uint v1OneShot, out uint v2OneShot);

        // Incremental, one DWORD at a time
        var state = AccurateRipChecksum.CreateState(2, 3, (uint)pcm.Length);

        for (int i = 0; i < pcm.Length; i++)
        {
            AccurateRipChecksum.Update(state, pcm.AsSpan(i, 1));
        }

        Assert.Equal(v1OneShot, state.V1);
        Assert.Equal(v2OneShot, state.V2);
    }

    // ── Helpers ──────────────────────────────────────────────────

    private static uint[] BuildLcgPattern(int count)
    {
        uint[] pcm = new uint[count];

        for (int i = 0; i < count; i++)
        {
            pcm[i] = unchecked((uint)i * 0x01234567u + 0xDEADBEEFu);
        }

        return pcm;
    }
}
