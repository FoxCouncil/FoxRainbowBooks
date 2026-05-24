namespace FoxRedbook.Tests;

public sealed class CdConstantsTests
{
    // Per-sector byte budget the backends allocate against:
    //   base PCM            = 2352
    //   + C2 error pointers = 2352 + 294 = 2646
    //   + subchannel        = 2352 +  96 = 2448
    //   + both              = 2352 + 294 + 96 = 2742
    // Expected values are written as independent literals (not re-derived
    // from CdConstants) so the test pins the contract rather than the math.
    [Theory]
    [InlineData(ReadOptions.None, 0, 0)]
    [InlineData(ReadOptions.None, 1, 2352)]
    [InlineData(ReadOptions.None, 5, 11760)]
    [InlineData(ReadOptions.C2ErrorPointers, 1, 2646)]
    [InlineData(ReadOptions.C2ErrorPointers, 5, 13230)]
    [InlineData(ReadOptions.SubchannelData, 1, 2448)]
    [InlineData(ReadOptions.SubchannelData, 5, 12240)]
    [InlineData(ReadOptions.C2ErrorPointers | ReadOptions.SubchannelData, 1, 2742)]
    [InlineData(ReadOptions.C2ErrorPointers | ReadOptions.SubchannelData, 5, 13710)]
    public void GetReadBufferSize_ComputesPerSectorTotal(ReadOptions flags, int sectorCount, int expected)
    {
        Assert.Equal(expected, CdConstants.GetReadBufferSize(flags, sectorCount));
    }
}
