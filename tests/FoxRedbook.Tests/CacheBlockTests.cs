namespace FoxRedbook.Tests;

public sealed class CacheBlockTests
{
    [Fact]
    public void Constructor_AllocatesCorrectSize()
    {
        using var block = new CacheBlock(1176);

        Assert.Equal(1176, block.Size);
        Assert.Equal(1176, block.Samples.Length);
        Assert.Equal(1176, block.Flags.Length);
    }

    [Fact]
    public void Flags_InitializedToNone()
    {
        using var block = new CacheBlock(100);

        for (int i = 0; i < block.Size; i++)
        {
            Assert.Equal(SampleFlags.None, block.Flags[i]);
        }
    }

    [Fact]
    public void Samples_ReadWrite()
    {
        using var block = new CacheBlock(10);

        block.Samples[0] = 12345;
        block.Samples[9] = -4567;

        Assert.Equal(12345, block.Samples[0]);
        Assert.Equal(-4567, block.Samples[9]);
    }

    [Fact]
    public void Flags_ReadWrite()
    {
        using var block = new CacheBlock(10);

        block.Flags[3] = SampleFlags.Verified;
        block.Flags[7] = SampleFlags.Edge | SampleFlags.Verified;

        Assert.Equal(SampleFlags.Verified, block.Flags[3]);
        Assert.Equal(SampleFlags.Edge | SampleFlags.Verified, block.Flags[7]);
    }

    [Fact]
    public void Begin_End_Computed()
    {
        using var block = new CacheBlock(1176);
        block.Begin = 5000;

        Assert.Equal(5000, block.Begin);
        Assert.Equal(5000 + 1176, block.End);
    }

    [Fact]
    public void Dispose_Samples_ThrowsObjectDisposedException()
    {
        var block = new CacheBlock(10);
        block.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = block.Samples);
    }

    [Fact]
    public void Dispose_Flags_ThrowsObjectDisposedException()
    {
        var block = new CacheBlock(10);
        block.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = block.Flags);
    }

    [Fact]
    public void Dispose_SamplesArray_ThrowsObjectDisposedException()
    {
        var block = new CacheBlock(10);
        block.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = block.SamplesArray);
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        var block = new CacheBlock(10);
        block.Dispose();
        block.Dispose();
    }
}
