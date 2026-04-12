namespace FoxRedbook.Tests;

public sealed class VerifiedFragmentTests
{
    [Fact]
    public void Constructor_AllocatesCorrectSize()
    {
        using var frag = new VerifiedFragment(200);

        Assert.Equal(200, frag.Size);
        Assert.Equal(200, frag.Samples.Length);
    }

    [Fact]
    public void Samples_ReadWrite()
    {
        using var frag = new VerifiedFragment(10);

        frag.Samples[0] = 999;
        frag.Samples[9] = -888;

        Assert.Equal(999, frag.Samples[0]);
        Assert.Equal(-888, frag.Samples[9]);
    }

    [Fact]
    public void Begin_End_Computed()
    {
        using var frag = new VerifiedFragment(100);
        frag.Begin = 3000;

        Assert.Equal(3000, frag.Begin);
        Assert.Equal(3100, frag.End);
    }

    [Fact]
    public void Dispose_Samples_ThrowsObjectDisposedException()
    {
        var frag = new VerifiedFragment(10);
        frag.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = frag.Samples);
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        var frag = new VerifiedFragment(10);
        frag.Dispose();
        frag.Dispose();
    }
}
