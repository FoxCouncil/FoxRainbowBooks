namespace FoxRedbook.Tests;

public sealed class RootBlockTests
{
    [Fact]
    public void NewRoot_IsEmpty()
    {
        using var root = new RootBlock(1024);

        Assert.True(root.IsEmpty);
        Assert.Equal(-1, root.Begin);
        Assert.Equal(-1, root.End);
        Assert.Equal(0, root.Size);
    }

    [Fact]
    public void InitializeFrom_SetsDataAndPosition()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(100);
        frag.Begin = 500;

        for (int i = 0; i < frag.Size; i++)
        {
            frag.Samples[i] = (short)(i * 3);
        }

        root.InitializeFrom(frag);

        Assert.False(root.IsEmpty);
        Assert.Equal(500, root.Begin);
        Assert.Equal(600, root.End);
        Assert.Equal(100, root.Size);

        for (int i = 0; i < 100; i++)
        {
            Assert.Equal((short)(i * 3), root.Samples[i]);
        }
    }

    [Fact]
    public void Append_ExtendsEnd()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(10);
        frag.Begin = 0;
        frag.Samples.Fill(1);

        root.InitializeFrom(frag);

        short[] extra = [100, 200, 300];
        root.Append(extra);

        Assert.Equal(13, root.Size);
        Assert.Equal(0, root.Begin);
        Assert.Equal(13, root.End);
        Assert.Equal(100, root.Samples[10]);
        Assert.Equal(200, root.Samples[11]);
        Assert.Equal(300, root.Samples[12]);
    }

    [Fact]
    public void Insert_ShiftsDataAndInserts()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(5);
        frag.Begin = 0;
        frag.Samples[0] = 1;
        frag.Samples[1] = 2;
        frag.Samples[2] = 3;
        frag.Samples[3] = 4;
        frag.Samples[4] = 5;

        root.InitializeFrom(frag);

        short[] inserted = [10, 20];
        root.Insert(2, inserted);

        Assert.Equal(7, root.Size);
        Assert.Equal(1, root.Samples[0]);
        Assert.Equal(2, root.Samples[1]);
        Assert.Equal(10, root.Samples[2]);
        Assert.Equal(20, root.Samples[3]);
        Assert.Equal(3, root.Samples[4]);
        Assert.Equal(4, root.Samples[5]);
        Assert.Equal(5, root.Samples[6]);
    }

    [Fact]
    public void Remove_ShiftsDataAndShrinks()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(5);
        frag.Begin = 0;
        frag.Samples[0] = 1;
        frag.Samples[1] = 2;
        frag.Samples[2] = 3;
        frag.Samples[3] = 4;
        frag.Samples[4] = 5;

        root.InitializeFrom(frag);
        root.Remove(1, 2);

        Assert.Equal(3, root.Size);
        Assert.Equal(1, root.Samples[0]);
        Assert.Equal(4, root.Samples[1]);
        Assert.Equal(5, root.Samples[2]);
    }

    [Fact]
    public void TrimBefore_DiscardsOldData()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(100);
        frag.Begin = 1000;
        frag.Samples.Fill(42);

        root.InitializeFrom(frag);
        root.TrimBefore(1050);

        Assert.Equal(1050, root.Begin);
        Assert.Equal(50, root.Size);
        Assert.Equal(1100, root.End);
    }

    [Fact]
    public void TrimBefore_EntireRange_BecomesEmpty()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(10);
        frag.Begin = 0;

        root.InitializeFrom(frag);
        root.TrimBefore(100);

        Assert.True(root.IsEmpty);
        Assert.Equal(-1, root.Begin);
    }

    [Fact]
    public void TrimBefore_BeforeBegin_NoOp()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(10);
        frag.Begin = 100;
        frag.Samples.Fill(7);

        root.InitializeFrom(frag);
        root.TrimBefore(50);

        Assert.Equal(100, root.Begin);
        Assert.Equal(10, root.Size);
    }

    [Fact]
    public void Covers_TrueWhenFullyCovered()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(100);
        frag.Begin = 0;

        root.InitializeFrom(frag);

        Assert.True(root.Covers(0, 100));
        Assert.True(root.Covers(10, 90));
        Assert.True(root.Covers(0, 1));
    }

    [Fact]
    public void Covers_FalseWhenNotCovered()
    {
        using var root = new RootBlock(1024);
        using var frag = new VerifiedFragment(100);
        frag.Begin = 10;

        root.InitializeFrom(frag);

        Assert.False(root.Covers(0, 100));
        Assert.False(root.Covers(10, 111));
        Assert.False(root.Covers(200, 300));
    }

    [Fact]
    public void Covers_FalseWhenEmpty()
    {
        using var root = new RootBlock(1024);

        Assert.False(root.Covers(0, 10));
    }

    [Fact]
    public void Append_GrowsBeyondInitialCapacity()
    {
        using var root = new RootBlock(16);
        using var frag = new VerifiedFragment(10);
        frag.Begin = 0;
        frag.Samples.Fill(1);

        root.InitializeFrom(frag);

        short[] big = new short[100];
        Array.Fill(big, (short)2);
        root.Append(big);

        Assert.Equal(110, root.Size);
        Assert.Equal(1, root.Samples[0]);
        Assert.Equal(2, root.Samples[10]);
        Assert.Equal(2, root.Samples[109]);
    }

    [Fact]
    public void Dispose_Samples_ThrowsObjectDisposedException()
    {
        var root = new RootBlock(1024);
        root.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = root.Samples);
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        var root = new RootBlock(1024);
        root.Dispose();
        root.Dispose();
    }
}
