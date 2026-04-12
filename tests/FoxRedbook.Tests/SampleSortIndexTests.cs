namespace FoxRedbook.Tests;

public sealed class SampleSortIndexTests
{
    [Fact]
    public void FindMatch_ExactPosition()
    {
        short[] data = [10, 20, 30, 40, 50];
        using var index = new SampleSortIndex(data, 0, data.Length);

        int result = index.FindMatch(2, 0, 30);

        Assert.Equal(2, result);
    }

    [Fact]
    public void FindMatch_WithinWindow()
    {
        short[] data = [10, 20, 30, 40, 50];
        using var index = new SampleSortIndex(data, 0, data.Length);

        // Value 50 is at index 4, search from position 2 with window 3
        int result = index.FindMatch(2, 3, 50);

        Assert.Equal(4, result);
    }

    [Fact]
    public void FindMatch_OutsideWindow_ReturnsSentinel()
    {
        short[] data = [10, 20, 30, 40, 50];
        using var index = new SampleSortIndex(data, 0, data.Length);

        // Value 50 is at index 4, search from position 0 with window 2
        int result = index.FindMatch(0, 2, 50);

        Assert.Equal(SampleSortIndex.NoMatch, result);
    }

    [Fact]
    public void FindMatch_ValueNotPresent_ReturnsSentinel()
    {
        short[] data = [10, 20, 30];
        using var index = new SampleSortIndex(data, 0, data.Length);

        int result = index.FindMatch(0, 10, 999);

        Assert.Equal(SampleSortIndex.NoMatch, result);
    }

    [Fact]
    public void FindMatch_DuplicateValues_ReturnsFirst()
    {
        short[] data = [5, 5, 5, 5, 5];
        using var index = new SampleSortIndex(data, 0, data.Length);

        int result = index.FindMatch(0, 10, 5);

        Assert.Equal(0, result);
    }

    [Fact]
    public void FindNextMatch_WalksChain()
    {
        short[] data = [5, 10, 5, 10, 5];
        using var index = new SampleSortIndex(data, 0, data.Length);

        int first = index.FindMatch(0, 10, 5);
        Assert.Equal(0, first);

        int second = index.FindNextMatch(first);
        Assert.Equal(2, second);

        int third = index.FindNextMatch(second);
        Assert.Equal(4, third);

        int end = index.FindNextMatch(third);
        Assert.Equal(SampleSortIndex.NoMatch, end);
    }

    [Fact]
    public void FindMatch_WithOffset()
    {
        short[] data = [99, 99, 10, 20, 30, 99, 99];
        using var index = new SampleSortIndex(data, 2, 3);

        // Index covers data[2..4] = [10, 20, 30]
        // Index position 0 = value 10, position 1 = value 20, position 2 = value 30
        int result = index.FindMatch(0, 5, 20);

        Assert.Equal(1, result);
    }

    [Fact]
    public void FindMatch_NegativeValues()
    {
        short[] data = [short.MinValue, -1, 0, 1, short.MaxValue];
        using var index = new SampleSortIndex(data, 0, data.Length);

        Assert.Equal(0, index.FindMatch(0, 10, short.MinValue));
        Assert.Equal(1, index.FindMatch(0, 10, -1));
        Assert.Equal(2, index.FindMatch(0, 10, 0));
        Assert.Equal(3, index.FindMatch(0, 10, 1));
        Assert.Equal(4, index.FindMatch(0, 10, short.MaxValue));
    }

    [Fact]
    public void GetSample_ReturnsCorrectValue()
    {
        short[] data = [100, 200, 300];
        using var index = new SampleSortIndex(data, 0, data.Length);

        Assert.Equal(100, index.GetSample(0));
        Assert.Equal(200, index.GetSample(1));
        Assert.Equal(300, index.GetSample(2));
    }

    [Fact]
    public void Dispose_FindMatch_ThrowsObjectDisposedException()
    {
        short[] data = [1, 2, 3];
        var index = new SampleSortIndex(data, 0, data.Length);
        index.Dispose();

        Assert.Throws<ObjectDisposedException>(() => index.FindMatch(0, 10, 1));
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        short[] data = [1, 2, 3];
        var index = new SampleSortIndex(data, 0, data.Length);
        index.Dispose();
        index.Dispose();
    }
}
