namespace FoxRedbook.Tests;

public sealed class JitterStatisticsTests
{
    [Fact]
    public void Initial_AllZero()
    {
        var stats = new JitterStatistics();

        Assert.Equal(0, stats.OffsetPoints);
        Assert.Equal(0, stats.OffsetAccum);
        Assert.Equal(0, stats.OffsetDiff);
        Assert.Equal(0, stats.OffsetMin);
        Assert.Equal(0, stats.OffsetMax);
    }

    [Fact]
    public void AddMeasurement_TracksCount()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(5);
        stats.AddMeasurement(-3);
        stats.AddMeasurement(0);

        Assert.Equal(3, stats.OffsetPoints);
    }

    [Fact]
    public void AddMeasurement_AccumulatesSum()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(10);
        stats.AddMeasurement(-4);
        stats.AddMeasurement(6);

        Assert.Equal(12, stats.OffsetAccum);
    }

    [Fact]
    public void AddMeasurement_AccumulatesAbsoluteDiff()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(10);
        stats.AddMeasurement(-4);
        stats.AddMeasurement(6);

        Assert.Equal(20, stats.OffsetDiff);
    }

    [Fact]
    public void AddMeasurement_TracksMinMax()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(5);
        stats.AddMeasurement(-10);
        stats.AddMeasurement(3);
        stats.AddMeasurement(20);
        stats.AddMeasurement(-2);

        Assert.Equal(-10, stats.OffsetMin);
        Assert.Equal(20, stats.OffsetMax);
    }

    [Fact]
    public void Reset_ClearsEverything()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(100);
        stats.AddMeasurement(-50);
        stats.Reset();

        Assert.Equal(0, stats.OffsetPoints);
        Assert.Equal(0, stats.OffsetAccum);
        Assert.Equal(0, stats.OffsetDiff);
        Assert.Equal(0, stats.OffsetMin);
        Assert.Equal(0, stats.OffsetMax);
    }

    [Fact]
    public void AddMeasurement_AfterReset_StartsFromZero()
    {
        var stats = new JitterStatistics();

        stats.AddMeasurement(100);
        stats.Reset();
        stats.AddMeasurement(7);

        Assert.Equal(1, stats.OffsetPoints);
        Assert.Equal(7, stats.OffsetAccum);
        Assert.Equal(7, stats.OffsetDiff);
        Assert.Equal(0, stats.OffsetMin);
        Assert.Equal(7, stats.OffsetMax);
    }
}
