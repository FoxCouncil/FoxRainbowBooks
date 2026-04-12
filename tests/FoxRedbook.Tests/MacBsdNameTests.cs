using FoxRedbook.Platforms.MacOS;

namespace FoxRedbook.Tests;

/// <summary>
/// Pure-function tests for <see cref="MacBsdName.Normalize"/>. The function
/// is string-only and runs identically on any host.
/// </summary>
public sealed class MacBsdNameTests
{
    [Theory]
    [InlineData("disk1")]
    [InlineData("disk2")]
    [InlineData("disk10")]
    public void Normalize_BareName_PassesThrough(string input)
    {
        Assert.Equal(input, MacBsdName.Normalize(input));
    }

    [Theory]
    [InlineData("/dev/disk1", "disk1")]
    [InlineData("/dev/disk2s1", "disk2s1")]
    public void Normalize_DevPrefix_Stripped(string input, string expected)
    {
        Assert.Equal(expected, MacBsdName.Normalize(input));
    }

    [Fact]
    public void Normalize_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => MacBsdName.Normalize(null!));
    }

    [Fact]
    public void Normalize_Empty_Throws()
    {
        Assert.Throws<ArgumentException>(() => MacBsdName.Normalize(""));
    }

    [Fact]
    public void Normalize_DevOnly_Throws()
    {
        Assert.Throws<ArgumentException>(() => MacBsdName.Normalize("/dev/"));
    }

    [Theory]
    [InlineData("/home/user/file")]    // filesystem path, not /dev/
    [InlineData("disk1/extra")]         // slash in bare name
    [InlineData("1disk")]               // doesn't start with letter
    [InlineData("disk-1")]              // hyphen
    [InlineData("disk 1")]              // space
    [InlineData("/Users/foo")]          // macOS filesystem path
    public void Normalize_Malformed_Throws(string input)
    {
        Assert.Throws<ArgumentException>(() => MacBsdName.Normalize(input));
    }
}
