namespace FoxRedbook.Tests;

public sealed class DriveNameNormalizerTests
{
    [Theory]
    [InlineData("PIONEER", "PIONEER")]
    [InlineData("pioneer", "PIONEER")]
    [InlineData("Pioneer", "PIONEER")]
    public void Normalize_Uppercases(string input, string expected)
    {
        Assert.Equal(expected, DriveNameNormalizer.Normalize(input));
    }

    [Theory]
    [InlineData("  PIONEER  ", "PIONEER")]
    [InlineData("PIONEER ", "PIONEER")]
    [InlineData(" PIONEER", "PIONEER")]
    public void Normalize_TrimsWhitespace(string input, string expected)
    {
        Assert.Equal(expected, DriveNameNormalizer.Normalize(input));
    }

    [Theory]
    [InlineData("BD-RW  BDR-XS07U", "BD-RW BDR-XS07U")]
    [InlineData("A   B    C", "A B C")]
    [InlineData("PLEXTOR  ", "PLEXTOR")]
    public void Normalize_CollapsesInternalSpaces(string input, string expected)
    {
        Assert.Equal(expected, DriveNameNormalizer.Normalize(input));
    }

    [Fact]
    public void Normalize_StripsEmbeddedNulls()
    {
        Assert.Equal("ABCD", DriveNameNormalizer.Normalize("AB\0CD"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Normalize_EmptyInput_ReturnsEmpty(string? input)
    {
        Assert.Equal(string.Empty, DriveNameNormalizer.Normalize(input!));
    }

    [Fact]
    public void BuildKey_FormatsCorrectly()
    {
        Assert.Equal("PIONEER|BD-RW BDR-XS07U", DriveNameNormalizer.BuildKey("PIONEER", "BD-RW BDR-XS07U"));
    }
}
