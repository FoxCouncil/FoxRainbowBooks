using FoxRedbook.Platforms.Windows;

namespace FoxRedbook.Tests;

/// <summary>
/// Pure-function tests for <see cref="WindowsDevicePath.Normalize"/>.
/// The function is string-only and runs identically on any host.
/// </summary>
public sealed class WindowsDevicePathTests
{
    [Theory]
    [InlineData("D:", @"\\.\D:")]
    [InlineData("Z:", @"\\.\Z:")]
    [InlineData("A:", @"\\.\A:")]
    public void Normalize_BareDriveLetter_PrependsDeviceNamespace(string input, string expected)
    {
        Assert.Equal(expected, WindowsDevicePath.Normalize(input));
    }

    [Theory]
    [InlineData("d:", @"\\.\D:")]
    [InlineData("z:", @"\\.\Z:")]
    public void Normalize_LowercaseDriveLetter_UppercasesAndPrepends(string input, string expected)
    {
        Assert.Equal(expected, WindowsDevicePath.Normalize(input));
    }

    [Theory]
    [InlineData(@"\\.\D:")]
    [InlineData(@"\\.\Z:")]
    public void Normalize_AlreadyNormalized_PassesThrough(string input)
    {
        Assert.Equal(input, WindowsDevicePath.Normalize(input));
    }

    [Theory]
    [InlineData(@"\\.\d:", @"\\.\D:")]
    public void Normalize_DeviceNamespaceLowercaseLetter_Uppercases(string input, string expected)
    {
        Assert.Equal(expected, WindowsDevicePath.Normalize(input));
    }

    [Theory]
    [InlineData(@"\\.\CdRom0")]
    [InlineData(@"\\.\CdRom1")]
    [InlineData(@"\\.\CdRom10")]
    public void Normalize_CdRomDeviceName_PassesThrough(string input)
    {
        Assert.Equal(input, WindowsDevicePath.Normalize(input));
    }

    [Fact]
    public void Normalize_Null_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => WindowsDevicePath.Normalize(null!));
    }

    [Fact]
    public void Normalize_Empty_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => WindowsDevicePath.Normalize(""));
    }

    [Theory]
    [InlineData("D")]                        // missing colon
    [InlineData("DD:")]                      // too long
    [InlineData("1:")]                       // not a letter
    [InlineData("/dev/sr0")]                 // Linux path
    [InlineData("C:/Windows")]               // full path
    [InlineData(@"\\.\Volume{abc}")]          // not a drive letter or CdRom
    [InlineData(@"\\.\PhysicalDrive0")]      // not CdRom
    [InlineData(@"\\?\Volume{abc}\")]         // Win32 device namespace
    public void Normalize_MalformedInput_ThrowsArgumentException(string input)
    {
        Assert.Throws<ArgumentException>(() => WindowsDevicePath.Normalize(input));
    }
}
