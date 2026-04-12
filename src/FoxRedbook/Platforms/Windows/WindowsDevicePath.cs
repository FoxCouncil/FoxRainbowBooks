namespace FoxRedbook.Platforms.Windows;

/// <summary>
/// Pure helper for normalizing Windows optical drive device paths.
/// Consumers may pass a drive letter (<c>D:</c>, <c>d:</c>), a
/// drive letter in the Win32 device namespace (<c>\\.\D:</c>), or a
/// direct device object name (<c>\\.\CdRom0</c>). All forms are
/// normalized to what <c>CreateFileW</c> expects.
/// </summary>
/// <remarks>
/// Deliberately NOT annotated with <c>[SupportedOSPlatform("windows")]</c>
/// — the function is pure string manipulation and runs identically on
/// any host, so tests can exercise it on Linux/macOS CI runners.
/// </remarks>
internal static class WindowsDevicePath
{
    /// <summary>
    /// Normalizes a user-supplied device path to the form expected by
    /// <c>CreateFileW</c>.
    /// </summary>
    /// <param name="input">Drive letter, device-namespace drive letter, or CdRom device object.</param>
    /// <returns>The normalized path (e.g., <c>\\.\D:</c> or <c>\\.\CdRom0</c>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="input"/> is null.</exception>
    /// <exception cref="ArgumentException">The input does not match any recognized form.</exception>
    internal static string Normalize(string input)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (input.Length == 0)
        {
            throw new ArgumentException("Device path must not be empty.", nameof(input));
        }

        // Already in device namespace — pass through with an uppercase
        // drive letter if present, otherwise unchanged.
        if (input.StartsWith(@"\\.\", StringComparison.Ordinal))
        {
            string tail = input.Substring(4);

            if (IsDriveLetterWithColon(tail))
            {
                return @"\\.\" + char.ToUpperInvariant(tail[0]) + ":";
            }

            if (IsCdRomDeviceName(tail))
            {
                return input;
            }

            throw new ArgumentException(
                $"Device path '{input}' is in the Windows device namespace but does not name a drive letter or CdRom device.",
                nameof(input));
        }

        // Bare drive letter like "D:" or "d:" — prepend \\.\
        if (IsDriveLetterWithColon(input))
        {
            return @"\\.\" + char.ToUpperInvariant(input[0]) + ":";
        }

        throw new ArgumentException(
            $"Device path '{input}' is not a recognized Windows optical drive path. " +
            @"Expected a drive letter (D:), a device-namespace drive letter (\\.\D:), " +
            @"or a device object name (\\.\CdRom0).",
            nameof(input));
    }

    private static bool IsDriveLetterWithColon(string s)
    {
        return s.Length == 2
            && char.IsAsciiLetter(s[0])
            && s[1] == ':';
    }

    private static bool IsCdRomDeviceName(string s)
    {
        if (!s.StartsWith("CdRom", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        string suffix = s.Substring(5);

        if (suffix.Length == 0)
        {
            return false;
        }

        foreach (char c in suffix)
        {
            if (!char.IsAsciiDigit(c))
            {
                return false;
            }
        }

        return true;
    }
}
