namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// Pure helper for normalizing macOS optical drive BSD device names.
/// Consumers may pass <c>disk1</c>, <c>/dev/disk1</c>, or similar; the
/// IOKit <c>IOBSDNameMatching</c> function expects the bare BSD name
/// without the <c>/dev/</c> prefix, so we strip it here.
/// </summary>
/// <remarks>
/// Deliberately NOT annotated with <c>[SupportedOSPlatform("macos")]</c> —
/// the function is pure string manipulation and runs identically on any
/// host, so tests can exercise it on Linux/Windows CI runners.
/// </remarks>
internal static class MacBsdName
{
    /// <summary>
    /// Normalizes a user-supplied BSD name to the bare form expected by
    /// <c>IOBSDNameMatching</c>.
    /// </summary>
    /// <param name="input">Either <c>disk1</c> or <c>/dev/disk1</c>.</param>
    /// <returns>The bare BSD name (e.g., <c>disk1</c>).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="input"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Input is empty, contains a path separator other than the <c>/dev/</c>
    /// prefix, or doesn't match the expected BSD name format.
    /// </exception>
    internal static string Normalize(string input)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (input.Length == 0)
        {
            throw new ArgumentException("BSD name must not be empty.", nameof(input));
        }

        // Strip the /dev/ prefix if present
        string candidate = input;

        if (candidate.StartsWith("/dev/", StringComparison.Ordinal))
        {
            candidate = candidate.Substring(5);
        }

        // The bare form must not contain any path separators and must look
        // like a BSD device name (letters + digits, e.g., "disk1", "disk2s1").
        if (candidate.Length == 0)
        {
            throw new ArgumentException(
                $"BSD name '{input}' has an empty name component after the /dev/ prefix.",
                nameof(input));
        }

        if (candidate.Contains('/', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"BSD name '{input}' must be a bare device name or a /dev/ path, not a deeper filesystem path.",
                nameof(input));
        }

        // Must start with a letter (BSD convention) and contain only ASCII letters and digits
        if (!char.IsAsciiLetter(candidate[0]))
        {
            throw new ArgumentException(
                $"BSD name '{input}' must start with an ASCII letter.",
                nameof(input));
        }

        foreach (char c in candidate)
        {
            if (!char.IsAsciiLetterOrDigit(c))
            {
                throw new ArgumentException(
                    $"BSD name '{input}' may only contain ASCII letters and digits.",
                    nameof(input));
            }
        }

        return candidate;
    }
}
