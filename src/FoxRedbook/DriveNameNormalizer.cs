namespace FoxRedbook;

/// <summary>
/// Canonical normalization for drive vendor and product strings, shared by
/// the build-time snapshot tool and the runtime lookup. Both sides must
/// apply identical rules so that keys match regardless of how the drive
/// reports its name.
/// </summary>
public static class DriveNameNormalizer
{
    /// <summary>
    /// Normalizes a drive name string: strips embedded nulls, trims
    /// leading/trailing whitespace, collapses internal multi-space runs
    /// to a single space, and uppercases everything.
    /// </summary>
    public static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        // Strip embedded nulls first.
        Span<char> buffer = stackalloc char[value.Length];
        int len = 0;

        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] != '\0')
            {
                buffer[len++] = char.ToUpperInvariant(value[i]);
            }
        }

        // Trim leading/trailing whitespace and collapse internal runs.
        int start = 0;
        while (start < len && buffer[start] == ' ')
        {
            start++;
        }

        int end = len - 1;
        while (end >= start && buffer[end] == ' ')
        {
            end--;
        }

        if (start > end)
        {
            return string.Empty;
        }

        Span<char> result = stackalloc char[end - start + 1];
        int writePos = 0;
        bool prevSpace = false;

        for (int i = start; i <= end; i++)
        {
            if (buffer[i] == ' ')
            {
                if (!prevSpace)
                {
                    result[writePos++] = ' ';
                    prevSpace = true;
                }
            }
            else
            {
                result[writePos++] = buffer[i];
                prevSpace = false;
            }
        }

        return new string(result.Slice(0, writePos));
    }

    /// <summary>
    /// Builds the dictionary key from normalized vendor and product strings.
    /// </summary>
    internal static string BuildKey(string normalizedVendor, string normalizedProduct)
    {
        return $"{normalizedVendor}|{normalizedProduct}";
    }
}
