using System.Buffers;

namespace FoxRedbook;

/// <summary>
/// <para>
/// Hash table with 65,536 buckets (one per possible 16-bit sample value)
/// for O(1) lookup of sample positions within a cache block. Used by Stage 1
/// to find matching positions between independent reads.
/// </para>
/// <para>
/// The backing sample array must outlive this index — the owning
/// <see cref="CacheBlock"/> enforces this in practice.
/// </para>
/// <para>
/// Uses -1 as the "no entry" sentinel because 0 is a valid array index.
/// </para>
/// </summary>
internal sealed class SampleSortIndex : IDisposable
{
    /// <summary>Sentinel value meaning "no match found" or "end of chain."</summary>
    internal const int NoMatch = -1;

    private const int BucketCount = 65536;

    private readonly short[] _source;
    private readonly int _sourceOffset;
    private readonly int _count;

    private int[]? _heads;
    private int[]? _next;
    private bool _disposed;

    /// <summary>
    /// Builds an index over the specified range of the sample array.
    /// </summary>
    /// <param name="buffer">Backing sample array. Must outlive this index.</param>
    /// <param name="offset">First sample to index within <paramref name="buffer"/>.</param>
    /// <param name="count">Number of samples to index.</param>
    internal SampleSortIndex(short[] buffer, int offset, int count)
    {
        _source = buffer;
        _sourceOffset = offset;
        _count = count;

        _heads = ArrayPool<int>.Shared.Rent(BucketCount);
        _heads.AsSpan(0, BucketCount).Fill(NoMatch);

        _next = ArrayPool<int>.Shared.Rent(count);

        // Build chains from last to first so that FindMatch traverses
        // in forward order (first inserted = first found).
        for (int i = count - 1; i >= 0; i--)
        {
            int bucket = SampleToBucket(buffer[offset + i]);
            _next[i] = _heads[bucket];
            _heads[bucket] = i;
        }
    }

    /// <summary>
    /// Finds the first sample matching <paramref name="value"/> within
    /// ±<paramref name="window"/> of <paramref name="position"/>.
    /// </summary>
    /// <returns>
    /// Index (relative to the indexed range, not the backing array) of the
    /// first match, or <see cref="NoMatch"/> (-1) if no sample with that
    /// value exists within the window.
    /// </returns>
    internal int FindMatch(int position, int window, short value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int lo = Math.Max(0, position - window);
        int hi = Math.Min(_count - 1, position + window);
        int bucket = SampleToBucket(value);
        int idx = _heads![bucket];

        // Walk the chain, skipping entries outside the window.
        // Chain is in forward order, so we can stop early once
        // we pass hi (all subsequent entries are further right).
        while (idx != NoMatch)
        {
            if (idx >= lo && idx <= hi)
            {
                return idx;
            }

            if (idx > hi)
            {
                break;
            }

            idx = _next![idx];
        }

        // Chain was ordered but we started before lo — scan for first in-window.
        // This handles the case where early chain entries are before lo.
        // Re-walk from bucket head.
        idx = _heads[bucket];

        while (idx != NoMatch)
        {
            if (idx > hi)
            {
                return NoMatch;
            }

            if (idx >= lo)
            {
                return idx;
            }

            idx = _next![idx];
        }

        return NoMatch;
    }

    /// <summary>
    /// Finds the next match after a previous <see cref="FindMatch"/> or
    /// <see cref="FindNextMatch"/> result, continuing in the same bucket chain.
    /// </summary>
    /// <returns>
    /// Index of the next match, or <see cref="NoMatch"/> (-1) if there are
    /// no more entries in this bucket.
    /// </returns>
    internal int FindNextMatch(int previousIndex)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (previousIndex < 0 || previousIndex >= _count)
        {
            return NoMatch;
        }

        return _next![previousIndex];
    }

    /// <summary>
    /// Gets the sample value at the given index in the indexed range.
    /// </summary>
    internal short GetSample(int index) => _source[_sourceOffset + index];

    /// <summary>Returns pooled int[] arrays.</summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_heads is not null)
            {
                ArrayPool<int>.Shared.Return(_heads);
                _heads = null;
            }

            if (_next is not null)
            {
                ArrayPool<int>.Shared.Return(_next);
                _next = null;
            }

            _disposed = true;
        }
    }

    /// <summary>
    /// Maps a 16-bit signed sample to a bucket index [0, 65535].
    /// Adds 32768 to convert from signed [-32768, 32767] to unsigned [0, 65535].
    /// </summary>
    private static int SampleToBucket(short value) => value - short.MinValue;
}
