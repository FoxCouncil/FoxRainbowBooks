using System.Buffers;

namespace FoxRedbook;

/// <summary>
/// Contiguous run of cross-verified samples extracted from a <see cref="CacheBlock"/>
/// after Stage 1 confirmation. Trimmed by <see cref="WiggleConstants.OverlapAdj"/>
/// on each end to ensure detectable gaps between regions from independent
/// verification runs. Backed by a pooled array — call <see cref="Dispose"/> to return it.
/// </summary>
internal sealed class VerifiedFragment : IDisposable
{
    private short[]? _samples;
    private readonly int _size;
    private bool _disposed;

    /// <summary>
    /// Creates a new fragment with a pooled backing array.
    /// </summary>
    /// <param name="sampleCount">Number of verified samples in this fragment.</param>
    internal VerifiedFragment(int sampleCount)
    {
        _samples = ArrayPool<short>.Shared.Rent(sampleCount);
        _size = sampleCount;
    }

    /// <summary>
    /// Verified audio sample data. Only valid while this fragment is not disposed.
    /// </summary>
    internal Span<short> Samples
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _samples.AsSpan(0, _size);
        }
    }

    /// <summary>Absolute position of the first sample in the audio stream.</summary>
    internal long Begin { get; set; }

    /// <summary>Number of valid samples. Fixed at extraction time.</summary>
    internal int Size => _size;

    /// <summary>Absolute position one past the last valid sample.</summary>
    internal long End => Begin + _size;

    /// <summary>Returns pooled array.</summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_samples is not null)
            {
                ArrayPool<short>.Shared.Return(_samples);
                _samples = null;
            }

            _disposed = true;
        }
    }
}
