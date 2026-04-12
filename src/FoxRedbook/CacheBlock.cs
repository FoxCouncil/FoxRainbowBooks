using System.Buffers;
using System.Runtime.InteropServices;

namespace FoxRedbook;

/// <summary>
/// Raw audio from a single drive read. Multiple blocks are cached
/// simultaneously for cross-verification. Backed by pooled arrays —
/// call <see cref="Dispose"/> to return them.
/// </summary>
internal sealed class CacheBlock : IDisposable
{
    private short[]? _samples;
    private byte[]? _flags;
    private readonly int _size;
    private bool _disposed;

    /// <summary>
    /// Creates a new cache block with pooled backing arrays.
    /// Flags are initialized to <see cref="SampleFlags.None"/>.
    /// </summary>
    /// <param name="sampleCount">Number of 16-bit samples this block holds.</param>
    internal CacheBlock(int sampleCount)
    {
        _samples = ArrayPool<short>.Shared.Rent(sampleCount);
        _flags = ArrayPool<byte>.Shared.Rent(sampleCount);
        _flags.AsSpan(0, sampleCount).Clear();
        _size = sampleCount;
    }

    /// <summary>
    /// Audio sample data. Only valid while this block is not disposed.
    /// </summary>
    internal Span<short> Samples
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _samples.AsSpan(0, _size);
        }
    }

    /// <summary>
    /// The raw backing array for samples. Must outlive any
    /// <see cref="SampleSortIndex"/> built from it.
    /// </summary>
    internal short[] SamplesArray
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _samples!;
        }
    }

    /// <summary>
    /// Per-sample verification state. Only valid while this block is not disposed.
    /// </summary>
    internal Span<SampleFlags> Flags
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return MemoryMarshal.Cast<byte, SampleFlags>(_flags.AsSpan(0, _size));
        }
    }

    /// <summary>Absolute position of the first sample in the audio stream.</summary>
    internal long Begin { get; set; }

    /// <summary>Number of valid samples. Fixed at construction.</summary>
    internal int Size => _size;

    /// <summary>Absolute position one past the last valid sample.</summary>
    internal long End => Begin + _size;

    /// <summary>Returns pooled arrays.</summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_samples is not null)
            {
                ArrayPool<short>.Shared.Return(_samples);
                _samples = null;
            }

            if (_flags is not null)
            {
                ArrayPool<byte>.Shared.Return(_flags);
                _flags = null;
            }

            _disposed = true;
        }
    }
}
