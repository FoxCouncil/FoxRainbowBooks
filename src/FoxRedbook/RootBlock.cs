using System.Buffers;

namespace FoxRedbook;

/// <summary>
/// Canonical verified output buffer. Only data that has been cross-verified
/// and rift-corrected lives here. Sectors are returned to the caller from
/// this buffer. Backed by a pooled array that grows as fragments merge and
/// compacts as returned data is trimmed.
/// </summary>
internal sealed class RootBlock : IDisposable
{
    private short[]? _buffer;
    private int _offset;
    private int _size;
    private int _capacity;
    private bool _disposed;

    /// <summary>
    /// Creates a root block with an initial pooled buffer.
    /// </summary>
    /// <param name="initialCapacity">Initial capacity in samples.</param>
    internal RootBlock(int initialCapacity)
    {
        _buffer = ArrayPool<short>.Shared.Rent(initialCapacity);
        _capacity = _buffer.Length;
    }

    /// <summary>
    /// Verified audio sample data. Only valid while this block is not disposed.
    /// </summary>
    internal Span<short> Samples
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _buffer.AsSpan(_offset, _size);
        }
    }

    /// <summary>Absolute position of the first sample in the audio stream. -1 if empty.</summary>
    internal long Begin { get; set; } = -1;

    /// <summary>Number of valid samples.</summary>
    internal int Size
    {
        get => _size;
        set => _size = value;
    }

    /// <summary>Absolute position one past the last valid sample.</summary>
    internal long End => Begin >= 0 ? Begin + _size : -1;

    /// <summary>
    /// Highest absolute sample position that has been copied out to the caller.
    /// Trim logic uses this to discard data behind the read cursor.
    /// </summary>
    internal long ReturnedLimit { get; set; } = -1;

    /// <summary>Whether the root contains any data.</summary>
    internal bool IsEmpty => _size == 0 || Begin < 0;

    /// <summary>
    /// Checks whether the root fully covers the given sample range.
    /// </summary>
    internal bool Covers(long beginWord, long endWord) =>
        !IsEmpty && Begin <= beginWord && End >= endWord;

    /// <summary>
    /// Initializes the root from a fragment, replacing any existing data.
    /// </summary>
    internal void InitializeFrom(VerifiedFragment fragment)
    {
        EnsureSpace(fragment.Size);
        _offset = 0;
        _size = fragment.Size;
        Begin = fragment.Begin;
        fragment.Samples.CopyTo(_buffer.AsSpan(0, fragment.Size));
    }

    /// <summary>
    /// Appends verified samples to the end of the root.
    /// </summary>
    internal void Append(ReadOnlySpan<short> samples)
    {
        EnsureSpace(_size + samples.Length);
        samples.CopyTo(_buffer.AsSpan(_offset + _size));
        _size += samples.Length;
    }

    /// <summary>
    /// Inserts samples at a position within the root (for rift repair
    /// of dropped samples).
    /// </summary>
    /// <param name="index">Index relative to the start of valid data.</param>
    /// <param name="samples">Samples to insert.</param>
    internal void Insert(int index, ReadOnlySpan<short> samples)
    {
        EnsureSpace(_size + samples.Length);
        var data = _buffer.AsSpan(_offset);
        data.Slice(index, _size - index).CopyTo(data.Slice(index + samples.Length));
        samples.CopyTo(data.Slice(index));
        _size += samples.Length;
    }

    /// <summary>
    /// Removes samples at a position within the root (for rift repair
    /// of duplicated/stuttered samples).
    /// </summary>
    /// <param name="index">Index relative to the start of valid data.</param>
    /// <param name="count">Number of samples to remove.</param>
    internal void Remove(int index, int count)
    {
        var data = _buffer.AsSpan(_offset);
        data.Slice(index + count, _size - index - count).CopyTo(data.Slice(index));
        _size -= count;
    }

    /// <summary>
    /// Drops all data, leaving the root in the empty state. Used when the
    /// engine seeks to a position outside the current root's extent and
    /// the existing data would otherwise be stitched onto new reads with
    /// misaligned absolute positions.
    /// </summary>
    internal void Clear()
    {
        _offset = 0;
        _size = 0;
        Begin = -1;
        ReturnedLimit = -1;
    }

    /// <summary>
    /// Discards all samples before the given absolute position.
    /// Compacts the buffer if the dead space exceeds half capacity.
    /// </summary>
    internal void TrimBefore(long absolutePosition)
    {
        if (IsEmpty || absolutePosition <= Begin)
        {
            return;
        }

        int samplesToTrim = (int)Math.Min(absolutePosition - Begin, _size);
        _offset += samplesToTrim;
        _size -= samplesToTrim;
        Begin += samplesToTrim;

        if (_size == 0)
        {
            _offset = 0;
            Begin = -1;
            return;
        }

        if (_offset > _capacity / 2)
        {
            Compact();
        }
    }

    /// <summary>Returns pooled buffer.</summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            if (_buffer is not null)
            {
                ArrayPool<short>.Shared.Return(_buffer);
                _buffer = null;
            }

            _disposed = true;
        }
    }

    private void Compact()
    {
        if (_offset > 0 && _size > 0)
        {
            _buffer.AsSpan(_offset, _size).CopyTo(_buffer.AsSpan());
        }

        _offset = 0;
    }

    private void EnsureSpace(int requiredSize)
    {
        int totalRequired = _offset + requiredSize;

        if (totalRequired <= _capacity)
        {
            return;
        }

        if (_offset > 0)
        {
            Compact();

            if (requiredSize <= _capacity)
            {
                return;
            }
        }

        int newCapacity = Math.Max(requiredSize, _capacity * 2);
        short[] newBuffer = ArrayPool<short>.Shared.Rent(newCapacity);
        _buffer.AsSpan(_offset, _size).CopyTo(newBuffer);
        ArrayPool<short>.Shared.Return(_buffer!);
        _buffer = newBuffer;
        _capacity = newBuffer.Length;
        _offset = 0;
    }
}
