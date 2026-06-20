namespace FoxOrangebook.FileSystem;

/// <summary>
/// A seekable, read-only stream over a <see cref="DiscImage"/>'s segment table.
/// In-memory segments are served directly; file-backed segments are opened on
/// demand and kept open while reads stay within the same segment, so sequential
/// reads (the common case for writing or burning) don't reopen per call.
/// </summary>
internal sealed class DiscImageStream : Stream
{
    private readonly Segment[] _segments;
    private readonly long[] _starts;
    private readonly long _length;

    private long _position;
    private int _openIndex = -1;
    private Stream? _openStream;

    public DiscImageStream(Segment[] segments, long length)
    {
        _segments = segments;
        _length = length;
        _starts = new long[segments.Length];

        long offset = 0;

        for (int i = 0; i < segments.Length; i++)
        {
            _starts[i] = offset;
            offset += segments[i].Length;
        }
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _position = value;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        if (_position >= _length || buffer.IsEmpty)
        {
            return 0;
        }

        int index = FindSegment(_position);
        var seg = _segments[index];
        long localOffset = _position - _starts[index];
        long segRemaining = seg.Length - localOffset;

        int toRead = (int)Math.Min(Math.Min(buffer.Length, segRemaining), _length - _position);
        var dst = buffer.Slice(0, toRead);

        if (localOffset >= seg.DataLength)
        {
            // Entirely within zero padding (or a zero gap).
            dst.Clear();
        }
        else
        {
            long dataAvail = seg.DataLength - localOffset;
            int dataPart = (int)Math.Min(toRead, dataAvail);

            if (seg.Bytes is not null)
            {
                seg.Bytes.AsSpan((int)localOffset, dataPart).CopyTo(dst);
            }
            else
            {
                EnsureOpen(index);
                _openStream!.Seek(localOffset, SeekOrigin.Begin);
                _openStream.ReadExactly(dst.Slice(0, dataPart));
            }

            if (dataPart < toRead)
            {
                dst.Slice(dataPart).Clear();
            }
        }

        _position += toRead;
        return toRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        long target = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin)),
        };

        if (target < 0)
        {
            throw new IOException("Cannot seek before the start of the stream.");
        }

        _position = target;
        return _position;
    }

    public override void Flush()
    {
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _openStream?.Dispose();
            _openStream = null;
            _openIndex = -1;
        }

        base.Dispose(disposing);
    }

    private void EnsureOpen(int index)
    {
        if (_openIndex == index && _openStream is not null)
        {
            return;
        }

        _openStream?.Dispose();
        _openStream = _segments[index].Source!.Open();
        _openIndex = index;
    }

    private int FindSegment(long position)
    {
        int lo = 0;
        int hi = _segments.Length - 1;

        while (lo < hi)
        {
            int mid = (lo + hi + 1) / 2;

            if (_starts[mid] <= position)
            {
                lo = mid;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return lo;
    }
}
