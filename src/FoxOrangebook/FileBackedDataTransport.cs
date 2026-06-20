using System.Buffers.Binary;
using FoxOrangebook.FileSystem;
using FoxRedbook;

namespace FoxOrangebook;

/// <summary>Media type a <see cref="FileBackedDataTransport"/> emulates.</summary>
public enum FileBackedMedia
{
    /// <summary>A blank CD-RW (Track-At-Once Mode 1 write path).</summary>
    CompactDiscRewritable,

    /// <summary>An unformatted DVD+RW (background-format + random write path).</summary>
    DvdPlusRewritable,
}

/// <summary>
/// An <see cref="IScsiTransport"/> that records a data-disc burn to a single
/// <c>.iso</c> file instead of real hardware. WRITE (10) payloads are placed at
/// their LBA offsets, so the finished file is byte-identical to the
/// <see cref="DiscImage"/> that was burned and mounts as a real disc. The
/// emulated <see cref="FileBackedMedia"/> selects which burn path the matching
/// <see cref="DataBurnSession"/> takes.
/// </summary>
public sealed class FileBackedDataTransport : IScsiTransport
{
    private const int SectorSize = DiscImage.LogicalSectorSize;

    private readonly string _path;
    private readonly FileBackedMedia _media;
    private FileStream? _stream;
    private bool _disposed;

    /// <summary>Creates a transport that writes the burned sectors to <paramref name="isoPath"/>.</summary>
    public FileBackedDataTransport(string isoPath, FileBackedMedia media = FileBackedMedia.CompactDiscRewritable)
    {
        ArgumentNullException.ThrowIfNull(isoPath);
        _path = isoPath;
        _media = media;
    }

    private ushort Profile => _media == FileBackedMedia.DvdPlusRewritable ? BurnCommands.ProfileDvdPlusRw : BurnCommands.ProfileCdRw;

    /// <inheritdoc />
    public DriveInquiry Inquiry => new()
    {
        Vendor = "FILE",
        Product = "IsoWriter",
        Revision = "1.0",
    };

    /// <inheritdoc />
    public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        switch (cdb[0])
        {
            case BurnCommands.OpGetConfiguration:
            {
                HandleGetConfiguration(cdb, buffer);
                break;
            }

            case BurnCommands.OpReadDiscInformation:
            {
                HandleReadDiscInformation(buffer);
                break;
            }

            case BurnCommands.OpReadFormatCapacities:
            {
                HandleReadFormatCapacities(buffer);
                break;
            }

            case BurnCommands.OpFormatUnit:
            case BurnCommands.OpSendOpc:
            case BurnCommands.OpModeSelect10:
            case BurnCommands.OpTestUnitReady:
            case BurnCommands.OpSynchronizeCache:
            {
                // Accept silently — nothing to calibrate, format, or flush to a file.
                break;
            }

            case BurnCommands.OpWrite10:
            {
                HandleWrite(cdb, buffer);
                break;
            }

            case BurnCommands.OpCloseTrackSession:
            {
                _stream?.Flush();
                break;
            }

            default:
            {
                break;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _stream?.Dispose();
            _disposed = true;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ── Command handlers ─────────────────────────────────────

    private void HandleGetConfiguration(ReadOnlySpan<byte> cdb, Span<byte> buffer)
    {
        if (buffer.Length >= 12)
        {
            buffer.Clear();
            BinaryPrimitives.WriteUInt32BigEndian(buffer, 8);                 // data length
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(6, 2), Profile); // current profile

            ushort requested = BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(2, 2));
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(8, 2), requested); // echo requested feature
        }
        else if (buffer.Length >= 8)
        {
            buffer.Clear();
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(6, 2), Profile);
        }
    }

    private static void HandleReadDiscInformation(Span<byte> buffer)
    {
        if (buffer.Length >= 34)
        {
            buffer.Clear();
            buffer[2] = (byte)DiscStatus.Blank;
        }
    }

    private static void HandleReadFormatCapacities(Span<byte> buffer)
    {
        if (buffer.Length < 20)
        {
            return;
        }

        buffer.Clear();
        const uint blocks = 2295104; // a representative DVD+RW capacity

        buffer[3] = 16; // capacity list length: current descriptor (8) + one formattable (8)

        // Current/maximum capacity descriptor — unformatted.
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(4, 4), blocks);
        buffer[8] = 0x01; // descriptor type: unformatted
        buffer[10] = 0x08; // block length 2048 (0x000800)

        // Formattable descriptor: DVD+RW background format (type 0x26).
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(12, 4), blocks);
        buffer[16] = BurnCommands.FormatTypeDvdPlusRwBackground << 2;
    }

    private void HandleWrite(ReadOnlySpan<byte> cdb, ReadOnlySpan<byte> data)
    {
        _stream ??= new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.None);

        uint lba = BinaryPrimitives.ReadUInt32BigEndian(cdb.Slice(2, 4));
        long offset = (long)lba * SectorSize;

        if (_stream.Position != offset)
        {
            _stream.Seek(offset, SeekOrigin.Begin);
        }

        _stream.Write(data);
    }
}
