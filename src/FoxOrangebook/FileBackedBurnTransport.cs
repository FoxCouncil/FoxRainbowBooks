using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using FoxRedbook;

namespace FoxOrangebook;

/// <summary>
/// An <see cref="IScsiTransport"/> that writes burn output to a .bin/.cue
/// file pair instead of real hardware. Responds to the DAO command sequence
/// (GET CONFIGURATION, READ DISC INFO, OPC, MODE SELECT, SEND CUE SHEET,
/// WRITE(10), CLOSE SESSION) and produces standard bin/cue files playable
/// in media players and burnable to disc by other tools.
/// </summary>
public sealed class FileBackedBurnTransport : IScsiTransport
{
    private readonly string _binPath;
    private readonly string _cuePath;
    private FileStream? _binStream;
    private readonly List<CueSheetEntry> _cueEntries = new();
    private bool _closed;
    private bool _disposed;

    /// <summary>
    /// Creates a transport that writes to the given file paths.
    /// </summary>
    /// <param name="binPath">Path for the raw sector data (.bin).</param>
    public FileBackedBurnTransport(string binPath)
    {
        ArgumentNullException.ThrowIfNull(binPath);
        _binPath = binPath;
        _cuePath = Path.ChangeExtension(binPath, ".cue");
    }

    /// <summary>Disc title for the cue sheet header.</summary>
    public string? DiscTitle { get; set; }

    /// <summary>Disc performer for the cue sheet header.</summary>
    public string? DiscPerformer { get; set; }

    /// <summary>
    /// Per-track metadata for the cue sheet. Index matches track order
    /// (element 0 = track 1). Set before calling <see cref="BurnSession.BurnAsync"/>.
    /// </summary>
    public IReadOnlyList<(string? Title, string? Performer)> TrackMetadata { get; set; } = Array.Empty<(string?, string?)>();

    /// <inheritdoc />
    public DriveInquiry Inquiry => new()
    {
        Vendor = "FILE",
        Product = "BinCueWriter",
        Revision = "1.0",
    };

    /// <inheritdoc />
    public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        byte opcode = cdb[0];

        switch (opcode)
        {
            case BurnCommands.OpGetConfiguration:
            {
                HandleGetConfiguration(buffer);
                break;
            }

            case BurnCommands.OpReadDiscInformation:
            {
                HandleReadDiscInformation(buffer);
                break;
            }

            case BurnCommands.OpSendOpc:
            case BurnCommands.OpModeSelect10:
            {
                // Accept silently — no hardware to calibrate or configure.
                break;
            }

            case BurnCommands.OpSendCueSheet:
            {
                HandleSendCueSheet(buffer);
                break;
            }

            case BurnCommands.OpWrite10:
            {
                HandleWrite(cdb, buffer);
                break;
            }

            case BurnCommands.OpCloseTrackSession:
            {
                HandleClose();
                break;
            }

            default:
            {
                // Unknown command — ignore for file-backed simulation.
                break;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            if (!_closed && _cueEntries.Count > 0)
            {
                WriteCueFile();
            }

            _binStream?.Dispose();
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

    private static void HandleGetConfiguration(Span<byte> buffer)
    {
        if (buffer.Length >= 12)
        {
            buffer.Clear();
            BinaryPrimitives.WriteUInt32BigEndian(buffer, 8);
            BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(8, 2), BurnCommands.FeatureCdMastering);
        }
    }

    private static void HandleReadDiscInformation(Span<byte> buffer)
    {
        if (buffer.Length >= 34)
        {
            buffer.Clear();
            buffer[2] = 0x00; // Blank disc
        }
    }

    private void HandleSendCueSheet(ReadOnlySpan<byte> data)
    {
        _cueEntries.Clear();

        int entryCount = data.Length / BurnCommands.CueSheetEntrySize;

        for (int i = 0; i < entryCount; i++)
        {
            int offset = i * BurnCommands.CueSheetEntrySize;

            _cueEntries.Add(new CueSheetEntry
            {
                CtlAdr = data[offset],
                TrackNumber = data[offset + 1],
                Index = data[offset + 2],
                DataForm = data[offset + 3],
                Scms = data[offset + 4],
                Minute = data[offset + 5],
                Second = data[offset + 6],
                Frame = data[offset + 7],
            });
        }
    }

    private void HandleWrite(ReadOnlySpan<byte> cdb, ReadOnlySpan<byte> data)
    {
        _binStream ??= new FileStream(_binPath, FileMode.Create, FileAccess.Write);
        _binStream.Write(data);
    }

    private void HandleClose()
    {
        _binStream?.Dispose();
        _binStream = null;
        WriteCueFile();
        _closed = true;
    }

    private void WriteCueFile()
    {
        var sb = new StringBuilder();

        if (DiscPerformer is not null)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"PERFORMER \"{DiscPerformer}\"");
        }

        if (DiscTitle is not null)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"TITLE \"{DiscTitle}\"");
        }

        string binFileName = Path.GetFileName(_binPath);
        sb.AppendLine(CultureInfo.InvariantCulture, $"FILE \"{binFileName}\" BINARY");

        // Find the first data position for file-relative MSF conversion.
        int firstDataFrame = 0;

        foreach (var e in _cueEntries)
        {
            if (e.TrackNumber != CueSheetEntry.LeadInTrack && e.TrackNumber != CueSheetEntry.LeadOutTrack)
            {
                firstDataFrame = e.Minute * 60 * 75 + e.Second * 75 + e.Frame;
                break;
            }
        }

        // Group entries by track number, emit TRACK line first, then indices.
        byte currentTrack = 0;

        foreach (var entry in _cueEntries)
        {
            if (entry.TrackNumber == CueSheetEntry.LeadInTrack || entry.TrackNumber == CueSheetEntry.LeadOutTrack)
            {
                continue;
            }

            if (entry.TrackNumber != currentTrack)
            {
                currentTrack = entry.TrackNumber;
                sb.AppendLine(CultureInfo.InvariantCulture, $"  TRACK {currentTrack:D2} AUDIO");

                int trackIdx = currentTrack - 1;

                if (trackIdx < TrackMetadata.Count)
                {
                    var (title, performer) = TrackMetadata[trackIdx];

                    if (title is not null)
                    {
                        sb.AppendLine(CultureInfo.InvariantCulture, $"    TITLE \"{title}\"");
                    }

                    if (performer is not null)
                    {
                        sb.AppendLine(CultureInfo.InvariantCulture, $"    PERFORMER \"{performer}\"");
                    }
                }
            }

            int absFrames = entry.Minute * 60 * 75 + entry.Second * 75 + entry.Frame;
            int relFrames = Math.Max(0, absFrames - firstDataFrame);
            int relMin = relFrames / 75 / 60;
            int relSec = (relFrames / 75) % 60;
            int relFrame = relFrames % 75;

            sb.AppendLine(CultureInfo.InvariantCulture, $"    INDEX {entry.Index:D2} {relMin:D2}:{relSec:D2}:{relFrame:D2}");
        }

        File.WriteAllText(_cuePath, sb.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }
}
