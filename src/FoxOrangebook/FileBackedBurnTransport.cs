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
    private readonly MemoryStream _cdTextLeadIn = new();
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

    /// <summary>
    /// Size of the simulated CD-TEXT lead-in region in 96-byte sectors.
    /// Modeled like the Pioneer BDR-XS07U: READ TRACK INFORMATION reports
    /// NWA -150 (the pregap, useless for lead-in writes) and the ATIP
    /// reports a start-of-lead-in of -150 minus this value. Real media
    /// lead-ins run to ~11,000 sectors; the default keeps simulated
    /// output small.
    /// </summary>
    public int CdTextLeadInSectors { get; init; } = 75;

    /// <summary>
    /// Raw 6-bit subchannel data captured from lead-in writes (LBA below
    /// -150) — the CD-TEXT pack stream as the drive would receive it.
    /// Empty when the burn carried no CD-TEXT. Not part of the .bin file.
    /// </summary>
    public ReadOnlyMemory<byte> CdTextLeadInData => _cdTextLeadIn.ToArray();

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

            case BurnCommands.OpReadTrackInformation:
            {
                HandleReadTrackInformation(buffer);
                break;
            }

            case BurnCommands.OpSendOpc:
            case BurnCommands.OpModeSelect10:
            {
                // Accept silently — no hardware to calibrate or configure.
                break;
            }

            case BurnCommands.OpReadTocPmaAtip:
            {
                HandleReadTocPmaAtip(cdb, buffer);
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
            _cdTextLeadIn.Dispose();
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

            // Last Possible Lead-Out Start Address: model an 80-minute
            // blank (79:59:74 ≈ 359,849 program sectors).
            buffer[21] = 79;
            buffer[22] = 59;
            buffer[23] = 74;
        }
    }

    private static void HandleReadTrackInformation(Span<byte> buffer)
    {
        if (buffer.Length >= 16)
        {
            buffer.Clear();
            buffer[7] = 0x01; // NWA valid

            // Like the Pioneer BDR-XS07U, the NWA (even for the invisible
            // track) is -150 — the pregap start, never the lead-in. The
            // lead-in address comes from the ATIP instead.
            BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(12, 4), unchecked((uint)-150));
        }
    }

    /// <summary>
    /// Answers READ TOC/PMA/ATIP format 4 (ATIP) with a start-of-lead-in
    /// of -150 minus <see cref="CdTextLeadInSectors"/>, expressed as MSF
    /// in the high-minute wrap range (lba + 450,150). Other formats are
    /// ignored.
    /// </summary>
    private void HandleReadTocPmaAtip(ReadOnlySpan<byte> cdb, Span<byte> buffer)
    {
        if ((cdb[2] & 0x0F) != 0x04 || buffer.Length < 11)
        {
            return;
        }

        buffer.Clear();
        buffer[1] = (byte)(buffer.Length - 2); // ATIP data length

        int leadInFrames = 450150 + (-150 - CdTextLeadInSectors);
        buffer[8] = (byte)(leadInFrames / (60 * 75));       // min
        buffer[9] = (byte)(leadInFrames / 75 % 60);         // sec
        buffer[10] = (byte)(leadInFrames % 75);             // frame
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
        int lba = unchecked((int)BinaryPrimitives.ReadUInt32BigEndian(cdb.Slice(2, 4)));

        if (lba < -150)
        {
            // CD-TEXT lead-in sectors (96 bytes of 6-bit subchannel data
            // each) — captured separately, not part of the .bin file.
            _cdTextLeadIn.Write(data);
            return;
        }

        // Program area writes start at LBA -150 (track 1's pregap), which
        // maps to file offset 0 so the .bin models the full program area.
        _binStream ??= new FileStream(_binPath, FileMode.Create, FileAccess.Write);
        long offset64 = (lba + 150L) * CdConstants.SectorSize;
        _binStream.Seek(offset64, SeekOrigin.Begin);
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
