using System.Buffers.Binary;
using System.Text;

namespace FoxRedbook.Platforms.Common;

/// <summary>
/// Pure functions for building the READ TOC format 5 CDB (CD-Text),
/// computing and verifying CD-Text CRC-16, and parsing the 18-byte-pack
/// response into a structured <see cref="CdText"/> record. Shared by all
/// three platform backends.
/// </summary>
/// <remarks>
/// No runtime platform dependencies — fully unit-testable on any host.
/// </remarks>
internal static class CdTextCommands
{
    // ── Constants ──────────────────────────────────────────────

    /// <summary>Size of one CD-Text pack in bytes.</summary>
    internal const int PackSize = 18;

    /// <summary>Size of the text-data region of a pack (bytes 4..15).</summary>
    internal const int TextDataLength = 12;

    /// <summary>Maximum theoretical CD-Text payload: 8 blocks × 256 packs × 18 bytes.</summary>
    internal const int MaxCdTextPayloadBytes = 8 * 256 * PackSize;

    // Pack types (from libcdio's CDTEXT_PACK_* enum, confirmed against MMC-6).
    internal const byte PackTitle = 0x80;
    internal const byte PackPerformer = 0x81;
    internal const byte PackSongwriter = 0x82;
    internal const byte PackComposer = 0x83;
    internal const byte PackArranger = 0x84;
    internal const byte PackMessage = 0x85;
    internal const byte PackDiscId = 0x86;
    internal const byte PackGenre = 0x87;
    internal const byte PackToc = 0x88;
    internal const byte PackToc2 = 0x89;
    internal const byte PackUpcIsrc = 0x8E;
    internal const byte PackBlockSize = 0x8F;

    // Character encodings (only these three are implemented in the wild
    // per libcdio's comment: "The following were proposed but never
    // implemented anywhere" for Korean and Chinese).
    internal const byte CharCodeIso88591 = 0x00;
    internal const byte CharCodeAscii = 0x01;
    internal const byte CharCodeShiftJis = 0x80;

    // Tab indicator (U+0009 = \t) — in single-byte encoding; a track's
    // entire text field consisting of just a tab means "same as the
    // previous track's value for this pack type."
    private const byte TabChar = 0x09;

    // ── READ TOC format 5 CDB ──────────────────────────────────

    /// <summary>
    /// Builds a 10-byte READ TOC CDB requesting format 5 (CD-Text),
    /// with a 65,535-byte allocation length to capture whatever the
    /// drive has (truncation is handled by the drive's data-length
    /// field in the response header).
    /// </summary>
    internal static void BuildReadCdText(Span<byte> cdb)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("READ TOC CDB buffer must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = 0x43; // READ TOC/PMA/ATIP opcode
        // byte 1: MSF=0 (irrelevant for format 5)
        cdb[2] = 0x05; // format = CD-Text (low 4 bits)
        // bytes 3-5: reserved
        // byte 6: starting track (ignored for format 5)
        // bytes 7-8: allocation length, big-endian — request the 16-bit max
        cdb[7] = 0xFF;
        cdb[8] = 0xFF;
        // byte 9: control
    }

    // ── CRC-16 (poly 0x1021, init 0x0000, final XOR 0xFFFF) ───

    /// <summary>
    /// Computes the CD-Text CRC-16 over a data buffer. Algorithm verified
    /// by hand-matching three real packs from libcdio's <c>cdtext.cdt</c>
    /// test fixture (packs with stored CRCs 0xF0F7, 0x431C, 0x43F9).
    /// </summary>
    /// <remarks>
    /// This is NOT CRC-16/CCITT-FALSE despite some sources claiming so.
    /// The actual CD-Text CRC uses:
    /// <list type="bullet">
    ///   <item>Polynomial: 0x1021 (standard CCITT)</item>
    ///   <item>Initial value: 0x0000 (NOT 0xFFFF as CCITT-FALSE uses)</item>
    ///   <item>Input reflection: none</item>
    ///   <item>Output reflection: none</item>
    ///   <item>Final XOR: 0xFFFF (invert the result)</item>
    /// </list>
    /// Changing any of these produces output that no real disc's stored
    /// CRC matches. The test suite locks all three parameters against
    /// oracle values from libcdio's reference data.
    /// </remarks>
    internal static ushort Crc16(ReadOnlySpan<byte> data)
    {
        ushort crc = 0x0000;

        for (int i = 0; i < data.Length; i++)
        {
            crc ^= (ushort)(data[i] << 8);

            for (int j = 0; j < 8; j++)
            {
                if ((crc & 0x8000) != 0)
                {
                    crc = (ushort)((crc << 1) ^ 0x1021);
                }
                else
                {
                    crc = (ushort)(crc << 1);
                }
            }
        }

        return (ushort)(crc ^ 0xFFFF);
    }

    // ── Response parser ────────────────────────────────────────

    /// <summary>
    /// Parses a READ TOC format 5 response buffer into a <see cref="CdText"/>
    /// record. Returns <see langword="null"/> when the response has no
    /// CD-Text data (empty payload after the 4-byte header).
    /// </summary>
    /// <param name="response">The raw response buffer from the drive.</param>
    /// <returns>
    /// A populated <see cref="CdText"/>, or <see langword="null"/> if the
    /// disc has no CD-Text.
    /// </returns>
    internal static CdText? ParseCdText(ReadOnlySpan<byte> response)
    {
        if (response.Length < 4)
        {
            return null;
        }

        // 4-byte header: 2-byte data length (BE, does NOT include itself) + 2 reserved
        int dataLength = BinaryPrimitives.ReadUInt16BigEndian(response.Slice(0, 2));

        if (dataLength < 2)
        {
            // No packs in the response — drive supports format 5 but this disc has no CD-Text.
            return null;
        }

        // dataLength includes the 2 bytes of "reserved" after the length field itself.
        // Pack data starts at byte 4 and runs for (dataLength - 2) bytes.
        int packAreaBytes = dataLength - 2;
        int packAreaStart = 4;

        if (packAreaBytes <= 0 || packAreaBytes % PackSize != 0)
        {
            // Truncated or malformed — best effort: parse what we can by
            // rounding down to a whole number of packs.
            packAreaBytes = Math.Min(
                response.Length - packAreaStart,
                (packAreaBytes / PackSize) * PackSize);

            if (packAreaBytes <= 0)
            {
                return null;
            }
        }

        if (packAreaStart + packAreaBytes > response.Length)
        {
            packAreaBytes = ((response.Length - packAreaStart) / PackSize) * PackSize;

            if (packAreaBytes <= 0)
            {
                return null;
            }
        }

        ReadOnlySpan<byte> packArea = response.Slice(packAreaStart, packAreaBytes);
        int packCount = packAreaBytes / PackSize;

        var warnings = new List<string>();

        // Pass 1: decode block size info from any 0x8F packs (they come last
        // by convention but we scan the whole area to be robust).
        BlockSizeInfo blockInfo = ParseBlockSizeInfo(packArea, packCount, warnings);

        // Pass 2: decode text packs for block 0 only.
        return DecodeBlockZero(packArea, packCount, blockInfo, warnings);
    }

    // ── Block size info extraction ────────────────────────────

    private readonly struct BlockSizeInfo
    {
        public readonly byte CharCode;
        public readonly byte FirstTrack;
        public readonly byte LastTrack;

        public BlockSizeInfo(byte charCode, byte firstTrack, byte lastTrack)
        {
            CharCode = charCode;
            FirstTrack = firstTrack;
            LastTrack = lastTrack;
        }

        public static BlockSizeInfo Default => new(CharCodeIso88591, 1, 99);
    }

    private static BlockSizeInfo ParseBlockSizeInfo(ReadOnlySpan<byte> packArea, int packCount, List<string> warnings)
    {
        // Block size info packs (0x8F) come in groups of 3 per block.
        // We only care about block 0; find its first 0x8F pack (seq 0)
        // and read bytes 0-2 of its text data (charcode, first track, last track).
        for (int i = 0; i < packCount; i++)
        {
            ReadOnlySpan<byte> pack = packArea.Slice(i * PackSize, PackSize);

            if (pack[0] != PackBlockSize)
            {
                continue;
            }

            // byte 3 layout: bit 7 = db_chars, bits 4..6 = block, bits 0..3 = char_pos
            int block = (pack[3] >> 4) & 0x07;
            int seq = pack[2];

            if (block != 0 || seq != 0)
            {
                continue;
            }

            // Verify CRC; if bad, fall back to default and warn.
            if (!VerifyCrc(pack, out _))
            {
                warnings.Add("Block size info pack has invalid CRC; using default (ISO-8859-1, tracks 1-99).");
                return BlockSizeInfo.Default;
            }

            byte charCode = pack[4];
            byte firstTrack = pack[5];
            byte lastTrack = pack[6];

            return new BlockSizeInfo(charCode, firstTrack, lastTrack);
        }

        // No block size info pack present — assume sensible defaults.
        return BlockSizeInfo.Default;
    }

    // ── Text-block decoding ──────────────────────────────────

    private static CdText DecodeBlockZero(
        ReadOnlySpan<byte> packArea,
        int packCount,
        BlockSizeInfo blockInfo,
        List<string> warnings)
    {
        Encoding encoding = SelectEncoding(blockInfo.CharCode, warnings);

        // Text fields accumulate across consecutive packs for the same
        // (pack type, block). Each terminating null flushes a completed
        // field, assigning it to the current track (starting from
        // blockInfo.FirstTrack), then incrementing the cursor.
        //
        // For each pack type we maintain:
        //   - current buffer (bytes being accumulated)
        //   - current track cursor (which track the next null-terminated field belongs to)
        //   - tab buffer (the most recently completed value for this pack type)

        var fieldMap = new Dictionary<byte, FieldAccumulator>();

        // Disc-level DiscId (0x86) and Genre (0x87) are handled specially —
        // their payload has a 2-byte binary prefix before the text.
        byte[]? rawDiscIdBytes = null;
        byte[]? rawGenreBytes = null;

        for (int i = 0; i < packCount; i++)
        {
            ReadOnlySpan<byte> pack = packArea.Slice(i * PackSize, PackSize);

            if (!VerifyCrc(pack, out ushort storedCrc))
            {
                warnings.Add($"Pack {i} has invalid CRC (stored 0x{storedCrc:X4}); skipped.");
                continue;
            }

            byte packType = pack[0];
            int block = (pack[3] >> 4) & 0x07;
            bool doubleByte = (pack[3] >> 7) != 0;

            if (block != 0)
            {
                // Multi-language data for block 1+ — ignored per the "block 0 only" simplification.
                continue;
            }

            if (packType == PackBlockSize || packType == PackToc || packType == PackToc2)
            {
                // 0x8F handled in pass 1; 0x88/0x89 are binary TOC data, not text.
                continue;
            }

            if (doubleByte && blockInfo.CharCode != CharCodeShiftJis)
            {
                warnings.Add($"Pack {i} has double-byte flag set but block 0 encoding is not Shift-JIS; ignoring DBCC.");
                doubleByte = false;
            }

            if (!fieldMap.TryGetValue(packType, out FieldAccumulator? acc))
            {
                acc = new FieldAccumulator(blockInfo.FirstTrack);
                fieldMap[packType] = acc;
            }

            // Feed the 12 bytes of text data into the accumulator for this pack type.
            ReadOnlySpan<byte> textData = pack.Slice(4, TextDataLength);
            acc.Feed(textData, doubleByte, encoding, packType, rawDiscIdBytes, rawGenreBytes, warnings);
        }

        // Translate accumulated fields into CdText / CdTextTrack records.
        return BuildResult(fieldMap, warnings);
    }

    // ── Field accumulator ────────────────────────────────────

    /// <summary>
    /// State machine for one pack type's text field accumulation.
    /// Tracks the current track cursor and the "previous value" buffer
    /// used for tab-indicator expansion.
    /// </summary>
    private sealed class FieldAccumulator
    {
        private readonly List<byte> _buffer = new();
        private string? _previousValue; // decoded, for tab-indicator copying
        private int _currentTrack;

        /// <summary>All completed (track → value) pairs for this pack type.</summary>
        public List<(int Track, string Value)> Completed { get; } = new();

        public FieldAccumulator(int firstTrack)
        {
            _currentTrack = firstTrack;
            // Start before the first track — CD-Text always begins with
            // the disc-level (track 0) record for every pack type, even
            // when firstTrack per block size info is 1.
            if (_currentTrack > 0)
            {
                _currentTrack = 0;
            }
        }

        public void Feed(
            ReadOnlySpan<byte> textData,
            bool doubleByte,
            Encoding encoding,
            byte packType,
            byte[]? rawDiscIdBytes,
            byte[]? rawGenreBytes,
            List<string> warnings)
        {
            int step = doubleByte ? 2 : 1;

            for (int i = 0; i + step <= textData.Length; i += step)
            {
                bool isNull = textData[i] == 0 && (!doubleByte || textData[i + 1] == 0);

                if (!isNull)
                {
                    _buffer.Add(textData[i]);

                    if (doubleByte)
                    {
                        _buffer.Add(textData[i + 1]);
                    }

                    continue;
                }

                // Null terminator — flush the current field.
                string value = DecodeField(_buffer, doubleByte, encoding);

                // Tab indicator: single tab character alone means
                // "copy the previous track's value for this pack type."
                if (value.Length == 1 && value[0] == (char)TabChar)
                {
                    if (_previousValue is null)
                    {
                        warnings.Add(
                            $"Tab indicator on pack type 0x{packType:X2} has no previous value to copy; ignoring.");
                    }
                    else
                    {
                        value = _previousValue;
                    }
                }

                if (value.Length > 0)
                {
                    Completed.Add((_currentTrack, value));
                    _previousValue = value;
                }

                _buffer.Clear();
                _currentTrack++;
            }
        }

        private static string DecodeField(List<byte> buffer, bool doubleByte, Encoding encoding)
        {
            if (buffer.Count == 0)
            {
                return string.Empty;
            }

            // For Shift-JIS and Latin-1 the BCL encoders handle the bytes directly.
            return encoding.GetString(buffer.ToArray());
        }
    }

    // ── Result builder ───────────────────────────────────────

    private static CdText BuildResult(Dictionary<byte, FieldAccumulator> fieldMap, List<string> warnings)
    {
        string? DiscLevel(byte packType)
        {
            if (!fieldMap.TryGetValue(packType, out var acc))
            {
                return null;
            }

            foreach (var (track, value) in acc.Completed)
            {
                if (track == 0)
                {
                    return StripBinaryPrefix(packType, value);
                }
            }

            return null;
        }

        // Collect tracks across all per-track pack types.
        var trackMap = new Dictionary<int, CdTextTrackBuilder>();

        void CollectTrack(byte packType, Action<CdTextTrackBuilder, string> setter)
        {
            if (!fieldMap.TryGetValue(packType, out var acc))
            {
                return;
            }

            foreach (var (track, value) in acc.Completed)
            {
                if (track == 0)
                {
                    continue; // disc-level; handled by DiscLevel
                }

                if (!trackMap.TryGetValue(track, out var tb))
                {
                    tb = new CdTextTrackBuilder { Number = track };
                    trackMap[track] = tb;
                }

                setter(tb, value);
            }
        }

        CollectTrack(PackTitle, (tb, v) => tb.Title = v);
        CollectTrack(PackPerformer, (tb, v) => tb.Performer = v);
        CollectTrack(PackSongwriter, (tb, v) => tb.Songwriter = v);
        CollectTrack(PackComposer, (tb, v) => tb.Composer = v);
        CollectTrack(PackArranger, (tb, v) => tb.Arranger = v);
        CollectTrack(PackMessage, (tb, v) => tb.Message = v);
        CollectTrack(PackUpcIsrc, (tb, v) => tb.Isrc = v);

        var tracks = new List<CdTextTrack>();

        foreach (int trackNum in trackMap.Keys.OrderBy(k => k))
        {
            var tb = trackMap[trackNum];
            tracks.Add(new CdTextTrack
            {
                Number = tb.Number,
                Title = tb.Title,
                Performer = tb.Performer,
                Songwriter = tb.Songwriter,
                Composer = tb.Composer,
                Arranger = tb.Arranger,
                Message = tb.Message,
                Isrc = tb.Isrc,
            });
        }

        return new CdText
        {
            AlbumTitle = DiscLevel(PackTitle),
            AlbumPerformer = DiscLevel(PackPerformer),
            AlbumSongwriter = DiscLevel(PackSongwriter),
            AlbumComposer = DiscLevel(PackComposer),
            AlbumArranger = DiscLevel(PackArranger),
            AlbumMessage = DiscLevel(PackMessage),
            DiscId = DiscLevel(PackDiscId),
            Genre = DiscLevel(PackGenre),
            UpcEan = DiscLevel(PackUpcIsrc),
            Tracks = tracks,
            Warnings = warnings,
        };
    }

    private sealed class CdTextTrackBuilder
    {
        public int Number { get; set; }
        public string? Title { get; set; }
        public string? Performer { get; set; }
        public string? Songwriter { get; set; }
        public string? Composer { get; set; }
        public string? Arranger { get; set; }
        public string? Message { get; set; }
        public string? Isrc { get; set; }
    }

    /// <summary>
    /// DISC_ID (0x86) and GENRE (0x87) packs have a 2-byte binary prefix
    /// before the text payload. Strip it when surfacing the string.
    /// </summary>
    private static string StripBinaryPrefix(byte packType, string value)
    {
        if (packType is PackDiscId or PackGenre && value.Length >= 2)
        {
            return value.Substring(2);
        }

        return value;
    }

    // ── Helpers ──────────────────────────────────────────────

    /// <summary>
    /// Verifies a pack's CRC. Returns true if the stored CRC matches the
    /// computed CRC over bytes 0..15. The computed CRC is not returned —
    /// <paramref name="storedCrc"/> is filled in for diagnostic purposes.
    /// </summary>
    private static bool VerifyCrc(ReadOnlySpan<byte> pack, out ushort storedCrc)
    {
        storedCrc = BinaryPrimitives.ReadUInt16BigEndian(pack.Slice(16, 2));
        ushort computed = Crc16(pack.Slice(0, 16));
        return computed == storedCrc;
    }

    private static Encoding SelectEncoding(byte charCode, List<string> warnings)
    {
        switch (charCode)
        {
            case CharCodeIso88591:
            case CharCodeAscii:
                return Encoding.Latin1;

            case CharCodeShiftJis:
                try
                {
                    return Encoding.GetEncoding("shift_jis");
                }
                catch (ArgumentException)
                {
                    warnings.Add("Shift-JIS encoding not available on this host; falling back to ISO-8859-1.");
                    return Encoding.Latin1;
                }

            default:
                warnings.Add($"Unknown CD-Text character code 0x{charCode:X2}; falling back to ISO-8859-1.");
                return Encoding.Latin1;
        }
    }
}
