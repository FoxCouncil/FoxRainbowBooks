using System.Buffers.Binary;
using System.Reflection;
using FoxRedbook.Platforms.Common;

namespace FoxRedbook.Tests;

/// <summary>
/// Pure-function tests for <see cref="CdTextCommands"/>. Locks the CRC-16
/// parameters against oracle values from libcdio's reference fixture, pins
/// the READ TOC format 5 CDB byte layout, and exercises the pack parser
/// with synthetic buffers covering truncation, encoding, and tab-indicator
/// semantics. The end-to-end oracle test lives in
/// <see cref="CdTextOracleTests"/>.
/// </summary>
public sealed class CdTextCommandsTests
{
    // ── CRC-16 ─────────────────────────────────────────────────

    [Fact]
    public void Crc16_EmptyBuffer_ReturnsFFFF()
    {
        // init=0x0000, no input, final XOR 0xFFFF => 0xFFFF.
        ushort crc = InvokeCrc16(ReadOnlySpan<byte>.Empty);
        Assert.Equal(0xFFFF, crc);
    }

    [Fact]
    public void Crc16_SingleZeroByte_MatchesShiftedPolyXorFinal()
    {
        // One zero byte: no XOR into the high byte, 8 shifts with no taps,
        // register stays 0x0000, final XOR 0xFFFF => 0xFFFF.
        ushort crc = InvokeCrc16(new byte[] { 0x00 });
        Assert.Equal(0xFFFF, crc);
    }

    [Fact]
    public void Crc16_KnownVector_0x31_ReturnsStandardXmodemInverted()
    {
        // XMODEM (init=0, poly=0x1021) of ASCII '1' = 0x2672 => 0x2672 ^ 0xFFFF = 0xD98D.
        ushort crc = InvokeCrc16(new byte[] { 0x31 });
        Assert.Equal(0xD98D, crc);
    }

    // ── Oracle CRC vectors from libcdio cdtext.cdt ────────────

    // The cdtext.cdt fixture is 96 raw 18-byte packs back-to-back (no MMC
    // 4-byte header, plus a single trailing byte we ignore). Pack 0 is the
    // TITLE pack containing "Joyful Night..." with stored CRC 0xF0F7.

    [Fact]
    public void Crc16_OraclePack0_MatchesStoredCrc()
    {
        byte[] file = LoadCdtFile();
        ReadOnlySpan<byte> pack = GetPack(file, 0);

        // Sanity: this is the TITLE pack.
        Assert.Equal(CdTextCommands.PackTitle, pack[0]);

        ushort stored = BinaryPrimitives.ReadUInt16BigEndian(pack.Slice(16, 2));
        ushort computed = InvokeCrc16(pack.Slice(0, 16));
        Assert.Equal(stored, computed);
        Assert.Equal(0xF0F7, stored); // pinned oracle value
    }

    [Fact]
    public void Crc16_AllOraclePacks_MatchStoredCrcs()
    {
        // Exhaustive sweep — every pack in the reference file must round-trip.
        byte[] file = LoadCdtFile();
        int packCount = GetPackCount(file);

        Assert.True(packCount > 0, "Reference cdtext.cdt file has no packs.");

        for (int i = 0; i < packCount; i++)
        {
            ReadOnlySpan<byte> pack = GetPack(file, i);
            ushort stored = BinaryPrimitives.ReadUInt16BigEndian(pack.Slice(16, 2));
            ushort computed = InvokeCrc16(pack.Slice(0, 16));

            Assert.True(
                stored == computed,
                $"Pack {i}: stored CRC 0x{stored:X4} != computed 0x{computed:X4}");
        }
    }

    // ── READ TOC format 5 CDB ─────────────────────────────────

    [Fact]
    public void BuildReadCdText_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        CdTextCommands.BuildReadCdText(cdb);

        Assert.Equal(0x43, cdb[0]); // READ TOC/PMA/ATIP opcode
        Assert.Equal(0x00, cdb[1]); // MSF=0
        Assert.Equal(0x05, cdb[2]); // format = CD-Text
        Assert.Equal(0x00, cdb[3]); // reserved
        Assert.Equal(0x00, cdb[4]); // reserved
        Assert.Equal(0x00, cdb[5]); // reserved
        Assert.Equal(0x00, cdb[6]); // starting track (ignored for format 5)
        Assert.Equal(0xFF, cdb[7]); // allocation length high
        Assert.Equal(0xFF, cdb[8]); // allocation length low
        Assert.Equal(0x00, cdb[9]); // control
    }

    [Fact]
    public void BuildReadCdText_BufferTooSmall_Throws()
    {
        byte[] cdb = new byte[9];
        Assert.Throws<ArgumentException>(() => CdTextCommands.BuildReadCdText(cdb));
    }

    // ── ParseCdText robustness ────────────────────────────────

    [Fact]
    public void ParseCdText_EmptyBuffer_ReturnsNull()
    {
        Assert.Null(CdTextCommands.ParseCdText(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void ParseCdText_HeaderOnly_ReturnsNull()
    {
        // 4-byte header declaring zero data length — drive supports format 5
        // but this disc has no CD-Text. Must return null, not an empty CdText.
        byte[] response = new byte[4];
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(0, 2), 0);

        Assert.Null(CdTextCommands.ParseCdText(response));
    }

    [Fact]
    public void ParseCdText_TruncatedMidPack_ParsesWholePacksOnly()
    {
        // Build a valid 2-pack response, then pass a buffer that ends
        // partway through pack 1. The parser must round down to whole
        // packs rather than reading garbage.
        byte[] full = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Hello\0", CdTextCommands.CharCodeIso88591),
            (CdTextCommands.PackTitle, 1, 1, "World\0", CdTextCommands.CharCodeIso88591));

        // Slice to 4-byte header + 1 pack + 5 extra bytes (incomplete second pack).
        byte[] truncated = new byte[4 + CdTextCommands.PackSize + 5];
        Array.Copy(full, truncated, truncated.Length);

        // Header still claims 2 packs worth of data — parser must clamp.
        var result = CdTextCommands.ParseCdText(truncated);

        Assert.NotNull(result);
        Assert.Equal("Hello", result!.AlbumTitle);
    }

    // ── ParseCdText basic text extraction ─────────────────────

    [Fact]
    public void ParseCdText_SingleTitlePack_ExtractsAlbumTitle()
    {
        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Hello World\0", CdTextCommands.CharCodeIso88591));

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Equal("Hello World", result!.AlbumTitle);
        Assert.Empty(result.Tracks);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void ParseCdText_DiscAndTrackTitles_AssignsToCorrectTracks()
    {
        // Pack 0 payload "Album\0Track1" fills all 12 bytes; no trailing null.
        // Pack 1 payload "\0" (1 byte, zero-padded) contains the terminator
        // that flushes "Track1" for track 1.
        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Album\0Track1", CdTextCommands.CharCodeIso88591),
            (CdTextCommands.PackTitle, 0, 1, "\0", CdTextCommands.CharCodeIso88591));

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Equal("Album", result!.AlbumTitle);
        Assert.Single(result.Tracks);
        Assert.Equal(1, result.Tracks[0].Number);
        Assert.Equal("Track1", result.Tracks[0].Title);
    }

    [Fact]
    public void ParseCdText_FieldSpansTwoPacks_ConcatenatesAcrossPackBoundary()
    {
        // Title "Long Album Title That Spans Packs\0" is longer than 12 bytes,
        // so it must span two packs. Null terminator appears only in the second.
        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Long Album T", CdTextCommands.CharCodeIso88591),
            (CdTextCommands.PackTitle, 0, 1, "itle Spans\0", CdTextCommands.CharCodeIso88591));

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Equal("Long Album Title Spans", result!.AlbumTitle);
    }

    [Fact]
    public void ParseCdText_TabIndicator_CopiesPreviousTrackValue()
    {
        // Pack 0: "Album\0Track1" (12 bytes, no trailing null).
        // Pack 1: "\0\t\0Track3\0" (10 bytes, zero-padded). The middle tab
        //   between two nulls is track 2's field — it should copy track 1.
        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Album\0Track1", CdTextCommands.CharCodeIso88591),
            (CdTextCommands.PackTitle, 0, 1, "\0\t\0Track3\0", CdTextCommands.CharCodeIso88591));

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Equal(3, result!.Tracks.Count);
        Assert.Equal("Track1", result.Tracks[0].Title);
        Assert.Equal("Track1", result.Tracks[1].Title); // copied from previous
        Assert.Equal("Track3", result.Tracks[2].Title);
    }

    [Fact]
    public void ParseCdText_BadCrc_AddsWarningAndSkipsPack()
    {
        // Build a valid pack, then corrupt the stored CRC.
        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Hello\0", CdTextCommands.CharCodeIso88591));

        // Pack starts at byte 4; CRC is at bytes 16..17 within the pack.
        response[4 + 16] ^= 0xFF;
        response[4 + 17] ^= 0xFF;

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Null(result!.AlbumTitle);
        Assert.NotEmpty(result.Warnings);
        Assert.Contains(result.Warnings, w => w.Contains("invalid CRC", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ParseCdText_BlockSizeInfoPack_IsRecognizedAndNotTreatedAsText()
    {
        // Block size info packs (0x8F) must be skipped in the text-pack loop
        // so they don't pollute AlbumTitle or track fields. The parser should
        // still extract the title from the preceding 0x80 pack normally.
        byte[] blockSizeText = new byte[12];
        blockSizeText[0] = CdTextCommands.CharCodeIso88591;
        blockSizeText[1] = 1; // first track
        blockSizeText[2] = 1; // last track

        byte[] response = BuildSynthResponse(
            (CdTextCommands.PackTitle, 0, 0, "Hello\0", CdTextCommands.CharCodeIso88591),
            (CdTextCommands.PackBlockSize, 0, 0, blockSizeText, CdTextCommands.CharCodeIso88591));

        var result = CdTextCommands.ParseCdText(response);

        Assert.NotNull(result);
        Assert.Equal("Hello", result!.AlbumTitle);
        Assert.Empty(result.Warnings);
    }

    // ── Helpers ──────────────────────────────────────────────

    /// <summary>
    /// Invokes <c>CdTextCommands.Crc16</c> via reflection. The method is
    /// internal and InternalsVisibleTo is already wired, but we use
    /// reflection so the test doesn't need a matching `internal` import —
    /// future-proofs against visibility changes.
    /// </summary>
    private static ushort InvokeCrc16(ReadOnlySpan<byte> data)
    {
        // Since InternalsVisibleTo is set, we can call it directly.
        return CdTextCommands.Crc16(data);
    }

    private static byte[] LoadCdtFile()
    {
        string path = Path.Combine(
            Path.GetDirectoryName(typeof(CdTextCommandsTests).Assembly.Location)!,
            "TestData",
            "cdtext.cdt");

        return File.ReadAllBytes(path);
    }

    private static ReadOnlySpan<byte> GetPack(byte[] file, int index)
    {
        // Raw pack dump: no MMC header, packs start at offset 0.
        int offset = index * CdTextCommands.PackSize;
        return file.AsSpan(offset, CdTextCommands.PackSize);
    }

    private static int GetPackCount(byte[] file)
    {
        // Raw pack dump; ignore any trailing byte that isn't a full pack.
        return file.Length / CdTextCommands.PackSize;
    }

    /// <summary>
    /// Builds a synthetic READ TOC format 5 response with the given packs.
    /// Each pack entry is (type, block, seq, textPayload, charCode) where
    /// textPayload is either a string (ISO-8859-1 encoded) or a byte[] for
    /// raw control. The 12-byte text region is filled with the payload and
    /// zero-padded; the CRC is computed over bytes 0..15.
    /// </summary>
    private static byte[] BuildSynthResponse(params (byte Type, int Block, int Seq, object Payload, byte CharCode)[] packs)
    {
        int packAreaBytes = packs.Length * CdTextCommands.PackSize;
        int totalBytes = 4 + packAreaBytes;
        byte[] response = new byte[totalBytes];

        // Header: data length = packAreaBytes + 2 reserved bytes after itself.
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(0, 2), (ushort)(packAreaBytes + 2));

        for (int i = 0; i < packs.Length; i++)
        {
            Span<byte> pack = response.AsSpan(4 + i * CdTextCommands.PackSize, CdTextCommands.PackSize);

            pack[0] = packs[i].Type;
            pack[1] = 0; // track 0 — we encode via null terminators, not this field
            pack[2] = (byte)packs[i].Seq;
            // byte 3: bit 7 = db_chars, bits 4..6 = block, bits 0..3 = char_pos
            pack[3] = (byte)((packs[i].Block & 0x07) << 4);

            Span<byte> textRegion = pack.Slice(4, CdTextCommands.TextDataLength);
            textRegion.Clear();

            byte[] payloadBytes = packs[i].Payload switch
            {
                string s => System.Text.Encoding.Latin1.GetBytes(s),
                byte[] b => b,
                _ => throw new ArgumentException("Payload must be string or byte[]."),
            };

            int copyLen = Math.Min(payloadBytes.Length, CdTextCommands.TextDataLength);
            payloadBytes.AsSpan(0, copyLen).CopyTo(textRegion);

            ushort crc = CdTextCommands.Crc16(pack.Slice(0, 16));
            BinaryPrimitives.WriteUInt16BigEndian(pack.Slice(16, 2), crc);
        }

        return response;
    }
}
