using System.Buffers.Binary;
using FoxOrangebook;

namespace FoxOrangebook.Tests;

public sealed class CdTextEncoderTests
{
    // ── CRC-16 known-answer vectors ──────────────────────────
    //
    // Complete packs lifted verbatim from libcdio's cdtext.cdt reference
    // dump (a rip of a real CD-TEXT disc): bytes 0-15 are the CRC input,
    // bytes 16-17 the stored big-endian CRC.

    public static TheoryData<byte[]> RealDiscPacks => new()
    {
        // Title pack 0: "Joyful Night" (first 12 chars of the album title)
        new byte[] { 0x80, 0x00, 0x00, 0x00, 0x4A, 0x6F, 0x79, 0x66, 0x75, 0x6C, 0x20, 0x4E, 0x69, 0x67, 0x68, 0x74, 0xF0, 0xF7 },
        // Title pack 1: continuation "s" at character position 12, then track 1 title
        new byte[] { 0x80, 0x00, 0x01, 0x0C, 0x73, 0x00, 0x53, 0x6F, 0x6E, 0x67, 0x20, 0x6F, 0x66, 0x20, 0x4A, 0x6F, 0x43, 0x1C },
        // Size info pack 0 of block 0
        new byte[] { 0x8F, 0x00, 0x2B, 0x00, 0x01, 0x01, 0x03, 0x00, 0x05, 0x06, 0x06, 0x05, 0x03, 0x06, 0x01, 0x02, 0x62, 0x42 },
    };

    [Theory]
    [MemberData(nameof(RealDiscPacks))]
    public void Crc16_MatchesRealDiscStoredCrc(byte[] pack)
    {
        ushort stored = BinaryPrimitives.ReadUInt16BigEndian(pack.AsSpan(16, 2));
        ushort computed = CdTextEncoder.Crc16(pack.AsSpan(0, 16));

        Assert.Equal(stored, computed);
    }

    // ── Pack generation basics ───────────────────────────────

    [Fact]
    public void GeneratePacks_NoMetadata_ReturnsNull()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks(null, null, new string?[] { null, null }, new string?[] { null, null });

        Assert.Null(packs);
    }

    [Fact]
    public void GeneratePacks_EmptyStrings_ReturnsNull()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("", "", new string?[] { "" }, new string?[] { "" });

        Assert.Null(packs);
    }

    [Fact]
    public void GeneratePacks_MismatchedTrackLists_Throws()
    {
        Assert.Throws<ArgumentException>(() => CdTextEncoder.GeneratePacks("A", null, new string?[] { "x" }, new string?[] { "y", "z" }));
    }

    [Fact]
    public void GeneratePacks_AllPacksHaveValidCrc()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album Title", "Album Artist", new string?[] { "Track One", "Track Two" }, new string?[] { "Artist A", "Artist B" });

        Assert.NotNull(packs);
        Assert.Equal(0, packs!.Length % 18);

        for (int i = 0; i < packs.Length; i += 18)
        {
            ushort stored = BinaryPrimitives.ReadUInt16BigEndian(packs.AsSpan(i + 16, 2));
            ushort computed = CdTextEncoder.Crc16(packs.AsSpan(i, 16));
            Assert.Equal(stored, computed);
        }
    }

    [Fact]
    public void GeneratePacks_SequenceNumbersAreContinuous()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album", "Artist", new string?[] { "One", "Two", "Three" }, new string?[] { "A", "B", "C" });

        Assert.NotNull(packs);

        int packCount = packs!.Length / 18;

        for (int i = 0; i < packCount; i++)
        {
            Assert.Equal((byte)i, packs[i * 18 + 2]);
        }
    }

    [Fact]
    public void GeneratePacks_TitleOnly_OmitsPerformerPacks()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album", null, new string?[] { "Track" }, new string?[] { null });

        Assert.NotNull(packs);

        for (int i = 0; i < packs!.Length; i += 18)
        {
            Assert.NotEqual(0x81, packs[i]);
        }
    }

    // ── Size info packs ──────────────────────────────────────

    [Fact]
    public void GeneratePacks_SizeInfo_MatchesPackCounts()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album Title", "Album Artist", new string?[] { "Track One", "Track Two", "Track Three" }, new string?[] { "Artist", "Artist", "Artist" });

        Assert.NotNull(packs);

        int packCount = packs!.Length / 18;
        int titlePacks = 0;
        int performerPacks = 0;
        var sizeInfoPacks = new List<byte[]>();

        for (int i = 0; i < packCount; i++)
        {
            byte type = packs[i * 18];

            if (type == 0x80)
            {
                titlePacks++;
            }
            else if (type == 0x81)
            {
                performerPacks++;
            }
            else if (type == 0x8F)
            {
                sizeInfoPacks.Add(packs.AsSpan(i * 18, 18).ToArray());
            }
        }

        Assert.Equal(3, sizeInfoPacks.Count);

        // The three 0x8F packs carry group indices 0-2 in the track field.
        Assert.Equal(0, sizeInfoPacks[0][1]);
        Assert.Equal(1, sizeInfoPacks[1][1]);
        Assert.Equal(2, sizeInfoPacks[2][1]);

        // Reassemble the 36-byte size info content (12 bytes per pack).
        byte[] content = new byte[36];
        for (int i = 0; i < 3; i++)
        {
            Array.Copy(sizeInfoPacks[i], 4, content, i * 12, 12);
        }

        Assert.Equal(0x00, content[0]);              // ISO 8859-1
        Assert.Equal(1, content[1]);                 // first track
        Assert.Equal(3, content[2]);                 // last track
        Assert.Equal(titlePacks, content[4]);        // 0x80 count
        Assert.Equal(performerPacks, content[5]);    // 0x81 count
        Assert.Equal(3, content[4 + 15]);            // 0x8F count
        Assert.Equal(packCount - 1, content[20]);    // last sequence number, block 0
        Assert.Equal(0x09, content[28]);             // language: English
    }

    // ── Header semantics ─────────────────────────────────────

    [Fact]
    public void GeneratePacks_ContinuationPack_CarriesCharPosition()
    {
        // 13-char album title: pack 0 holds 12 chars, pack 1 starts with
        // the 13th at character position 12 — same shape as the real-disc
        // "Joyful Nights" vectors above.
        byte[]? packs = CdTextEncoder.GeneratePacks("Joyful Nights", null, new string?[] { "Song of Joy" }, new string?[] { null });

        Assert.NotNull(packs);
        Assert.Equal(0x80, packs![0]);
        Assert.Equal(0x00, packs[1]);  // track 0 (disc level)
        Assert.Equal(0x00, packs[3]);  // char position 0

        Assert.Equal(0x80, packs[18]);
        Assert.Equal(0x00, packs[18 + 1]); // first byte still belongs to track 0
        Assert.Equal(0x0C, packs[18 + 3]); // continuation at character 12
        Assert.Equal((byte)'s', packs[18 + 4]);
    }

    [Fact]
    public void GeneratePacks_CharPosition_CapsAt15()
    {
        string longTitle = new('A', 40);
        byte[]? packs = CdTextEncoder.GeneratePacks(longTitle, null, new string?[] { "T" }, new string?[] { null });

        Assert.NotNull(packs);

        // Pack 2 starts at character 24 of the title, but the 4-bit char
        // position field caps at 15.
        Assert.Equal(0x0F, packs![2 * 18 + 3]);
    }

    [Fact]
    public void GeneratePacks_TrackFieldIdentifiesFirstCharacterOwner()
    {
        // The 10-char disc title plus terminator fills 11 of pack 0's 12
        // payload bytes, so track 1's title starts mid-pack; pack 1's
        // first byte then belongs to track 1 and its header must say so.
        byte[]? packs = CdTextEncoder.GeneratePacks("0123456789", null, new string?[] { "Track One Title" }, new string?[] { null });

        Assert.NotNull(packs);
        Assert.Equal(0x00, packs![1]); // pack 0 starts with the disc title
        Assert.Equal(0x01, packs[18 + 1]); // pack 1's first byte is inside track 1's title
    }

    // ── Transliteration ──────────────────────────────────────

    [Theory]
    [InlineData("Frankie’s First Affair", "Frankie's First Affair")]
    [InlineData("Burn Test 1 — Smoke", "Burn Test 1 - Smoke")]
    [InlineData("‘quoted’ “speech”", "'quoted' \"speech\"")]
    [InlineData("low ‚quote„ high ‛‟", "low 'quote\" high '\"")]
    [InlineData("1–2 A—B bar―line 3−4", "1-2 A-B bar-line 3-4")]
    [InlineData("To be continued…", "To be continued...")]
    [InlineData("• Item", "* Item")]
    public void TransliterateToLatin1_TypographicPunctuation_BecomesAscii(string input, string expected)
    {
        Assert.Equal(expected, CdTextEncoder.TransliterateToLatin1(input));
    }

    [Fact]
    public void TransliterateToLatin1_NoBreakSpaces_BecomePlainSpace()
    {
        // U+00A0 no-break, U+2007 figure, U+202F narrow no-break space,
        // built from char casts so the invisible characters are explicit.
        string input = "A" + (char)0x00A0 + "B" + (char)0x2007 + "C" + (char)0x202F + "D";

        Assert.Equal("A B C D", CdTextEncoder.TransliterateToLatin1(input));
    }
    [Theory]
    [InlineData("Café Über Señor Ångström")]
    [InlineData("«Guillemets» stay")]
    [InlineData("Plain ASCII 123!")]
    public void TransliterateToLatin1_RepresentableText_PassesThroughUnchanged(string input)
    {
        Assert.Same(input, CdTextEncoder.TransliterateToLatin1(input));
    }

    [Fact]
    public void GeneratePacks_CurlyApostrophe_RoundTripsAsAscii()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album", null, new string?[] { "Frankie’s First Affair" }, new string?[] { null });

        Assert.NotNull(packs);

        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(packs!);

        Assert.Equal("Frankie's First Affair", parsed!.Tracks[0].Title);
    }

    [Fact]
    public void GeneratePacks_EmDashAndEllipsis_RoundTripAsAscii()
    {
        var warnings = new List<string>();
        byte[]? packs = CdTextEncoder.GeneratePacks("Burn Test 1 — Smoke", null, new string?[] { "Wait…" }, new string?[] { null }, warnings);

        Assert.NotNull(packs);

        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(packs!);

        Assert.Equal("Burn Test 1 - Smoke", parsed!.AlbumTitle);
        Assert.Equal("Wait...", parsed.Tracks[0].Title);

        // Fully transliterated — nothing became '?', so no warning.
        Assert.Empty(warnings);
    }

    [Fact]
    public void GeneratePacks_AccentedLatin_RoundTripsByteIdentical()
    {
        var warnings = new List<string>();
        byte[]? packs = CdTextEncoder.GeneratePacks("Café Åñü", null, new string?[] { "Señor Über" }, new string?[] { null }, warnings);

        Assert.NotNull(packs);

        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(packs!);

        Assert.Equal("Café Åñü", parsed!.AlbumTitle);
        Assert.Equal("Señor Über", parsed.Tracks[0].Title);
        Assert.Empty(warnings);
    }

    [Fact]
    public void GeneratePacks_EllipsisExpansion_BudgetedBeforePackLimit()
    {
        // 99 track titles of 10 ellipses each look tiny (11 bytes per
        // string) but expand to 31 bytes each — past the 256-pack block
        // limit. The overflow must be detected AFTER expansion, or a
        // title could overflow its packs on the disc.
        var titles = new string?[99];
        var performers = new string?[99];

        for (int i = 0; i < 99; i++)
        {
            titles[i] = new string('…', 10);
        }

        var warnings = new List<string>();
        byte[]? packs = CdTextEncoder.GeneratePacks(null, null, titles, performers, warnings);

        Assert.Null(packs);
        Assert.Single(warnings);
        Assert.Contains("block holds at most", warnings[0], StringComparison.Ordinal);
    }

    // ── Encoding ─────────────────────────────────────────────

    [Fact]
    public void GeneratePacks_NonLatin1Characters_ReplacedWithQuestionMarkAndWarns()
    {
        var warnings = new List<string>();
        byte[]? packs = CdTextEncoder.GeneratePacks("日本語", null, new string?[] { "Track" }, new string?[] { null }, warnings);

        Assert.NotNull(packs);

        // First three payload bytes are the replaced characters.
        Assert.Equal((byte)'?', packs![4]);
        Assert.Equal((byte)'?', packs[5]);
        Assert.Equal((byte)'?', packs[6]);

        Assert.Single(warnings);
        Assert.Contains("'?'", warnings[0], StringComparison.Ordinal);
    }

    [Fact]
    public void GeneratePacks_Latin1Characters_EncodeDirectly()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Café", null, new string?[] { "Track" }, new string?[] { null });

        Assert.NotNull(packs);
        Assert.Equal(0xE9, packs![4 + 3]); // 'é' in ISO 8859-1
    }

    // ── Overflow ─────────────────────────────────────────────

    [Fact]
    public void GeneratePacks_TooMuchText_ReturnsNullWithWarning()
    {
        // 99 tracks with long titles and performers blows the 256-pack
        // block limit.
        var titles = new string?[99];
        var performers = new string?[99];

        for (int i = 0; i < 99; i++)
        {
            titles[i] = new string('T', 30);
            performers[i] = new string('P', 30);
        }

        var warnings = new List<string>();
        byte[]? packs = CdTextEncoder.GeneratePacks("Album", "Artist", titles, performers, warnings);

        Assert.Null(packs);
        Assert.Single(warnings);
        Assert.Contains("CD-TEXT", warnings[0], StringComparison.Ordinal);
    }

    // ── 6-bit expansion ──────────────────────────────────────

    [Fact]
    public void ExpandTo6Bit_RoundTripsThroughCollapse()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Album", "Artist", new string?[] { "One" }, new string?[] { "A" });

        Assert.NotNull(packs);

        byte[] expanded = CdTextEncoder.ExpandTo6Bit(packs!);

        Assert.Equal(packs!.Length / 3 * 4, expanded.Length);
        Assert.All(expanded, b => Assert.True(b <= 0x3F, "each expanded byte carries only 6 bits"));
        Assert.Equal(packs, CdTextTestHelpers.CollapseFrom6Bit(expanded));
    }

    [Fact]
    public void ExpandTo6Bit_NonPackMultiple_Throws()
    {
        Assert.Throws<ArgumentException>(() => CdTextEncoder.ExpandTo6Bit(new byte[17]));
    }

    // ── Round-trip through FoxRedbook's oracle-verified parser ─

    [Fact]
    public void GeneratePacks_RoundTripsThroughFoxRedbookParser()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks(
            "Public Domain Blues",
            "Various Artists",
            new string?[] { "One Dime Blues", "Court House Blues", "Jump Steady Blues" },
            new string?[] { "Blind Lemon Jefferson", "Clara Smith", "Pine Top Smith" });

        Assert.NotNull(packs);

        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(packs!);

        Assert.NotNull(parsed);
        Assert.Empty(parsed!.Warnings);
        Assert.Equal("Public Domain Blues", parsed.AlbumTitle);
        Assert.Equal("Various Artists", parsed.AlbumPerformer);
        Assert.Equal(3, parsed.Tracks.Count);
        Assert.Equal("One Dime Blues", parsed.Tracks[0].Title);
        Assert.Equal("Blind Lemon Jefferson", parsed.Tracks[0].Performer);
        Assert.Equal("Court House Blues", parsed.Tracks[1].Title);
        Assert.Equal("Clara Smith", parsed.Tracks[1].Performer);
        Assert.Equal("Jump Steady Blues", parsed.Tracks[2].Title);
        Assert.Equal("Pine Top Smith", parsed.Tracks[2].Performer);
    }

    [Fact]
    public void GeneratePacks_TitleOnly_RoundTrips()
    {
        byte[]? packs = CdTextEncoder.GeneratePacks("Only A Title", null, new string?[] { "Track A", "Track B" }, new string?[] { null, null });

        Assert.NotNull(packs);

        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(packs!);

        Assert.NotNull(parsed);
        Assert.Equal("Only A Title", parsed!.AlbumTitle);
        Assert.Null(parsed.AlbumPerformer);
        Assert.Equal("Track A", parsed.Tracks[0].Title);
        Assert.Equal("Track B", parsed.Tracks[1].Title);
    }
}
