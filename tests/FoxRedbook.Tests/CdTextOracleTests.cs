using System.Buffers.Binary;
using FoxRedbook.Platforms.Common;

namespace FoxRedbook.Tests;

/// <summary>
/// End-to-end oracle test for <see cref="CdTextCommands.ParseCdText"/>.
/// Feeds libcdio's reference <c>cdtext.cdt</c> fixture through the parser
/// and verifies the decoded strings against the expected values from
/// <c>cdtext.right</c>. Both files are GPL-licensed test data from libcdio,
/// used unmodified — see <c>ATTRIBUTION.md</c> at the repo root.
/// </summary>
/// <remarks>
/// The .cdt file is a raw pack dump (96 packs × 18 bytes + 1 byte of trailer)
/// with no MMC READ TOC header. The parser expects a full drive response,
/// so we prepend a synthetic 4-byte header (length field = pack area + 2
/// reserved) before handing it off.
/// </remarks>
public sealed class CdTextOracleTests
{
    private const int PackSize = 18;
    private const int PackCount = 96;

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumTitle()
    {
        var cdText = ParseFixture();
        Assert.NotNull(cdText);
        Assert.Equal("Joyful Nights", cdText!.AlbumTitle);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumPerformer()
    {
        var cdText = ParseFixture();
        Assert.Equal("United Cat Orchestra", cdText!.AlbumPerformer);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumSongwriter()
    {
        var cdText = ParseFixture();
        Assert.Equal("Various Songwriters", cdText!.AlbumSongwriter);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumComposer()
    {
        var cdText = ParseFixture();
        Assert.Equal("Various Composers", cdText!.AlbumComposer);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumArranger()
    {
        var cdText = ParseFixture();
        Assert.Equal("Tom Cat", cdText!.AlbumArranger);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_AlbumMessage()
    {
        var cdText = ParseFixture();
        Assert.Equal("For all our fans", cdText!.AlbumMessage);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_UpcEan()
    {
        var cdText = ParseFixture();
        Assert.Equal("1234567890123", cdText!.UpcEan);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_HasThreeTracks()
    {
        var cdText = ParseFixture();
        Assert.Equal(3, cdText!.Tracks.Count);
        Assert.Equal(1, cdText.Tracks[0].Number);
        Assert.Equal(2, cdText.Tracks[1].Number);
        Assert.Equal(3, cdText.Tracks[2].Number);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_Track1Fields()
    {
        var cdText = ParseFixture();
        var track = cdText!.Tracks.Single(t => t.Number == 1);

        Assert.Equal("Song of Joy", track.Title);
        Assert.Equal("Felix and The Purrs", track.Performer);
        Assert.Equal("Friedrich Schiller", track.Songwriter);
        Assert.Equal("Ludwig van Beethoven", track.Composer);
        Assert.Equal("Fritz and Louie once were punks", track.Message);
        Assert.Equal("Tom Cat", track.Arranger);
        Assert.Equal("XYBLG1101234", track.Isrc);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_Track2Fields()
    {
        var cdText = ParseFixture();
        var track = cdText!.Tracks.Single(t => t.Number == 2);

        Assert.Equal("Humpty Dumpty", track.Title);
        Assert.Equal("Catwalk Beauties", track.Performer);
        Assert.Equal("Mother Goose", track.Songwriter);
        Assert.Equal("unknown", track.Composer);
        Assert.Equal("Pluck the goose", track.Message);
        Assert.Equal("Tom Cat", track.Arranger);
        Assert.Equal("XYBLG1100005", track.Isrc);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_Track3Fields()
    {
        var cdText = ParseFixture();
        var track = cdText!.Tracks.Single(t => t.Number == 3);

        Assert.Equal("Mee Owwww", track.Title);
        Assert.Equal("Mia Kitten", track.Performer);
        Assert.Equal("Mia Kitten", track.Songwriter);
        Assert.Equal("Mia Kitten", track.Composer);
        Assert.Equal("Mia Kitten", track.Arranger);
        Assert.Equal("XYBLG1100006", track.Isrc);
    }

    [Fact]
    public void ParseCdText_LibcdioFixture_ParsesWithoutWarnings()
    {
        var cdText = ParseFixture();
        Assert.Empty(cdText!.Warnings);
    }

    // ── Helpers ──────────────────────────────────────────────

    private static CdText? ParseFixture()
    {
        byte[] raw = LoadCdtFile();
        byte[] response = WrapInMmcHeader(raw);
        return CdTextCommands.ParseCdText(response);
    }

    private static byte[] LoadCdtFile()
    {
        string path = Path.Combine(
            Path.GetDirectoryName(typeof(CdTextOracleTests).Assembly.Location)!,
            "TestData",
            "cdtext.cdt");

        return File.ReadAllBytes(path);
    }

    /// <summary>
    /// Wraps a raw pack dump in a 4-byte MMC READ TOC header so it matches
    /// what the parser expects from a live drive response. The header's
    /// length field is (packArea + 2 reserved bytes) big-endian. We only
    /// pass through whole 18-byte packs — the .cdt file has a 1-byte trailer
    /// we ignore.
    /// </summary>
    private static byte[] WrapInMmcHeader(byte[] rawPacks)
    {
        int packAreaBytes = PackCount * PackSize;
        byte[] response = new byte[4 + packAreaBytes];

        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(0, 2), (ushort)(packAreaBytes + 2));
        // bytes 2..3 are reserved (already zero)
        Array.Copy(rawPacks, 0, response, 4, packAreaBytes);

        return response;
    }
}
