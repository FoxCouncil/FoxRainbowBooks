namespace FoxRedbook;

/// <summary>
/// Parsed CD-Text metadata from a disc's lead-in area. Populated from
/// the response to a <c>READ TOC</c> format 5 command. Only the first
/// language block (typically English, ISO-8859-1 encoded) is returned —
/// multi-language discs exist but the vast majority of CD-Text-capable
/// discs ship with only block 0.
/// </summary>
/// <remarks>
/// <para>
/// CD-Text is optional on commercial CDs. An estimated 10–20% of discs
/// pressed during the CD era contain CD-Text data; modern re-releases
/// and streaming-era discs usually don't. <see cref="IOpticalDrive.ReadCdTextAsync"/>
/// returns <see langword="null"/> when the disc has no CD-Text.
/// </para>
/// <para>
/// All string fields are null when the corresponding pack type was not
/// present on the disc. Unpopulated tracks are simply absent from the
/// <see cref="Tracks"/> collection — the parser does not synthesize
/// empty <see cref="CdTextTrack"/> records to match the disc's TOC.
/// </para>
/// </remarks>
public sealed record CdText
{
    /// <summary>Album title (pack type 0x80, track 0).</summary>
    public string? AlbumTitle { get; init; }

    /// <summary>Album performer / artist (pack type 0x81, track 0).</summary>
    public string? AlbumPerformer { get; init; }

    /// <summary>Album songwriter (pack type 0x82, track 0).</summary>
    public string? AlbumSongwriter { get; init; }

    /// <summary>Album composer (pack type 0x83, track 0).</summary>
    public string? AlbumComposer { get; init; }

    /// <summary>Album arranger (pack type 0x84, track 0).</summary>
    public string? AlbumArranger { get; init; }

    /// <summary>Disc-level free-form message (pack type 0x85, track 0).</summary>
    public string? AlbumMessage { get; init; }

    /// <summary>Disc ID string (pack type 0x86). Free-form, distinct from MusicBrainz / CDDB.</summary>
    public string? DiscId { get; init; }

    /// <summary>Genre name (pack type 0x87, text portion after the 2-byte genre code).</summary>
    public string? Genre { get; init; }

    /// <summary>UPC / EAN barcode (pack type 0x8E, track 0).</summary>
    public string? UpcEan { get; init; }

    /// <summary>
    /// Per-track CD-Text metadata, ordered by track number. Tracks with
    /// no CD-Text data are absent; the collection may be empty even when
    /// disc-level fields are populated.
    /// </summary>
    public IReadOnlyList<CdTextTrack> Tracks { get; init; } = [];

    /// <summary>
    /// Non-fatal parsing issues encountered during decoding (bad CRC,
    /// truncated pack, unknown encoding falling back to Latin-1, etc).
    /// An empty list indicates a clean parse.
    /// </summary>
    public IReadOnlyList<string> Warnings { get; init; } = [];
}
