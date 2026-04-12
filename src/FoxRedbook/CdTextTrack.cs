namespace FoxRedbook;

/// <summary>
/// Per-track CD-Text metadata. All string fields are null when the
/// corresponding pack type was not present for this track on the disc.
/// </summary>
public sealed record CdTextTrack
{
    /// <summary>1-based track number on the disc.</summary>
    public required int Number { get; init; }

    /// <summary>Track title (pack type 0x80).</summary>
    public string? Title { get; init; }

    /// <summary>Track performer / artist (pack type 0x81).</summary>
    public string? Performer { get; init; }

    /// <summary>Track songwriter (pack type 0x82).</summary>
    public string? Songwriter { get; init; }

    /// <summary>Track composer (pack type 0x83).</summary>
    public string? Composer { get; init; }

    /// <summary>Track arranger (pack type 0x84).</summary>
    public string? Arranger { get; init; }

    /// <summary>Track-specific free-form message (pack type 0x85).</summary>
    public string? Message { get; init; }

    /// <summary>Track ISRC (International Standard Recording Code, pack type 0x8E).</summary>
    public string? Isrc { get; init; }
}
