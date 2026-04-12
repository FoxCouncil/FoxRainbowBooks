namespace FoxRedbook;

/// <summary>
/// Aggregated disc fingerprint — the TOC alongside the three disc ID
/// values that identify it in the major community databases, plus
/// optional CD-Text metadata. Produced by either
/// <see cref="DiscFingerprint.Compute"/> (pure, TOC-only) or
/// <see cref="DriveExtensions.ReadDiscInfoAsync"/> (reads TOC and
/// CD-Text from a live drive).
/// </summary>
/// <remarks>
/// Does not include audio sample data or verification checksums. For
/// AccurateRip v1/v2 track checksums (which require a full rip), see
/// <see cref="RipSession.GetAccurateRipV1Crc"/> and
/// <see cref="RipSession.GetAccurateRipV2Crc"/>.
/// </remarks>
public sealed record DiscInfo
{
    /// <summary>
    /// The Table of Contents this fingerprint was computed from.
    /// </summary>
    public required TableOfContents Toc { get; init; }

    /// <summary>
    /// MusicBrainz disc ID (28 characters, custom base64 alphabet).
    /// Used to look up a disc in the MusicBrainz database.
    /// </summary>
    public required string MusicBrainzDiscId { get; init; }

    /// <summary>
    /// freedb / CDDB disc ID (32-bit value). Displayed as 8 lowercase
    /// hex characters in the freedb and gnudb databases.
    /// </summary>
    public required uint FreedbDiscId { get; init; }

    /// <summary>
    /// First AccurateRip disc ID (32-bit). Sum of audio track start LBAs
    /// plus the lead-out offset. Used together with <see cref="AccurateRipId2"/>
    /// and <see cref="FreedbDiscId"/> to construct the AccurateRip database URL.
    /// </summary>
    public required uint AccurateRipId1 { get; init; }

    /// <summary>
    /// Second AccurateRip disc ID (32-bit). Position-weighted sum of audio
    /// track start LBAs plus the lead-out offset weighted by track count + 1.
    /// </summary>
    public required uint AccurateRipId2 { get; init; }

    /// <summary>
    /// CD-Text metadata read from the disc's lead-in area, or
    /// <see langword="null"/> if the disc has no CD-Text.
    /// </summary>
    /// <remarks>
    /// Populated when <see cref="DiscInfo"/> is built via
    /// <see cref="DriveExtensions.ReadDiscInfoAsync"/> (which reads from
    /// a live drive). Always <see langword="null"/> when built via
    /// <see cref="DiscFingerprint.Compute"/>, since that is a pure
    /// function from the TOC alone with no drive access.
    /// </remarks>
    public CdText? CdText { get; init; }
}
