namespace FoxRedbook;

/// <summary>
/// Flags from the control nibble of a TOC entry's Q subchannel data.
/// These map directly to bits 0–3 of the control field.
/// </summary>
[Flags]
public enum TrackControl
{
    /// <summary>
    /// No flags set. Standard two-channel audio without pre-emphasis, copy prohibited.
    /// </summary>
    None = 0,

    /// <summary>
    /// Audio recorded with 50/15 µs pre-emphasis. The ripper or playback chain
    /// must apply de-emphasis to produce flat frequency response.
    /// </summary>
    PreEmphasis = 1 << 0,

    /// <summary>
    /// Digital copy of this track is permitted (Serial Copy Management System flag).
    /// </summary>
    CopyPermitted = 1 << 1,

    /// <summary>
    /// Track contains data rather than audio. When set, <see cref="TrackType"/>
    /// should be <see cref="TrackType.Data"/>.
    /// </summary>
    DataTrack = 1 << 2,

    /// <summary>
    /// Four-channel audio (quadraphonic). Virtually never encountered on modern discs.
    /// Only meaningful when <see cref="DataTrack"/> is not set.
    /// </summary>
    FourChannel = 1 << 3,
}
