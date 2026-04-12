namespace FoxRedbook;

/// <summary>
/// Broad classification of a CD track.
/// Derived from the control nibble in the TOC Q subchannel data.
/// </summary>
public enum TrackType
{
    /// <summary>
    /// Audio track (CD-DA PCM data).
    /// </summary>
    Audio = 0,

    /// <summary>
    /// Data track (Mode 1, Mode 2, or any non-audio format).
    /// </summary>
    Data = 1,
}
