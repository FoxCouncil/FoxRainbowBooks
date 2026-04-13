namespace FoxOrangebook;

/// <summary>
/// Describes one audio track to burn. The PCM data is provided as a
/// <see cref="Stream"/> of raw 16-bit stereo 44.1 kHz little-endian
/// samples (the same format as CD-DA sectors, no WAV header).
/// </summary>
public sealed class AudioTrackSource
{
    /// <summary>
    /// Stream of raw PCM audio. Must be seekable so the burn session
    /// can determine the track length for the cue sheet. The stream
    /// is read sequentially during the burn and is NOT disposed by
    /// the session — the caller owns it.
    /// </summary>
    public required Stream Pcm { get; init; }

    /// <summary>
    /// Length of the pre-gap in sectors (75 sectors = 1 second).
    /// The first track must have at least 150 sectors (2 seconds)
    /// per Red Book. Subsequent tracks default to 0 (no gap).
    /// Set to 150 for a standard 2-second gap between tracks.
    /// </summary>
    public int PregapSectors { get; init; }

    /// <summary>
    /// Track title for CD-Text and cue sheet metadata. Optional.
    /// </summary>
    public string? Title { get; init; }

    /// <summary>
    /// Track performer/artist for CD-Text and cue sheet metadata. Optional.
    /// </summary>
    public string? Performer { get; init; }

    /// <summary>
    /// Number of audio sectors in this track. Computed from the
    /// stream length: <c>Pcm.Length / 2352</c>.
    /// </summary>
    public int SectorCount => (int)(Pcm.Length / FoxRedbook.CdConstants.SectorSize);
}
