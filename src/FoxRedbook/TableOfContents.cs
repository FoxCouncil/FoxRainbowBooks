using System.Collections.ObjectModel;

namespace FoxRedbook;

/// <summary>
/// Parsed Table of Contents from a CD, obtained via READ TOC (opcode 0x43) format 0.
/// </summary>
/// <remarks>
/// The TOC describes the track layout of the disc. Audio tracks contain CD-DA data
/// and can be ripped; data tracks should be skipped by the verification engine.
/// <see cref="LeadOutLba"/> marks the end of the last track's data area.
/// </remarks>
public sealed class TableOfContents
{
    /// <summary>
    /// First track number on the disc (almost always 1).
    /// </summary>
    public required int FirstTrackNumber { get; init; }

    /// <summary>
    /// Last track number on the disc (1–99).
    /// </summary>
    public required int LastTrackNumber { get; init; }

    /// <summary>
    /// LBA of the lead-out area. This is one past the last sector of the final track.
    /// </summary>
    public required long LeadOutLba { get; init; }

    /// <summary>
    /// Ordered list of tracks on the disc, sorted by track number.
    /// </summary>
    public required ReadOnlyCollection<TrackInfo> Tracks { get; init; }

    /// <summary>
    /// Total number of tracks on the disc.
    /// </summary>
    public int TrackCount => Tracks.Count;

    /// <summary>
    /// Total number of audio sectors on the disc (sum of all audio track sector counts).
    /// </summary>
    public long TotalAudioSectors
    {
        get
        {
            long total = 0;

            for (int i = 0; i < Tracks.Count; i++)
            {
                if (Tracks[i].Type == TrackType.Audio)
                {
                    total += Tracks[i].SectorCount;
                }
            }

            return total;
        }
    }
}
