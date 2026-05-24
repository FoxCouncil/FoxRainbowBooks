using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

public sealed class TableOfContentsTests
{
    [Fact]
    public void TotalAudioSectors_AudioOnly_SumsAllTracks()
    {
        var toc = BuildToc(
            (TrackType.Audio, 1000),
            (TrackType.Audio, 2000),
            (TrackType.Audio, 3000));

        Assert.Equal(6000L, toc.TotalAudioSectors);
    }

    [Fact]
    public void TotalAudioSectors_MixedMode_ExcludesDataTracks()
    {
        // Enhanced CD: 2 audio tracks + a trailing data track. The data
        // track's sectors must not be counted toward the audio total.
        var toc = BuildToc(
            (TrackType.Audio, 5000),
            (TrackType.Audio, 7000),
            (TrackType.Data, 100000));

        Assert.Equal(12000L, toc.TotalAudioSectors);
    }

    [Fact]
    public void TotalAudioSectors_DataOnly_IsZero()
    {
        var toc = BuildToc((TrackType.Data, 100000));

        Assert.Equal(0L, toc.TotalAudioSectors);
    }

    [Fact]
    public void TrackCount_ReflectsTrackListLength()
    {
        var toc = BuildToc(
            (TrackType.Audio, 1000),
            (TrackType.Audio, 2000));

        Assert.Equal(2, toc.TrackCount);
    }

    private static TableOfContents BuildToc(params (TrackType Type, int SectorCount)[] specs)
    {
        var tracks = new List<TrackInfo>(specs.Length);
        long lba = 0;

        for (int i = 0; i < specs.Length; i++)
        {
            tracks.Add(new TrackInfo
            {
                Number = i + 1,
                StartLba = lba,
                SectorCount = specs[i].SectorCount,
                Type = specs[i].Type,
                Control = specs[i].Type == TrackType.Data ? TrackControl.DataTrack : TrackControl.None,
            });

            lba += specs[i].SectorCount;
        }

        return new TableOfContents
        {
            FirstTrackNumber = 1,
            LastTrackNumber = specs.Length,
            LeadOutLba = lba,
            Tracks = new ReadOnlyCollection<TrackInfo>(tracks),
        };
    }
}
