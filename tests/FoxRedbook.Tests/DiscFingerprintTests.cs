using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

/// <summary>
/// Test vectors for disc fingerprint computation.
///
/// Two external oracles are used:
///
/// 1. python-discid's own test fixtures (metabrainz/python-discid) —
///    provides MusicBrainz disc IDs and freedb IDs for 4 known discs.
///    Source format note: python-discid's "offsets" array uses libdiscid's
///    convention of raw_LBA + 150 (MSF form). The tests below subtract
///    150 at the conversion boundary so TrackInfo records store raw LBAs
///    consistently with the rest of the library.
///
/// 2. whipper's Bloc Party Silent Alarm test fixture
///    (whipper-team/whipper/whipper/test/test_image_toc.py) — the only
///    public source found that provides CDDB + AccurateRip ID1 + AccurateRip ID2
///    for the same disc. Whipper verifies CDDB against cd-discid and AR IDs
///    against the actual AccurateRip database.
/// </summary>
public sealed class DiscFingerprintTests
{
    private const int MsfOffset = 150; // CdConstants.MsfLbaOffset

    // ── python-discid fixtures (MB + freedb) ────────────────────

    [Fact]
    public void GuanoApes_MatchesPythonDiscidFixture()
    {
        // python-discid offsets (raw_LBA + 150): 150, 17510, 33275, 45910, 57805, 78310,
        //   94650, 109580, 132010, 149160, 165115, 177710, 203325, 215555, 235590
        // Subtract 150 to get raw LBAs.
        int[] rawLbas =
        [
            0, 17360, 33125, 45760, 57655, 78160, 94500, 109430,
            131860, 149010, 164965, 177560, 203175, 215405, 235440,
        ];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 258725 - MsfOffset);

        var info = DiscFingerprint.Compute(toc);

        Assert.Equal("TqvKjMu7dMliSfmVEBtrL7sBSno-", info.MusicBrainzDiscId);
        Assert.Equal(0xb60d770fu, info.FreedbDiscId);
    }

    [Fact]
    public void Lunar_FirstTrackNumberIsTwo_MatchesPythonDiscidFixture()
    {
        // "Lunar - There Is No 1" — first track is number 2, not 1.
        // Offsets (libdiscid form): 150, 11512, 34143, 50747, 63640, 98491,
        //   123534, 174410, 195438, 201127
        int[] rawLbas =
        [
            0, 11362, 33993, 50597, 63490, 98341,
            123384, 174260, 195288, 200977,
        ];
        var toc = BuildToc(first: 2, rawLbas, rawLeadOutLba: 225781 - MsfOffset);

        var info = DiscFingerprint.Compute(toc);

        Assert.Equal("6RDuz0d7.M5SVMLe1z4DP0yaEC8-", info.MusicBrainzDiscId);
        Assert.Equal(0x840bc20bu, info.FreedbDiscId);
    }

    [Fact]
    public void MinimalSingleTrack_MatchesPythonDiscidFixture()
    {
        int[] rawLbas = [0]; // offset 150 in libdiscid form
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 44942 - MsfOffset);

        var info = DiscFingerprint.Compute(toc);

        Assert.Equal("ANJa4DGYN_ktpzOwvVPtcjwP7mE-", info.MusicBrainzDiscId);
        Assert.Equal(0x02025501u, info.FreedbDiscId);
    }

    [Fact]
    public void Korn_PregapAudioTrack_MatchesPythonDiscidFixture()
    {
        // "Korn - See You on the Other Side, with pre-gap audio track"
        // First track offset is 5475 — not the standard 150 — because this
        // disc has hidden track one audio in the pregap area.
        // Offsets (libdiscid form): 5475, 19645, 34416, 51655, 68900, 90015,
        //   111090, 130510, 158652, 173635, 189015, 208122, 224413, 252866
        int[] rawLbas =
        [
            5325, 19495, 34266, 51505, 68750, 89865,
            110940, 130360, 158502, 173485, 188865, 207972, 224263, 252716,
        ];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 275749 - MsfOffset);

        var info = DiscFingerprint.Compute(toc);

        Assert.Equal("CnkXRItZOUxex7JwyWmHfdbFdqE-", info.MusicBrainzDiscId);
        Assert.Equal(0xbe0e130eu, info.FreedbDiscId);
    }

    // ── whipper Bloc Party fixture (CDDB + AR1 + AR2) ───────────

    [Fact]
    public void BlocParty_MatchesWhipperFullFingerprint()
    {
        // Whipper test case: Bloc Party - Silent Alarm
        // cd-discid output: ad0be00d 13 15370 35019 51532 69190 84292 96826
        //   112527 132448 148595 168072 185539 203331 222103 3244
        // AccurateRip URL: e/d/2/dBAR-013-001af2de-0105994e-ad0be00d.bin
        //
        // cd-discid reports offsets in raw_LBA + 150 form. Subtract 150 to
        // get raw LBAs used by our TrackInfo records. Leadout LBA was
        // derived by hand during research: sum(raw_LBAs) + leadout = ID1,
        // so leadout = 0x001af2de - 1522894 = 243216.
        int[] rawLbas =
        [
            15220, 34869, 51382, 69040, 84142, 96676, 112377,
            132298, 148445, 167922, 185389, 203181, 221953,
        ];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 243216);

        var info = DiscFingerprint.Compute(toc);

        Assert.Equal(0xad0be00du, info.FreedbDiscId);
        Assert.Equal(0x001af2deu, info.AccurateRipId1);
        Assert.Equal(0x0105994eu, info.AccurateRipId2);
    }

    // ── Mixed-mode handling ────────────────────────────────────

    [Fact]
    public void MixedMode_UsesAudioSessionLeadOut()
    {
        // Enhanced CD: 5 audio tracks + 1 data track.
        // Data track starts at LBA 283535 → audio session lead-out should
        // be 283535 - 11400 = 272135, not the raw TOC lead-out.
        var tracks = new List<TrackInfo>
        {
            new() { Number = 1, StartLba = 0,      SectorCount = 50000, Type = TrackType.Audio, Control = TrackControl.None },
            new() { Number = 2, StartLba = 50000,  SectorCount = 60000, Type = TrackType.Audio, Control = TrackControl.None },
            new() { Number = 3, StartLba = 110000, SectorCount = 55000, Type = TrackType.Audio, Control = TrackControl.None },
            new() { Number = 4, StartLba = 165000, SectorCount = 52000, Type = TrackType.Audio, Control = TrackControl.None },
            new() { Number = 5, StartLba = 217000, SectorCount = 55135, Type = TrackType.Audio, Control = TrackControl.None },
            new() { Number = 6, StartLba = 283535, SectorCount = 100000, Type = TrackType.Data,  Control = TrackControl.DataTrack },
        };

        var toc = new TableOfContents
        {
            FirstTrackNumber = 1,
            LastTrackNumber = 6,
            LeadOutLba = 383535,
            Tracks = new ReadOnlyCollection<TrackInfo>(tracks),
        };

        long audioLeadOut = MusicBrainzDiscId.ComputeAudioSessionLeadOut(toc);

        Assert.Equal(272135L, audioLeadOut);
    }

    [Fact]
    public void AudioOnlyDisc_UsesRawLeadOut()
    {
        int[] rawLbas = [0, 50000, 100000];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 150000);

        long audioLeadOut = MusicBrainzDiscId.ComputeAudioSessionLeadOut(toc);

        Assert.Equal(150000L, audioLeadOut);
    }

    // ── ID2 zero-LBA guard ─────────────────────────────────────

    [Fact]
    public void AccurateRipId2_Track1AtLbaZero_UsesGuardedWeight()
    {
        // Track 1 at LBA 0 would contribute 0 * 1 = 0 to ID2 without the
        // max(lba, 1) guard, silently dropping the track from the sum.
        // With the guard, it contributes max(0,1) * 1 = 1.
        int[] rawLbas = [0, 10000, 20000];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 30000);

        var (id1, id2) = AccurateRipDiscIds.Compute(toc);

        // Hand computation:
        // id1 = 0 + 10000 + 20000 + 30000 = 60000
        // id2 = max(0,1)*1 + 10000*2 + 20000*3 + 30000*4 = 1 + 20000 + 60000 + 120000 = 200001
        Assert.Equal(60000u, id1);
        Assert.Equal(200001u, id2);
    }

    // ── DiscInfo aggregate ─────────────────────────────────────

    [Fact]
    public void DiscInfo_ContainsAllFieldsFromCompute()
    {
        int[] rawLbas = [0, 50000, 100000];
        var toc = BuildToc(first: 1, rawLbas, rawLeadOutLba: 150000);

        var info = DiscFingerprint.Compute(toc);

        Assert.Same(toc, info.Toc);
        Assert.NotEmpty(info.MusicBrainzDiscId);
        Assert.Equal(28, info.MusicBrainzDiscId.Length);
        Assert.True(info.FreedbDiscId != 0);
        Assert.True(info.AccurateRipId1 != 0);
        Assert.True(info.AccurateRipId2 != 0);
    }

    [Fact]
    public void Compute_NullToc_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => DiscFingerprint.Compute(null!));
    }

    // ── Helpers ────────────────────────────────────────────────

    private static TableOfContents BuildToc(int first, int[] rawLbas, long rawLeadOutLba)
    {
        var tracks = new List<TrackInfo>(rawLbas.Length);

        for (int i = 0; i < rawLbas.Length; i++)
        {
            int trackNumber = first + i;
            long nextStart = (i + 1 < rawLbas.Length) ? rawLbas[i + 1] : rawLeadOutLba;
            int sectorCount = (int)(nextStart - rawLbas[i]);

            tracks.Add(new TrackInfo
            {
                Number = trackNumber,
                StartLba = rawLbas[i],
                SectorCount = sectorCount,
                Type = TrackType.Audio,
                Control = TrackControl.None,
            });
        }

        return new TableOfContents
        {
            FirstTrackNumber = first,
            LastTrackNumber = first + rawLbas.Length - 1,
            LeadOutLba = rawLeadOutLba,
            Tracks = new ReadOnlyCollection<TrackInfo>(tracks),
        };
    }
}
