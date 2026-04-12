using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

public sealed class RipSessionAccurateRipTests : IDisposable
{
    private const int TrackSectorCount = 10;
    private const int TotalTracks = 3;

    private readonly string _tempDir;
    private readonly string _testFilePath;
    private readonly TableOfContents _toc;

    public RipSessionAccurateRipTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxredbook_aracrc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _testFilePath = CreateTestFile(TotalTracks * TrackSectorCount);
        _toc = CreateToc(TotalTracks, TrackSectorCount);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    // ── Accessor contract ───────────────────────────────────────

    [Fact]
    public void GetAccurateRipV1Crc_BeforeAnyRip_Throws()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        Assert.Throws<InvalidOperationException>(
            () => session.GetAccurateRipV1Crc(_toc.Tracks[0]));
    }

    [Fact]
    public void GetAccurateRipV2Crc_BeforeAnyRip_Throws()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        Assert.Throws<InvalidOperationException>(
            () => session.GetAccurateRipV2Crc(_toc.Tracks[0]));
    }

    [Fact]
    public async Task GetAccurateRipV1Crc_AfterPartialRip_Throws()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0];
        int consumed = 0;

        // Consume only the first two sectors, then stop
        await foreach (var sector in session.RipTrackAsync(track))
        {
            consumed++;

            if (consumed >= 2)
            {
                break;
            }
        }

        Assert.Throws<InvalidOperationException>(
            () => session.GetAccurateRipV1Crc(track));
    }

    [Fact]
    public async Task GetAccurateRipV1Crc_AfterFullRip_ReturnsValue()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[1]; // middle track, no skip

        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        // Just verify it returns without throwing — value is validated in
        // the explicit-value test below.
        _ = session.GetAccurateRipV1Crc(track);
    }

    // ── Value correctness ───────────────────────────────────────

    [Fact]
    public async Task RipSession_MiddleTrack_CrcMatchesDirectComputation()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[1]; // track 2 of 3 — no skip applies

        // Rip the track via RipSession
        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint sessionV1 = session.GetAccurateRipV1Crc(track);
        uint sessionV2 = session.GetAccurateRipV2Crc(track);

        // Compute the expected value directly from the file data for this track
        uint[] expectedPcm = ReadTrackPcm(track);
        AccurateRipChecksum.Compute(expectedPcm, track.Number, TotalTracks, out uint expectedV1, out uint expectedV2);

        Assert.Equal(expectedV1, sessionV1);
        Assert.Equal(expectedV2, sessionV2);
    }

    [Fact]
    public async Task RipSession_FirstTrack_CrcAppliesStartSkip()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0]; // track 1 of 3

        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint sessionV1 = session.GetAccurateRipV1Crc(track);
        uint sessionV2 = session.GetAccurateRipV2Crc(track);

        uint[] expectedPcm = ReadTrackPcm(track);
        AccurateRipChecksum.Compute(expectedPcm, 1, TotalTracks, out uint expectedV1, out uint expectedV2);

        Assert.Equal(expectedV1, sessionV1);
        Assert.Equal(expectedV2, sessionV2);
    }

    [Fact]
    public async Task RipSession_LastTrack_CrcAppliesEndSkip()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[2]; // track 3 of 3

        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint sessionV1 = session.GetAccurateRipV1Crc(track);
        uint sessionV2 = session.GetAccurateRipV2Crc(track);

        uint[] expectedPcm = ReadTrackPcm(track);
        AccurateRipChecksum.Compute(expectedPcm, 3, TotalTracks, out uint expectedV1, out uint expectedV2);

        Assert.Equal(expectedV1, sessionV1);
        Assert.Equal(expectedV2, sessionV2);
    }

    [Fact]
    public async Task RipSession_AllThreeTracks_RetainsPerTrackState()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        foreach (var track in _toc.Tracks)
        {
            await foreach (var _ in session.RipTrackAsync(track))
            {
            }
        }

        // All three tracks' checksums must be accessible after the rip completes.
        foreach (var track in _toc.Tracks)
        {
            uint v1 = session.GetAccurateRipV1Crc(track);
            uint v2 = session.GetAccurateRipV2Crc(track);

            uint[] expectedPcm = ReadTrackPcm(track);
            AccurateRipChecksum.Compute(expectedPcm, track.Number, TotalTracks, out uint expectedV1, out uint expectedV2);

            Assert.Equal(expectedV1, v1);
            Assert.Equal(expectedV2, v2);
        }
    }

    // ── Re-rip resets state ──────────────────────────────────────

    [Fact]
    public async Task RipSession_ReRip_ResetsState()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[1];

        // First rip
        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint firstV1 = session.GetAccurateRipV1Crc(track);

        // Re-rip the same track — state should reset and recompute fresh
        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint secondV1 = session.GetAccurateRipV1Crc(track);

        // Values must match because the underlying data is identical,
        // AND the second call must not throw (state was reset, not accumulated).
        Assert.Equal(firstV1, secondV1);
    }

    [Fact]
    public async Task RipSession_ReRip_AfterPartialRip_RecoversCleanly()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[1];

        // First attempt: abort after 2 sectors
        int consumed = 0;

        await foreach (var _ in session.RipTrackAsync(track))
        {
            consumed++;

            if (consumed >= 2)
            {
                break;
            }
        }

        // Second attempt: full rip — state must be reset, not carry over the partial
        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        uint v1 = session.GetAccurateRipV1Crc(track);

        uint[] expectedPcm = ReadTrackPcm(track);
        AccurateRipChecksum.Compute(expectedPcm, track.Number, TotalTracks, out uint expectedV1, out uint _);

        Assert.Equal(expectedV1, v1);
    }

    // ── Dispose ─────────────────────────────────────────────────

    [Fact]
    public async Task Dispose_ClearsState_AccessorThrowsObjectDisposed()
    {
        var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        var session = new RipSession(drive);

        var track = _toc.Tracks[1];

        await foreach (var _ in session.RipTrackAsync(track))
        {
        }

        session.Dispose();

        Assert.Throws<ObjectDisposedException>(() => session.GetAccurateRipV1Crc(track));
        Assert.Throws<ObjectDisposedException>(() => session.GetAccurateRipV2Crc(track));

        drive.Dispose();
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Test data pattern — same LCG-like pattern as the direct-compute tests,
    /// derived from absolute sample index.
    /// </summary>
    private static uint ExpectedDword(long absoluteSampleIndex)
    {
        return unchecked((uint)absoluteSampleIndex * 0x01234567u + 0xDEADBEEFu);
    }

    private string CreateTestFile(int sectorCount)
    {
        string path = Path.Combine(_tempDir, "test.bin");
        int totalDwords = sectorCount * (CdConstants.SectorSize / sizeof(uint));
        byte[] data = new byte[sectorCount * CdConstants.SectorSize];

        for (int i = 0; i < totalDwords; i++)
        {
            uint value = ExpectedDword(i);
            int offset = i * sizeof(uint);
            data[offset + 0] = (byte)(value & 0xFF);
            data[offset + 1] = (byte)((value >> 8) & 0xFF);
            data[offset + 2] = (byte)((value >> 16) & 0xFF);
            data[offset + 3] = (byte)((value >> 24) & 0xFF);
        }

        File.WriteAllBytes(path, data);
        return path;
    }

    private static uint[] ReadTrackPcm(TrackInfo track)
    {
        int dwordsPerSector = CdConstants.SectorSize / sizeof(uint);
        uint[] pcm = new uint[track.SectorCount * dwordsPerSector];
        long startDword = track.StartLba * dwordsPerSector;

        for (int i = 0; i < pcm.Length; i++)
        {
            pcm[i] = ExpectedDword(startDword + i);
        }

        return pcm;
    }

    private static TableOfContents CreateToc(int trackCount, int sectorsPerTrack)
    {
        var tracks = new List<TrackInfo>();

        for (int t = 0; t < trackCount; t++)
        {
            tracks.Add(new TrackInfo
            {
                Number = t + 1,
                StartLba = t * sectorsPerTrack,
                SectorCount = sectorsPerTrack,
                Type = TrackType.Audio,
                Control = TrackControl.None,
            });
        }

        return new TableOfContents
        {
            FirstTrackNumber = 1,
            LastTrackNumber = trackCount,
            LeadOutLba = trackCount * sectorsPerTrack,
            Tracks = new ReadOnlyCollection<TrackInfo>(tracks),
        };
    }
}
