using FoxOrangebook;
using FoxRedbook;

namespace FoxOrangebook.Tests;

public sealed class FileBackedBurnTests : IDisposable
{
    private readonly string _tempDir;

    public FileBackedBurnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxorangebook_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    [Fact]
    public async Task BurnToFile_SingleTrack_ProducesBinAndCue()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        int sectorCount = 100;
        byte[] pcm = CreateTestPcm(sectorCount);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        Assert.True(File.Exists(binPath), ".bin file should exist");
        Assert.True(File.Exists(Path.ChangeExtension(binPath, ".cue")), ".cue file should exist");

        byte[] binData = File.ReadAllBytes(binPath);
        Assert.Equal(sectorCount * CdConstants.SectorSize, binData.Length);
    }

    [Fact]
    public async Task BurnToFile_SingleTrack_BinContainsCorrectPcm()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        int sectorCount = 10;
        byte[] pcm = CreateTestPcm(sectorCount);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        byte[] binData = File.ReadAllBytes(binPath);

        for (int i = 0; i < pcm.Length; i++)
        {
            Assert.Equal(pcm[i], binData[i]);
        }
    }

    [Fact]
    public async Task BurnToFile_TwoTracks_CueHasBothTracks()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 500]) },
            new() { Pcm = new MemoryStream(new byte[2352 * 300]) },
        };

        await session.BurnAsync(tracks);

        string cue = File.ReadAllText(Path.ChangeExtension(binPath, ".cue"));

        Assert.Contains("TRACK 01 AUDIO", cue, StringComparison.Ordinal);
        Assert.Contains("TRACK 02 AUDIO", cue, StringComparison.Ordinal);
        Assert.Contains("FILE \"test.bin\" BINARY", cue, StringComparison.Ordinal);
        Assert.Contains("INDEX 01", cue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnToFile_TwoTracks_BinHasCorrectTotalSize()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        int track1Sectors = 500;
        int track2Sectors = 300;

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * track1Sectors]) },
            new() { Pcm = new MemoryStream(new byte[2352 * track2Sectors]) },
        };

        await session.BurnAsync(tracks);

        long expectedSize = (long)(track1Sectors + track2Sectors) * CdConstants.SectorSize;
        long actualSize = new FileInfo(binPath).Length;
        Assert.Equal(expectedSize, actualSize);
    }

    [Fact]
    public async Task BurnToFile_CueIndexTimesAreFileRelative()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
        };

        await session.BurnAsync(tracks);

        string cue = File.ReadAllText(Path.ChangeExtension(binPath, ".cue"));

        // Track 1 pregap should start at 00:00:00 (file-relative)
        Assert.Contains("INDEX 00 00:00:00", cue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnToFile_ReportsProgress()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 25 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
        };

        var reports = new List<BurnProgress>();

        await session.BurnAsync(tracks, new Progress<BurnProgress>(p => reports.Add(p)));

        Assert.NotEmpty(reports);
        Assert.Equal(100, reports[^1].TotalSectorsWritten);
    }

    private static byte[] CreateTestPcm(int sectorCount)
    {
        byte[] pcm = new byte[sectorCount * CdConstants.SectorSize];

        for (int i = 0; i < pcm.Length; i++)
        {
            pcm[i] = (byte)(i & 0xFF);
        }

        return pcm;
    }
}
