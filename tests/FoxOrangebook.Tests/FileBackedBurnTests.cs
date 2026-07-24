using FoxOrangebook;
using FoxRedbook;

namespace FoxOrangebook.Tests;

public sealed class FileBackedBurnTests : IDisposable
{
    private const int PregapSectors = 150;

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

        int sectorCount = 400;
        byte[] pcm = CreateTestPcm(sectorCount);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        Assert.True(File.Exists(binPath), ".bin file should exist");
        Assert.True(File.Exists(Path.ChangeExtension(binPath, ".cue")), ".cue file should exist");

        // The .bin models the full program area: 150 pregap sectors of
        // silence followed by the audio.
        byte[] binData = File.ReadAllBytes(binPath);
        Assert.Equal((PregapSectors + sectorCount) * CdConstants.SectorSize, binData.Length);
    }

    [Fact]
    public async Task BurnToFile_SingleTrack_PregapIsSilenceThenPcm()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        int sectorCount = 300;
        byte[] pcm = CreateTestPcm(sectorCount);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        byte[] binData = File.ReadAllBytes(binPath);
        int audioOffset = PregapSectors * CdConstants.SectorSize;

        // Pregap region (negative LBAs landing at file offset 0) is silence.
        Assert.Equal(-1, binData.AsSpan(0, audioOffset).IndexOfAnyExcept((byte)0));

        // Audio follows at LBA 0 = file offset 150 sectors.
        Assert.True(binData.AsSpan(audioOffset, pcm.Length).SequenceEqual(pcm));
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

        long expectedSize = (long)(PregapSectors + track1Sectors + track2Sectors) * CdConstants.SectorSize;
        long actualSize = new FileInfo(binPath).Length;
        Assert.Equal(expectedSize, actualSize);
    }

    [Fact]
    public async Task BurnToFile_CueIndexTimes_PregapAtZeroTrackStartAtTwoSeconds()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        string cue = File.ReadAllText(Path.ChangeExtension(binPath, ".cue"));

        // The pregap silence is in the file, so index 0 is at 00:00:00 and
        // the audio genuinely starts at 00:02:00.
        Assert.Contains("INDEX 00 00:00:00", cue, StringComparison.Ordinal);
        Assert.Contains("INDEX 01 00:02:00", cue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnToFile_ReportsProgress()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 25 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        var reports = new List<BurnProgress>();

        await session.BurnAsync(tracks, new Progress<BurnProgress>(p => reports.Add(p)));

        Assert.NotEmpty(reports);
        Assert.Equal(PregapSectors + 400, reports[^1].TotalSectorsWritten);
    }

    [Fact]
    public async Task BurnToFile_WithMetadata_CapturesCdTextLeadIn()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath)
        {
            DiscTitle = "Album",
            DiscPerformer = "Artist",
        };
        var session = new BurnSession(transport, new BurnOptions { DiscTitle = "Album", DiscPerformer = "Artist" });

        byte[] pcm = CreateTestPcm(400);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm), Title = "Song One", Performer = "Artist" },
        };

        await session.BurnAsync(tracks);

        // The simulated lead-in is completely filled with 96-byte sectors.
        Assert.Equal(transport.CdTextLeadInSectors * 96, transport.CdTextLeadInData.Length);

        // The .bin holds only the program area — lead-in data stays out,
        // and the 96 P-W sub-channel bytes of each 2,448-byte program
        // sector are stripped so the audio is byte-exact at its offset.
        Assert.Equal((PregapSectors + 400L) * CdConstants.SectorSize, new FileInfo(binPath).Length);

        byte[] binData = File.ReadAllBytes(binPath);
        Assert.True(binData.AsSpan(PregapSectors * CdConstants.SectorSize, pcm.Length).SequenceEqual(pcm));

        // Decoded packs round-trip to the original metadata.
        byte[] packs = CdTextTestHelpers.CollapseFrom6Bit(transport.CdTextLeadInData.Span);
        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(CdTextTestHelpers.TakeFirstCycle(packs));

        Assert.NotNull(parsed);
        Assert.Equal("Album", parsed!.AlbumTitle);
        Assert.Equal("Artist", parsed.AlbumPerformer);
        Assert.Equal("Song One", parsed.Tracks.Single(t => t.Number == 1).Title);
    }

    [Fact]
    public async Task BurnToFile_NoMetadata_NoCdTextLeadIn()
    {
        string binPath = Path.Combine(_tempDir, "test.bin");

        using var transport = new FileBackedBurnTransport(binPath);
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 300]) },
        };

        await session.BurnAsync(tracks);

        Assert.Equal(0, transport.CdTextLeadInData.Length);
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
