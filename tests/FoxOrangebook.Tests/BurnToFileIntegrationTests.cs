using FoxOrangebook;
using FoxRedbook;

namespace FoxOrangebook.Tests;

/// <summary>
/// Integration test that reads the public domain WAV files from the
/// assets/ directory, burns them to a bin/cue file pair via
/// <see cref="FileBackedBurnTransport"/>, and verifies the output.
/// </summary>
public sealed class BurnToFileIntegrationTests : IDisposable
{
    private readonly string _outputDir;

    public BurnToFileIntegrationTests()
    {
        // Write to artifacts/burn/ in the repo root so the output persists
        // for manual inspection and playback testing.
        string repoRoot = FindRepoRoot();
        _outputDir = Path.Combine(repoRoot, "artifacts", "burn");
        Directory.CreateDirectory(_outputDir);
    }

    public void Dispose()
    {
        // Intentionally left empty — keep the output for playback.
    }

    [Fact]
    public async Task BurnAssets_ProducesPlayableBinCue()
    {
        string assetsDir = FindAssetsDir();
        string[] wavFiles = Directory.GetFiles(assetsDir, "*.wav");
        Array.Sort(wavFiles, StringComparer.OrdinalIgnoreCase);

        Assert.Equal(7, wavFiles.Length);

        string binPath = Path.Combine(_outputDir, "PublicDomainBlues.bin");

        var tracks = new List<AudioTrackSource>();
        var streams = new List<Stream>();

        try
        {
            var trackMeta = new List<(string? Title, string? Performer)>();

            foreach (string wavPath in wavFiles)
            {
                var pcmStream = StripWavHeader(wavPath);
                streams.Add(pcmStream);

                // Parse "Artist - Title.wav" from filename.
                string name = Path.GetFileNameWithoutExtension(wavPath);
                string? artist = null;
                string? title = name;
                int sep = name.IndexOf(" - ", StringComparison.Ordinal);

                if (sep >= 0)
                {
                    artist = name[..sep];
                    title = name[(sep + 3)..];
                }

                tracks.Add(new AudioTrackSource
                {
                    Pcm = pcmStream,
                    PregapSectors = tracks.Count == 0 ? 150 : 0,
                    Title = title,
                    Performer = artist,
                });

                trackMeta.Add((title, artist));
            }

            using var transport = new FileBackedBurnTransport(binPath)
            {
                DiscTitle = "Public Domain Blues",
                DiscPerformer = "Various Artists",
                TrackMetadata = trackMeta,
            };
            var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 32 });

            var reports = new List<BurnProgress>();

            await session.BurnAsync(tracks, new Progress<BurnProgress>(p => reports.Add(p)));

            // Verify bin file exists and has correct total size.
            Assert.True(File.Exists(binPath));

            long expectedBytes = tracks.Sum(t => (long)t.SectorCount * CdConstants.SectorSize);
            long actualBytes = new FileInfo(binPath).Length;
            Assert.Equal(expectedBytes, actualBytes);

            // Verify cue file exists and references all 7 tracks.
            string cuePath = Path.ChangeExtension(binPath, ".cue");
            Assert.True(File.Exists(cuePath));

            string cue = File.ReadAllText(cuePath);
            Assert.Contains("FILE \"PublicDomainBlues.bin\" BINARY", cue, StringComparison.Ordinal);

            for (int i = 1; i <= 7; i++)
            {
                Assert.Contains($"TRACK {i:D2} AUDIO", cue, StringComparison.Ordinal);
            }

            // Verify progress was reported.
            Assert.NotEmpty(reports);
            Assert.Equal(expectedBytes / CdConstants.SectorSize, reports[^1].TotalSectorsWritten);

            // Print summary for manual inspection.
            long totalSeconds = expectedBytes / CdConstants.SectorSize / 75;
            Console.WriteLine($"Burn complete: {wavFiles.Length} tracks, {actualBytes:N0} bytes, ~{totalSeconds / 60}:{totalSeconds % 60:D2}");
            Console.WriteLine($"BIN: {binPath}");
            Console.WriteLine($"CUE: {cuePath}");
        }
        finally
        {
            foreach (var stream in streams)
            {
                stream.Dispose();
            }
        }
    }

    /// <summary>
    /// Opens a WAV file and returns a stream positioned past the header,
    /// containing only raw PCM data. Truncates to a whole number of
    /// CD-DA sectors (2352 bytes).
    /// </summary>
    private static MemoryStream StripWavHeader(string wavPath)
    {
        byte[] file = File.ReadAllBytes(wavPath);

        // WAV header: find the "data" chunk and skip past its 8-byte header.
        int dataOffset = -1;

        for (int i = 0; i < file.Length - 8; i++)
        {
            if (file[i] == 'd' && file[i + 1] == 'a' && file[i + 2] == 't' && file[i + 3] == 'a')
            {
                dataOffset = i + 8; // skip "data" + 4-byte chunk size
                break;
            }
        }

        if (dataOffset < 0)
        {
            throw new InvalidOperationException($"No 'data' chunk found in {wavPath}");
        }

        int pcmLength = file.Length - dataOffset;

        // Truncate to whole sectors.
        pcmLength = (pcmLength / CdConstants.SectorSize) * CdConstants.SectorSize;

        return new MemoryStream(file, dataOffset, pcmLength);
    }

    private static string FindAssetsDir()
    {
        string dir = AppContext.BaseDirectory;

        while (dir is not null)
        {
            string candidate = Path.Combine(dir, "assets");

            if (Directory.Exists(candidate) && Directory.GetFiles(candidate, "*.wav").Length > 0)
            {
                return candidate;
            }

            dir = Path.GetDirectoryName(dir)!;
        }

        throw new InvalidOperationException("Could not find assets/ directory with WAV files.");
    }

    private static string FindRepoRoot()
    {
        string dir = AppContext.BaseDirectory;

        while (dir is not null)
        {
            if (Directory.Exists(Path.Combine(dir, ".git")))
            {
                return dir;
            }

            dir = Path.GetDirectoryName(dir)!;
        }

        return AppContext.BaseDirectory;
    }
}
