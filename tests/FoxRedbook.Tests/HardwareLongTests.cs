using System.Buffers.Binary;
using System.Globalization;

namespace FoxRedbook.Tests;

/// <summary>
/// Long-running hardware tests that perform real ripping operations against
/// a physical drive. Tagged <c>Category=Hardware</c> so they run alongside
/// the fast smoke tiers. A single track rip takes 30 seconds to several
/// minutes depending on disc and drive speed.
/// </summary>
[Collection(nameof(SerialHardware))]
[Trait("Category", "Hardware")]
public sealed class HardwareLongTests
{
    [SkippableFact]
    public async Task Tier5_RipTrack_CompletesWithComputedChecksums()
    {
        using var drive = TryOpenOrSkip();

        var toc = await HardwareTests.TryReadTocOrSkip(drive).ConfigureAwait(false);

        var track = toc.Tracks.First(t => t.Type == TrackType.Audio);

        int? driveOffset = KnownDriveOffsets.Lookup(drive.Inquiry);
        Console.WriteLine(driveOffset.HasValue
            ? $"Drive offset: {driveOffset.Value} samples (from embedded DB, {KnownDriveOffsets.EntryCount} entries dated {KnownDriveOffsets.DatabaseDate})"
            : $"Drive not in offset database ({KnownDriveOffsets.EntryCount} entries dated {KnownDriveOffsets.DatabaseDate}) — ripping without correction");

        using var session = RipSession.CreateAutoCorrected(drive);

        int sectorsYielded = 0;
        long totalBytes = 0;
        int errorSectors = 0;

        // Collect PCM for WAV output. Memory is recycled per iteration so
        // we must copy each sector's data.
        using var pcmStream = new MemoryStream((int)((long)track.SectorCount * CdConstants.SectorSize));

        await foreach (var sector in session.RipTrackAsync(track).ConfigureAwait(false))
        {
            sectorsYielded++;
            totalBytes += sector.Pcm.Length;
            pcmStream.Write(sector.Pcm.Span);

            if (sector.HadErrors)
            {
                errorSectors++;
            }
        }

        Assert.Equal(track.SectorCount, sectorsYielded);
        Assert.Equal((long)track.SectorCount * CdConstants.SectorSize, totalBytes);

        uint v1 = session.GetAccurateRipV1Crc(track);
        uint v2 = session.GetAccurateRipV2Crc(track);

        // Write the ripped audio as a WAV file so Fox can listen to it.
        string wavPath = Path.Combine(
            Path.GetTempPath(),
            string.Create(CultureInfo.InvariantCulture, $"foxredbook_track{track.Number:D2}.wav"));
        WriteWav(wavPath, pcmStream.ToArray());

        // Compute disc fingerprint for the AccurateRip database lookup URL.
        var info = DiscFingerprint.Compute(toc);
        uint arId1 = info.AccurateRipId1;
        uint arId2 = info.AccurateRipId2;
        uint freedb = info.FreedbDiscId;
        int trackCount = toc.Tracks.Count(t => t.Type == TrackType.Audio);

        string arUrl = string.Create(CultureInfo.InvariantCulture,
            $"http://www.accuraterip.com/accuraterip/"
            + $"{arId1 & 0xF:x}/{(arId1 >> 4) & 0xF:x}/{(arId1 >> 8) & 0xF:x}/"
            + $"dBAR-{trackCount:D3}-{arId1:x8}-{arId2:x8}-{freedb:x8}.bin");

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"Ripped track {track.Number}: "
            + $"{sectorsYielded} sectors, {totalBytes:N0} bytes, "
            + $"{errorSectors} with corrections, "
            + $"AR v1=0x{v1:X8} v2=0x{v2:X8}"));
        Console.WriteLine($"WAV: {wavPath}");
        Console.WriteLine($"AccurateRip lookup: {arUrl}");
    }

    // ── Helpers ──────────────────────────────────────────────

    private static IOpticalDrive TryOpenOrSkip()
    {
        string? devicePath = HardwareTestEnvironment.GetDevicePath();

        Skip.If(
            devicePath is null,
            $"No optical drive available. Set {HardwareTestEnvironment.EnvironmentVariableName} or insert a drive at the platform default.");

        try
        {
            return OpticalDrive.Open(devicePath!);
        }
        catch (OpticalDriveException ex)
        {
            Skip.If(true, $"Could not open '{devicePath}': {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Writes raw CD-DA PCM (16-bit stereo 44100 Hz little-endian) as a
    /// standard WAV file. Minimal header — no LIST/INFO chunks, just
    /// RIFF + fmt + data.
    /// </summary>
    private static void WriteWav(string path, byte[] pcmData)
    {
        const int sampleRate = 44100;
        const short channels = 2;
        const short bitsPerSample = 16;
        int byteRate = sampleRate * channels * (bitsPerSample / 8);
        short blockAlign = (short)(channels * (bitsPerSample / 8));

        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);

        Span<byte> header = stackalloc byte[44];

        // RIFF header
        "RIFF"u8.CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(4), 36 + pcmData.Length);
        "WAVE"u8.CopyTo(header.Slice(8));

        // fmt chunk
        "fmt "u8.CopyTo(header.Slice(12));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(16), 16); // chunk size
        BinaryPrimitives.WriteInt16LittleEndian(header.Slice(20), 1);  // PCM format
        BinaryPrimitives.WriteInt16LittleEndian(header.Slice(22), channels);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(24), sampleRate);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(28), byteRate);
        BinaryPrimitives.WriteInt16LittleEndian(header.Slice(32), blockAlign);
        BinaryPrimitives.WriteInt16LittleEndian(header.Slice(34), bitsPerSample);

        // data chunk
        "data"u8.CopyTo(header.Slice(36));
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(40), pcmData.Length);

        fs.Write(header);
        fs.Write(pcmData);
    }
}
