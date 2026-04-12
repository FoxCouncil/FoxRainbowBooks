namespace FoxRedbook.Tests;

/// <summary>
/// Manual hardware smoke tests that talk to a real optical drive via the
/// current platform's SCSI passthrough backend.
/// </summary>
/// <remarks>
/// <para>
/// Tests use <c>SkippableFact</c> and skip cleanly when no drive is
/// available on the host, so a drive-less CI runner that accidentally
/// enables the hardware category still reports the suite as green.
/// </para>
/// <para>
/// Tiers are layered from smallest to largest blast radius:
/// <list type="number">
///   <item>Tier 1: drive opens — validates device-path normalization and handle acquisition.</item>
///   <item>Tier 2: INQUIRY — validates the full SCSI passthrough stack with the smallest possible command.</item>
///   <item>Tier 3: READ TOC — validates TOC parsing from a real audio CD.</item>
///   <item>Tier 4: READ CD — validates single-sector audio read returns 2,352 bytes of plausible data.</item>
///   <item>Tier 5: full track rip via <see cref="RipSession"/> with WAV
///   output. Takes 30s–5min depending on track length and drive speed.</item>
/// </list>
/// </remarks>
[Collection(nameof(SerialHardware))]
[Trait("Category", "Hardware")]
public sealed class HardwareTests
{
    // ── Tier 1: drive opens ───────────────────────────────────

    [SkippableFact]
    public void Tier1_OpenDrive_Succeeds()
    {
        IOpticalDrive? drive = null;

        try
        {
            drive = TryOpenOrSkip();
            Assert.NotNull(drive);
        }
        finally
        {
            drive?.Dispose();
        }
    }

    // ── Tier 2: INQUIRY ───────────────────────────────────────

    [SkippableFact]
    public void Tier2_Inquiry_ReturnsParseableStrings()
    {
        using IOpticalDrive drive = TryOpenOrSkip();

        DriveInquiry inquiry = drive.Inquiry;

        // Shape-only assertions — we don't know what drive is plugged in.
        // Vendor: up to 8 ASCII chars per SPC-4, trimmed.
        Assert.NotNull(inquiry.Vendor);
        Assert.False(string.IsNullOrWhiteSpace(inquiry.Vendor), "Vendor must be non-empty.");
        Assert.True(inquiry.Vendor.Length <= 8, $"Vendor '{inquiry.Vendor}' exceeds 8 chars.");

        // Product: up to 16 ASCII chars, trimmed.
        Assert.NotNull(inquiry.Product);
        Assert.False(string.IsNullOrWhiteSpace(inquiry.Product), "Product must be non-empty.");
        Assert.True(inquiry.Product.Length <= 16, $"Product '{inquiry.Product}' exceeds 16 chars.");

        // Revision: up to 4 ASCII chars, trimmed.
        Assert.NotNull(inquiry.Revision);
        Assert.False(string.IsNullOrWhiteSpace(inquiry.Revision), "Revision must be non-empty.");
        Assert.True(inquiry.Revision.Length <= 4, $"Revision '{inquiry.Revision}' exceeds 4 chars.");

        // OffsetDatabaseKey is the canonical AccurateRip lookup string.
        Assert.NotNull(inquiry.OffsetDatabaseKey);
        Assert.Contains(" - ", inquiry.OffsetDatabaseKey, StringComparison.Ordinal);
    }

    // ── Tier 3: READ TOC ──────────────────────────────────────

    [SkippableFact]
    public async Task Tier3_ReadToc_ReturnsParseableAudioToc()
    {
        using var drive = TryOpenOrSkip();

        TableOfContents toc = await TryReadTocOrSkip(drive).ConfigureAwait(false);

        Assert.InRange(toc.FirstTrackNumber, 1, 99);
        Assert.InRange(toc.LastTrackNumber, toc.FirstTrackNumber, 99);
        Assert.True(toc.LeadOutLba > 0, "Lead-out LBA must be positive.");

        foreach (var track in toc.Tracks.Where(t => t.Type == TrackType.Audio))
        {
            Assert.InRange(track.Number, 1, 99);
            Assert.True(track.StartLba < toc.LeadOutLba,
                $"Track {track.Number} start LBA {track.StartLba} >= lead-out {toc.LeadOutLba}");
            Assert.True(track.SectorCount > 0,
                $"Track {track.Number} has zero sectors.");
        }

        for (int i = 1; i < toc.Tracks.Count; i++)
        {
            Assert.True(toc.Tracks[i].StartLba > toc.Tracks[i - 1].StartLba,
                $"Tracks out of LBA order at index {i}.");
        }
    }

    // ── Tier 4: READ CD ──────────────────────────────────────

    [SkippableFact]
    public async Task Tier4_ReadSectors_ReturnsAudioData()
    {
        using var drive = TryOpenOrSkip();

        var toc = await TryReadTocOrSkip(drive).ConfigureAwait(false);

        var firstAudioTrack = toc.Tracks.First(t => t.Type == TrackType.Audio);

        // Read well past the 2-second pregap (150 sectors) where data is
        // typically digital silence. 1000 sectors in (~13 seconds) should
        // have actual audio on any music CD.
        long readLba = firstAudioTrack.StartLba
            + Math.Min(1000, firstAudioTrack.SectorCount / 2);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // Fill with a sentinel so we can distinguish "buffer never written"
        // (still 0xCC) from "drive actively wrote zeros" (0x00).
        Array.Fill(buffer, (byte)0xCC);

        int sectorsRead = await drive.ReadSectorsAsync(
            readLba,
            count: 1,
            buffer,
            ReadOptions.None).ConfigureAwait(false);

        Assert.Equal(1, sectorsRead);

        bool allSentinel = buffer.All(b => b == 0xCC);
        bool allZero = buffer.All(b => b == 0);

        DriveInquiry inq = drive.Inquiry;
        string diag = $"Drive: {inq.Vendor} / {inq.Product} / {inq.Revision}, "
            + $"LBA: {readLba}, Track {firstAudioTrack.Number} "
            + $"(starts {firstAudioTrack.StartLba}, {firstAudioTrack.SectorCount} sectors), "
            + $"LeadOut: {toc.LeadOutLba}, "
            + $"Buffer: {(allSentinel ? "UNTOUCHED (0xCC)" : allZero ? "ALL ZEROS" : "HAS DATA")}, "
            + $"Distinct: {buffer.Distinct().Count()}, "
            + $"First 16: [{string.Join(" ", buffer.Take(16).Select(b => b.ToString("X2", System.Globalization.CultureInfo.InvariantCulture)))}]";

        Assert.False(allSentinel,
            $"Buffer was never written to — DMA/transfer did not occur. {diag}");
        Assert.False(allZero,
            $"Buffer is all zeros — drive wrote zeros instead of audio. {diag}");

        int distinct = buffer.Distinct().Count();
        Assert.True(distinct > 10,
            $"Only {distinct} distinct byte values — looks synthetic. {diag}");
    }

    // ── Helpers ──────────────────────────────────────────────

    /// <summary>
    /// Resolves the device path and opens the drive. Skips if no path is
    /// available or the open call fails.
    /// </summary>
    private static IOpticalDrive TryOpenOrSkip()
    {
        string? devicePath = HardwareTestEnvironment.GetDevicePath();

        Skip.If(
            devicePath is null,
            $"No optical drive available. Set {HardwareTestEnvironment.EnvironmentVariableName} or insert a drive at the platform default.");

        return TryOpenOrSkip(devicePath!);
    }

    /// <summary>
    /// Attempts to open the drive at the given path. If the open call
    /// throws an <see cref="OpticalDriveException"/>, treats that as
    /// "no drive at this path" and skips the test rather than failing
    /// it — matches the Windows path where we can't cheaply probe a
    /// drive letter before opening it.
    /// </summary>
    private static IOpticalDrive TryOpenOrSkip(string devicePath)
    {
        try
        {
            return OpticalDrive.Open(devicePath);
        }
        catch (OpticalDriveException ex)
        {
            Skip.If(true, $"Could not open '{devicePath}': {ex.Message}");
            throw; // unreachable — Skip.If(true, ...) throws SkipException
        }
    }

    /// <summary>
    /// Reads the TOC, skipping if no disc is present or the drive can't
    /// read it. Also skips if the TOC has no audio tracks.
    /// </summary>
    internal static async Task<TableOfContents> TryReadTocOrSkip(IOpticalDrive drive)
    {
        TableOfContents toc;

        try
        {
            toc = await drive.ReadTocAsync().ConfigureAwait(false);
        }
        catch (MediaNotPresentException)
        {
            Skip.If(true, "No disc in drive.");
            throw; // unreachable
        }
        catch (OpticalDriveException ex)
        {
            Skip.If(true, $"Could not read TOC: {ex.Message}");
            throw; // unreachable
        }

        Skip.IfNot(toc.TrackCount > 0, "No tracks in TOC — is an audio CD inserted?");

        bool hasAudio = toc.Tracks.Any(t => t.Type == TrackType.Audio);
        Skip.IfNot(hasAudio, "No audio tracks in TOC — insert an audio CD.");

        return toc;
    }
}
