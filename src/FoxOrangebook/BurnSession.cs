using FoxRedbook;

namespace FoxOrangebook;

/// <summary>
/// Orchestrates Disc-At-Once burning of audio CDs. Takes a list of
/// <see cref="AudioTrackSource"/> objects and writes them to a blank
/// CD-R/CD-RW via the <see cref="IScsiTransport"/> interface.
/// </summary>
/// <remarks>
/// <para>
/// The burn sequence follows the MMC-6 DAO workflow:
/// <list type="number">
///   <item>Check the drive supports CD Mastering (feature 0x002F).</item>
///   <item>Verify the disc is blank.</item>
///   <item>Run Optimum Power Calibration.</item>
///   <item>Set Write Parameters mode page for DAO audio.</item>
///   <item>Send the cue sheet describing the full disc layout.</item>
///   <item>Stream all audio sectors via WRITE (10).</item>
///   <item>Close the session to finalize the disc.</item>
/// </list>
/// </para>
/// </remarks>
public sealed class BurnSession
{
    private readonly IScsiTransport _transport;
    private readonly BurnOptions _options;

    public BurnSession(IScsiTransport transport, BurnOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
        _options = options ?? new BurnOptions();
    }

    /// <summary>
    /// Checks whether the drive supports DAO burning (CD Mastering feature 0x002F).
    /// </summary>
    public bool SupportsDaoBurn()
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[16];
        BurnCommands.BuildGetConfiguration(cdb, BurnCommands.FeatureCdMastering, response.Length);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseGetConfigurationHasFeature(response, BurnCommands.FeatureCdMastering);
    }

    /// <summary>
    /// Reads the disc status from the drive.
    /// </summary>
    public DiscInfo ReadDiscInfo()
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[BurnCommands.ReadDiscInfoResponseLength];
        BurnCommands.BuildReadDiscInformation(cdb);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseReadDiscInformation(response);
    }

    /// <summary>
    /// Erases a CD-RW disc.
    /// </summary>
    /// <param name="minimal">
    /// If true, performs a minimal blank (PMA/TOC only, ~1 minute).
    /// If false, performs a full blank (entire disc, several minutes).
    /// </param>
    public void Blank(bool minimal = true)
    {
        byte[] cdb = new byte[12];
        BurnCommands.BuildBlank(cdb, minimal, immediate: false);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

    /// <summary>
    /// Burns a complete audio CD in Disc-At-Once mode.
    /// </summary>
    /// <param name="tracks">
    /// Audio tracks to burn, in order. Each track's <see cref="AudioTrackSource.Pcm"/>
    /// stream must contain raw 16-bit stereo 44.1 kHz PCM (2,352 bytes per sector).
    /// </param>
    /// <param name="progress">Optional progress callback.</param>
    /// <param name="cancellationToken">Token to cancel the burn.</param>
    /// <exception cref="InvalidOperationException">
    /// The drive doesn't support DAO, or the disc isn't blank.
    /// </exception>
    public async Task BurnAsync(
        IReadOnlyList<AudioTrackSource> tracks,
        IProgress<BurnProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tracks);

        if (tracks.Count == 0)
        {
            throw new ArgumentException("At least one track is required.", nameof(tracks));
        }

        // Step 1: Verify drive capability.
        if (!SupportsDaoBurn())
        {
            throw new InvalidOperationException("Drive does not support Disc-At-Once (CD Mastering feature 0x002F).");
        }

        // Step 2: Verify disc is blank.
        var discInfo = ReadDiscInfo();

        if (discInfo.Status != DiscStatus.Blank)
        {
            throw new InvalidOperationException($"Disc is not blank (status: {discInfo.Status}). Insert a blank CD-R or blank a CD-RW first.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Step 3: Optimum Power Calibration.
        RunOpc();

        // Step 4: Set write parameters for DAO audio.
        SetWriteParameters();

        // Step 5: Send the cue sheet.
        var cueSheet = BuildCueSheet(tracks);
        SendCueSheet(cueSheet);

        cancellationToken.ThrowIfCancellationRequested();

        // Step 6: Write all sectors.
        long totalDiscSectors = tracks.Sum(t => (long)t.SectorCount);
        long totalWritten = 0;
        uint currentLba = 0;

        // First track has a mandatory 150-sector pregap (2 seconds) that
        // the drive handles via the cue sheet — we start writing at the
        // pregap offset for track 1.
        for (int trackIdx = 0; trackIdx < tracks.Count; trackIdx++)
        {
            var track = tracks[trackIdx];
            int trackSectors = track.SectorCount;
            int written = 0;

            track.Pcm.Position = 0;

            while (written < trackSectors)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int remaining = trackSectors - written;
                int batch = Math.Min(remaining, _options.SectorsPerWrite);
                int byteCount = batch * CdConstants.SectorSize;

                byte[] buffer = new byte[byteCount];
                int bytesRead = 0;

                while (bytesRead < byteCount)
                {
                    int n = await track.Pcm.ReadAsync(
                        buffer.AsMemory(bytesRead, byteCount - bytesRead),
                        cancellationToken).ConfigureAwait(false);

                    if (n == 0)
                    {
                        // Pad with silence if the stream is shorter than expected.
                        Array.Clear(buffer, bytesRead, byteCount - bytesRead);
                        break;
                    }

                    bytesRead += n;
                }

                byte[] cdb = new byte[10];
                BurnCommands.BuildWrite10(cdb, currentLba, (ushort)batch);
                _transport.Execute(cdb, buffer, ScsiDirection.Out);

                currentLba += (uint)batch;
                written += batch;
                totalWritten += batch;

                progress?.Report(new BurnProgress
                {
                    TrackNumber = trackIdx + 1,
                    TrackSectors = trackSectors,
                    SectorsWritten = written,
                    TotalDiscSectors = totalDiscSectors,
                    TotalSectorsWritten = totalWritten,
                });
            }
        }

        // Step 7: Close the session to finalize.
        CloseSession();
    }

    // ── Internal steps ───────────────────────────────────────

    private void RunOpc()
    {
        byte[] cdb = new byte[10];
        BurnCommands.BuildSendOpc(cdb);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

    private void SetWriteParameters()
    {
        byte[] pageData = new byte[60];
        int len = BurnCommands.BuildWriteParametersPage(pageData, _options.TestWrite, _options.BufferUnderrunProtection);

        byte[] cdb = new byte[10];
        BurnCommands.BuildModeSelect10(cdb, len);
        _transport.Execute(cdb, pageData.AsSpan(0, len), ScsiDirection.Out);
    }

    private void SendCueSheet(IReadOnlyList<CueSheetEntry> entries)
    {
        byte[] data = BurnCommands.SerializeCueSheet(entries);

        byte[] cdb = new byte[10];
        BurnCommands.BuildSendCueSheet(cdb, data.Length);
        _transport.Execute(cdb, data, ScsiDirection.Out);
    }

    private void CloseSession()
    {
        byte[] cdb = new byte[10];
        BurnCommands.BuildCloseSession(cdb, immediate: false);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

    // ── Cue sheet builder ────────────────────────────────────

    internal static IReadOnlyList<CueSheetEntry> BuildCueSheet(IReadOnlyList<AudioTrackSource> tracks)
    {
        var entries = new List<CueSheetEntry>();

        // Lead-in
        entries.Add(CueSheetEntry.LeadIn());

        long currentLba = 0;

        for (int i = 0; i < tracks.Count; i++)
        {
            byte trackNum = (byte)(i + 1);
            int pregap = i == 0 ? Math.Max(tracks[i].PregapSectors, 150) : tracks[i].PregapSectors;

            if (pregap > 0)
            {
                var (pMin, pSec, pFrame) = BurnCommands.LbaToMsf(currentLba);
                entries.Add(CueSheetEntry.TrackPregap(trackNum, pMin, pSec, pFrame));
                currentLba += pregap;
            }

            var (tMin, tSec, tFrame) = BurnCommands.LbaToMsf(currentLba);
            entries.Add(CueSheetEntry.TrackStart(trackNum, tMin, tSec, tFrame));

            currentLba += tracks[i].SectorCount;
        }

        // Lead-out
        var (loMin, loSec, loFrame) = BurnCommands.LbaToMsf(currentLba);
        entries.Add(CueSheetEntry.LeadOut(loMin, loSec, loFrame));

        return entries;
    }
}
