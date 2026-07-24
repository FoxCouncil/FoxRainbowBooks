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
///   <item>Set the write speed if requested.</item>
///   <item>Run Optimum Power Calibration.</item>
///   <item>Set Write Parameters mode page for DAO audio.</item>
///   <item>Send the cue sheet describing the full disc layout.</item>
///   <item>Stream CD-TEXT packs into the lead-in when metadata is present.</item>
///   <item>Stream all program sectors via WRITE (10), starting at LBA -150
///   with track 1's pregap silence.</item>
///   <item>Flush the drive's cache and wait — in SAO the drive finalizes the session itself.</item>
/// </list>
/// </para>
/// <para>
/// Track 1's mandatory 150-sector pregap sits at absolute MSF 00:00:00
/// (LBA -150) with index 1 at 00:02:00 (LBA 0), matching the MMC DAO
/// annex: the host writes the pregap silence itself, so the sectors
/// streamed exactly cover the cue sheet's program area.
/// </para>
/// </remarks>
public sealed class BurnSession
{
    /// <summary>Mandatory pregap before track 1, in sectors (2 seconds).</summary>
    private const int MandatoryPregapSectors = 150;

    /// <summary>Red Book minimum track length in sectors (4 seconds).</summary>
    private const int MinTrackSectors = 300;

    /// <summary>
    /// 96-byte lead-in sectors streamed per WRITE (10) while writing
    /// CD-TEXT: 341 × 96 = 32,736 bytes per command, safely under the
    /// 64 KB transfer cap of USB transports.
    /// </summary>
    private const int CdTextSectorsPerWrite = 341;

    /// <summary>Invisible/incomplete track number for READ TRACK INFORMATION.</summary>
    private const uint InvisibleTrack = 0xFF;

    /// <summary>
    /// Retry cap for "long write in progress" NOT READY responses during
    /// lead-in writes (40 ms apart, per cdrdao's GenericMMC handling).
    /// </summary>
    private const int MaxLeadInNotReadyRetries = 1500;

    private readonly IScsiTransport _transport;
    private readonly BurnOptions _options;
    private readonly List<string> _warnings = new();
    private long _programSectorsWritten;

    public BurnSession(IScsiTransport transport, BurnOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
        _options = options ?? new BurnOptions();
    }

    /// <summary>
    /// Non-fatal issues from the most recent <see cref="BurnAsync"/> call —
    /// currently CD-TEXT being dropped because the drive rejected the
    /// CD-TEXT cue sheet or the metadata did not fit. The burn itself
    /// completed despite these.
    /// </summary>
    public IReadOnlyList<string> Warnings => _warnings;

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
    /// Erases a CD-RW disc. Blocks until the erase finishes, but issues
    /// the BLANK command with IMMED=1 and polls TEST UNIT READY: keeping
    /// a single SCSI command open for the whole erase would exceed the
    /// platform transports' per-command timeout and kill the blank
    /// mid-erase.
    /// </summary>
    /// <param name="minimal">
    /// If true, performs a minimal blank (PMA/TOC only, ~1 minute).
    /// If false, performs a full blank (entire disc, several minutes).
    /// </param>
    public void Blank(bool minimal = true)
    {
        byte[] cdb = new byte[12];
        BurnCommands.BuildBlank(cdb, minimal, immediate: true);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
        BurnCommands.WaitWhileNotReady(_transport);
    }

    /// <summary>
    /// Ejects the disc via START STOP UNIT (LoEj=1, Start=0). Call after
    /// a finished burn so the OS re-reads the new TOC on reinsertion.
    /// </summary>
    public void Eject()
    {
        byte[] cdb = new byte[6];
        BurnCommands.BuildStartStopUnit(cdb, loadEject: true, start: false);
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
    /// <exception cref="ArgumentException">
    /// More than 99 tracks, or a track shorter than 300 sectors (4 seconds).
    /// </exception>
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

        if (tracks.Count > CdConstants.MaxTrackNumber)
        {
            throw new ArgumentException($"Red Book allows at most {CdConstants.MaxTrackNumber} tracks; got {tracks.Count}.", nameof(tracks));
        }

        for (int i = 0; i < tracks.Count; i++)
        {
            int sectors = tracks[i].SectorCount;

            if (sectors < MinTrackSectors)
            {
                throw new ArgumentException($"Track {i + 1} is {sectors} sectors; Red Book requires at least {MinTrackSectors} (4 seconds).", nameof(tracks));
            }
        }

        _warnings.Clear();

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

        // Step 3: Set write speed if requested, then Optimum Power Calibration.
        if (_options.WriteSpeedKBps is int speed)
        {
            SetWriteSpeed(speed);
        }

        RunOpc();

        // Steps 4-8: write parameters, cue sheet, CD-TEXT lead-in, program
        // area, close. The CD-TEXT recipe is hardware-validated end-to-end
        // on the Pioneer BDR-XS07U: plain SAO write parameters (data block
        // type 0), a cue sheet whose lead-in entry carries data form 0x41,
        // the whole lead-in filled with 96-byte sub-channel sectors from
        // the ATIP lead-in start through LBA -151, then the program area
        // as plain 2,352-byte sectors from LBA -150. A 0x41 cue sheet is
        // only sent when a lead-in write address is actually available —
        // promising CD-TEXT and then not delivering the lead-in hangs the
        // drive on the first program WRITE. On failure before any program
        // data, fall back to a plain burn without CD-TEXT.
        byte[]? cdTextPacks = BuildCdTextPacks(tracks);

        if (cdTextPacks is not null)
        {
            int? leadInStart = ResolveCdTextLeadInStart();

            if (leadInStart is null)
            {
                _warnings.Add("No lead-in write address available (invisible-track NWA and ATIP both unusable); burning without CD-TEXT.");
            }
            else if (await TryBurnWithCdTextAsync(tracks, cdTextPacks, leadInStart.Value, progress, cancellationToken).ConfigureAwait(false))
            {
                return;
            }
        }

        SetWriteParameters();
        SendCueSheet(BuildCueSheet(tracks, cdTextInLeadIn: false));

        cancellationToken.ThrowIfCancellationRequested();

        await WriteProgramAreaAsync(tracks, progress, cancellationToken).ConfigureAwait(false);

        FinalizeSession();
    }

    /// <summary>
    /// Runs the hardware-validated CD-TEXT burn: write parameters, 0x41
    /// cue sheet, lead-in sub-channel sectors from
    /// <paramref name="leadInStartLba"/> through -151, program area,
    /// close. Returns false — with a warning recorded — when the drive
    /// rejects the cue sheet or the burn fails before any program sector,
    /// so the caller can fall back to a plain burn. Failures after program
    /// data has hit the disc are not retryable and propagate.
    /// </summary>
    private async Task<bool> TryBurnWithCdTextAsync(
        IReadOnlyList<AudioTrackSource> tracks,
        byte[] cdTextPacks,
        int leadInStartLba,
        IProgress<BurnProgress>? progress,
        CancellationToken cancellationToken)
    {
        SetWriteParameters();
        var cueSheet = BuildCueSheet(tracks, cdTextInLeadIn: true);

        try
        {
            SendCueSheet(cueSheet);
        }
        catch (OpticalDriveException ex) when (!IsTransient(ex))
        {
            _warnings.Add($"Drive rejected the CD-TEXT cue sheet ({ex.Message}); burning without CD-TEXT.");
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            WriteCdTextLeadIn(cdTextPacks, leadInStartLba, cancellationToken);
            await WriteProgramAreaAsync(tracks, progress, cancellationToken).ConfigureAwait(false);
        }
        catch (OpticalDriveException ex) when (!IsTransient(ex) && _programSectorsWritten == 0)
        {
            _warnings.Add($"CD-TEXT burn failed before any program data ({ex.Message}); retrying without CD-TEXT.");
            return false;
        }

        FinalizeSession();
        return true;
    }

    /// <summary>
    /// Finds the LBA where lead-in writes must start for a CD-TEXT burn.
    /// Preferred source is the invisible track's (0xFF) Next Writable
    /// Address — usable only when the drive points it into the lead-in.
    /// Many drives (including the Pioneer BDR-XS07U, whose invisible
    /// track reports -150, the pregap) don't, so the fallback is the
    /// ATIP's start-of-lead-in — the source the hardware-validated
    /// recipe uses. Returns null when neither yields an address below
    /// -150; the caller must then burn without CD-TEXT rather than send
    /// a 0x41 cue sheet it cannot fulfill.
    /// </summary>
    private int? ResolveCdTextLeadInStart()
    {
        try
        {
            int? nwa = ReadNextWritableAddress(InvisibleTrack);

            if (nwa is int reported && reported < -MandatoryPregapSectors)
            {
                return reported;
            }
        }
        catch (OpticalDriveException)
        {
            // Drive rejected the invisible-track query — try ATIP.
        }

        try
        {
            byte[] cdb = new byte[10];
            byte[] response = new byte[BurnCommands.ReadAtipResponseLength];
            BurnCommands.BuildReadAtip(cdb, response.Length);
            _transport.Execute(cdb, response, ScsiDirection.In);
            return BurnCommands.ParseAtipLeadInStart(response);
        }
        catch (OpticalDriveException)
        {
            return null;
        }
    }

    private static bool IsTransient(OpticalDriveException ex) => ex is DriveNotReadyException or MediaNotPresentException;

    // ── Program area streaming ───────────────────────────────

    private async Task WriteProgramAreaAsync(
        IReadOnlyList<AudioTrackSource> tracks,
        IProgress<BurnProgress>? progress,
        CancellationToken cancellationToken)
    {
        _programSectorsWritten = 0;

        // The drive may still be committing the lead-in (CD-TEXT writes,
        // session setup) — don't race it with the first program WRITE.
        BurnCommands.WaitWhileNotReady(_transport);

        // Every region the laser passes over is streamed by the host:
        // pregap silence (track 1's forced 150 plus any later pregaps)
        // followed by each track's PCM.
        var segments = new List<(int TrackNumber, int TrackTotalSectors, Stream? Pcm, int Sectors)>();
        long totalDiscSectors = 0;

        for (int i = 0; i < tracks.Count; i++)
        {
            int pregap = i == 0 ? Math.Max(tracks[i].PregapSectors, MandatoryPregapSectors) : tracks[i].PregapSectors;
            int trackTotal = pregap + tracks[i].SectorCount;

            if (pregap > 0)
            {
                segments.Add((i + 1, trackTotal, null, pregap));
            }

            segments.Add((i + 1, trackTotal, tracks[i].Pcm, tracks[i].SectorCount));
            totalDiscSectors += trackTotal;
        }

        long lba = -MandatoryPregapSectors;
        long totalWritten = 0;
        int currentTrack = 0;
        int trackWritten = 0;

        foreach (var (trackNumber, trackTotalSectors, pcm, sectors) in segments)
        {
            if (trackNumber != currentTrack)
            {
                currentTrack = trackNumber;
                trackWritten = 0;
            }

            int written = 0;

            if (pcm is not null)
            {
                pcm.Position = 0;
            }

            while (written < sectors)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int remaining = sectors - written;
                int batch = Math.Min(remaining, _options.SectorsPerWrite);
                int byteCount = batch * CdConstants.SectorSize;

                byte[] buffer = new byte[byteCount];

                if (pcm is not null)
                {
                    int bytesRead = 0;

                    while (bytesRead < byteCount)
                    {
                        int n = await pcm.ReadAsync(
                            buffer.AsMemory(bytesRead, byteCount - bytesRead),
                            cancellationToken).ConfigureAwait(false);

                        if (n == 0)
                        {
                            // A stream that ends early leaves the remainder
                            // as silence.
                            break;
                        }

                        bytesRead += n;
                    }
                }

                byte[] cdb = new byte[10];
                BurnCommands.BuildWrite10(cdb, unchecked((uint)lba), (ushort)batch);
                _transport.Execute(cdb, buffer, ScsiDirection.Out);

                lba += batch;
                written += batch;
                trackWritten += batch;
                totalWritten += batch;
                _programSectorsWritten = totalWritten;

                progress?.Report(new BurnProgress
                {
                    TrackNumber = trackNumber,
                    TrackSectors = trackTotalSectors,
                    SectorsWritten = trackWritten,
                    TotalDiscSectors = totalDiscSectors,
                    TotalSectorsWritten = totalWritten,
                });
            }
        }
    }

    // ── CD-TEXT lead-in streaming ────────────────────────────

    private byte[]? BuildCdTextPacks(IReadOnlyList<AudioTrackSource> tracks)
    {
        var titles = new string?[tracks.Count];
        var performers = new string?[tracks.Count];

        for (int i = 0; i < tracks.Count; i++)
        {
            titles[i] = tracks[i].Title;
            performers[i] = tracks[i].Performer;
        }

        return CdTextEncoder.GeneratePacks(_options.DiscTitle, _options.DiscPerformer, titles, performers, _warnings);
    }

    private void WriteCdTextLeadIn(byte[] packs, int leadInStartLba, CancellationToken cancellationToken)
    {
        // Hardware-validated on the Pioneer BDR-XS07U: the 0x41 lead-in
        // cue entry (sub-channel from host, main data generated by the
        // drive) obliges the host to fill the ENTIRE lead-in,
        // [leadInStartLba, -151], with 96-byte sub-channel sectors —
        // packs expanded to 6-bit form, 4 packs per sector, cycled. On
        // that drive the ATIP reports lead-in start 97:34:23 → LBA
        // -11,077, i.e. ~11,000 sectors. Skipping this write after a
        // 0x41 cue sheet hangs the drive on the first program WRITE.
        byte[] subdata = CdTextEncoder.ExpandTo6Bit(packs);
        int packCount = packs.Length / CdTextEncoder.PackSize;
        int sectorsRemaining = -MandatoryPregapSectors - leadInStartLba;
        long lba = leadInStartLba;
        int subCursor = 0;
        int notReadyRetries = 0;

        while (sectorsRemaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int batch = Math.Min(sectorsRemaining, CdTextSectorsPerWrite);
            byte[] buffer = new byte[batch * CdTextEncoder.LeadInSectorSize];
            int offset = 0;

            for (int s = 0; s < batch; s++)
            {
                for (int j = 0; j < CdTextEncoder.PacksPerLeadInSector; j++)
                {
                    Array.Copy(subdata, subCursor * CdTextEncoder.ExpandedPackSize, buffer, offset, CdTextEncoder.ExpandedPackSize);
                    subCursor = (subCursor + 1) % packCount;
                    offset += CdTextEncoder.ExpandedPackSize;
                }
            }

            byte[] cdb = new byte[10];
            BurnCommands.BuildWrite10(cdb, unchecked((uint)lba), (ushort)batch);

            // The drive may answer NOT READY ("long write in progress")
            // while its buffer drains — retry the same batch after 40 ms,
            // as cdrdao does.
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    _transport.Execute(cdb, buffer, ScsiDirection.Out);
                    break;
                }
                catch (DriveNotReadyException)
                {
                    if (++notReadyRetries > MaxLeadInNotReadyRetries)
                    {
                        throw;
                    }

                    Thread.Sleep(40);
                }
            }

            lba += batch;
            sectorsRemaining -= batch;
        }
    }

    private int? ReadNextWritableAddress(uint trackNumber)
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[BurnCommands.ReadTrackInfoResponseLength];
        BurnCommands.BuildReadTrackInformation(cdb, trackNumber);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseNextWritableAddress(response);
    }

    // ── Internal steps ───────────────────────────────────────

    private void SetWriteSpeed(int kBps)
    {
        ushort clamped = (ushort)Math.Clamp(kBps, BurnCommands.OneXAudioKBps, ushort.MaxValue);
        byte[] cdb = new byte[12];
        BurnCommands.BuildSetCdSpeed(cdb, BurnCommands.MaxSpeed, clamped);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

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

    private void FinalizeSession()
    {
        // In Session-At-Once the drive closes the session itself once the
        // host has written the full program area described by the cue
        // sheet — an explicit CLOSE TRACK/SESSION is not part of the SAO
        // flow (cdrecord/cdrdao send none), and the Pioneer BDR-XS07U
        // rejects one with 5/30/05 once the disc is already finalized.
        // Flushing the cache and waiting for ready IS the SAO finalize.
        // (TAO data burns differ: DataBurnSession keeps its CLOSE.)
        byte[] cdb = new byte[10];
        BurnCommands.BuildSynchronizeCache(cdb);

        try
        {
            _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
        }
        catch (DriveNotReadyException)
        {
            // Still flushing — the readiness wait below blocks until done.
        }

        BurnCommands.WaitWhileNotReady(_transport);

        // Belt and braces: confirm the drive actually finalized the disc.
        var discInfo = ReadDiscInfo();

        if (discInfo.Status != DiscStatus.Complete)
        {
            BurnCommands.WaitWhileNotReady(_transport);
            discInfo = ReadDiscInfo();

            if (discInfo.Status != DiscStatus.Complete)
            {
                throw new InvalidOperationException($"Drive did not finalize the disc after the DAO burn (status: {discInfo.Status}).");
            }
        }
    }

    // ── Cue sheet builder ────────────────────────────────────

    internal static IReadOnlyList<CueSheetEntry> BuildCueSheet(IReadOnlyList<AudioTrackSource> tracks, bool cdTextInLeadIn = false)
    {
        var entries = new List<CueSheetEntry>();

        // Lead-in
        entries.Add(cdTextInLeadIn
            ? CueSheetEntry.LeadIn(CueSheetEntry.DataFormCdTextLeadIn)
            : CueSheetEntry.LeadIn());

        // Track 1's pregap occupies LBA -150..-1 (absolute MSF 00:00:00),
        // putting index 1 at 00:02:00 per the MMC DAO annex. The host
        // streams those pregap sectors itself, starting at LBA -150.
        long currentLba = -MandatoryPregapSectors;

        for (int i = 0; i < tracks.Count; i++)
        {
            byte trackNum = (byte)(i + 1);
            int pregap = i == 0 ? Math.Max(tracks[i].PregapSectors, MandatoryPregapSectors) : tracks[i].PregapSectors;

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
