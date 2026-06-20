using FoxOrangebook.FileSystem;
using FoxRedbook;

namespace FoxOrangebook;

/// <summary>
/// Burns a data disc by streaming a <see cref="DiscImage"/> (2048-byte logical
/// sectors) to an <see cref="IScsiTransport"/>. The write strategy is chosen
/// from the loaded media's MMC profile:
/// <list type="bullet">
///   <item><b>CD-R / CD-RW</b> — Track-At-Once Mode 1: OPC, Mode 1 write
///   parameters, <c>WRITE(10)</c> stream, flush + close.</item>
///   <item><b>DVD+RW</b> — background <c>FORMAT UNIT</c> (if unformatted) then
///   random <c>WRITE(10)</c>, retrying while the format frontier catches up,
///   then flush.</item>
/// </list>
/// </summary>
/// <remarks>
/// Pair it with <see cref="FileBackedDataTransport"/> to "burn" to a <c>.iso</c>
/// file with no hardware — the produced file is byte-identical to
/// <see cref="DiscImage.WriteTo"/> and mounts as a real disc.
/// </remarks>
public sealed class DataBurnSession
{
    private const int SectorSize = DiscImage.LogicalSectorSize;
    private const int FormatWriteRetryAttempts = 600; // up to ~5 min at 500 ms

    private readonly IScsiTransport _transport;
    private readonly DataBurnOptions _options;

    public DataBurnSession(IScsiTransport transport, DataBurnOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
        _options = options ?? new DataBurnOptions();
    }

    /// <summary>Checks whether the drive supports CD Track-At-Once writing (feature 0x002D).</summary>
    public bool SupportsDataBurn()
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[16];
        BurnCommands.BuildGetConfiguration(cdb, BurnCommands.FeatureCdTrackAtOnce, response.Length);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseGetConfigurationHasFeature(response, BurnCommands.FeatureCdTrackAtOnce);
    }

    /// <summary>Reads the current media profile from GET CONFIGURATION (e.g. 0x000A CD-RW, 0x001A DVD+RW).</summary>
    public ushort GetCurrentProfile()
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[8];
        BurnCommands.BuildGetConfigurationHeader(cdb, response.Length);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseCurrentProfile(response);
    }

    /// <summary>Reads the disc status from the drive.</summary>
    public DiscInfo ReadDiscInfo()
    {
        byte[] cdb = new byte[10];
        byte[] response = new byte[BurnCommands.ReadDiscInfoResponseLength];
        BurnCommands.BuildReadDiscInformation(cdb);
        _transport.Execute(cdb, response, ScsiDirection.In);
        return BurnCommands.ParseReadDiscInformation(response);
    }

    /// <summary>Erases a CD-RW disc.</summary>
    public void Blank(bool minimal = true)
    {
        byte[] cdb = new byte[12];
        BurnCommands.BuildBlank(cdb, minimal, immediate: false);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

    /// <summary>
    /// Burns the given filesystem image to the loaded disc, choosing the write
    /// strategy from the media profile.
    /// </summary>
    /// <param name="image">The image to write. Its sectors are streamed in order from LBA 0.</param>
    /// <param name="progress">Optional progress callback.</param>
    /// <param name="cancellationToken">Token to cancel the burn.</param>
    /// <exception cref="NotSupportedException">The loaded media isn't a supported writable profile.</exception>
    /// <exception cref="InvalidOperationException">The drive or disc can't accept the burn.</exception>
    public async Task BurnAsync(
        DiscImage image,
        IProgress<BurnProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(image);

        ushort profile = GetCurrentProfile();

        switch (profile)
        {
            case BurnCommands.ProfileCdR:
            case BurnCommands.ProfileCdRw:
            {
                await BurnCompactDiscAsync(image, progress, cancellationToken).ConfigureAwait(false);
                break;
            }

            case BurnCommands.ProfileDvdPlusRw:
            {
                await BurnDvdPlusRwAsync(image, progress, cancellationToken).ConfigureAwait(false);
                break;
            }

            default:
            {
                throw new NotSupportedException($"Unsupported media profile 0x{profile:X4} for data burning. Supported profiles: CD-R/CD-RW and DVD+RW.");
            }
        }
    }

    // ── CD-R / CD-RW: Track-At-Once Mode 1 ───────────────────────

    private async Task BurnCompactDiscAsync(DiscImage image, IProgress<BurnProgress>? progress, CancellationToken cancellationToken)
    {
        if (!SupportsDataBurn())
        {
            throw new InvalidOperationException("Drive does not support Track-At-Once data writing (feature 0x002D).");
        }

        var discInfo = ReadDiscInfo();

        if (discInfo.Status != DiscStatus.Blank)
        {
            throw new InvalidOperationException($"Disc is not blank (status: {discInfo.Status}). Insert a blank disc or blank a rewritable one first.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        RunOpc();
        SetDataWriteParameters();

        await StreamImageAsync(image, progress, WriteCompactDiscBatchAsync, cancellationToken).ConfigureAwait(false);

        await FinalizeCompactDiscAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task WriteCompactDiscBatchAsync(uint lba, byte[] buffer, int byteCount, int sectors, CancellationToken cancellationToken)
    {
        byte[] cdb = new byte[10];
        BurnCommands.BuildWrite10(cdb, lba, (ushort)sectors);
        _transport.Execute(cdb, buffer.AsSpan(0, byteCount), ScsiDirection.Out);
        return Task.CompletedTask;
    }

    private void RunOpc()
    {
        byte[] cdb = new byte[10];
        BurnCommands.BuildSendOpc(cdb);
        _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
    }

    private void SetDataWriteParameters()
    {
        byte[] pageData = new byte[60];
        int len = BurnCommands.BuildDataWriteParametersPage(pageData, _options.TestWrite, _options.BufferUnderrunProtection);

        byte[] cdb = new byte[10];
        BurnCommands.BuildModeSelect10(cdb, len);
        _transport.Execute(cdb, pageData.AsSpan(0, len), ScsiDirection.Out);
    }

    private async Task FinalizeCompactDiscAsync(CancellationToken cancellationToken)
    {
        // Flush the drive's write buffer before closing — without this the drive
        // is still recording when CLOSE arrives and reports "operation in progress".
        SynchronizeCache();
        await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);

        byte[] cdb = new byte[10];
        BurnCommands.BuildCloseSession(cdb, immediate: true);

        try
        {
            _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
        }
        catch (DriveNotReadyException)
        {
            // CLOSE accepted as a background operation; wait for it to finish.
        }

        await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
    }

    // ── DVD+RW: background format then random write ───────────────

    private async Task BurnDvdPlusRwAsync(DiscImage image, IProgress<BurnProgress>? progress, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        EnsureDvdPlusRwFormatted();

        await StreamImageAsync(image, progress, WriteDvdPlusRwBatchAsync, cancellationToken).ConfigureAwait(false);

        SynchronizeCache();

        try
        {
            await WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (DriveNotReadyException)
        {
            // The disc is written and the cache is flushed; the background format
            // of the remaining (unwritten) area may still be running. The written
            // region is valid and readable, so this is not a failure.
        }
    }

    private void EnsureDvdPlusRwFormatted()
    {
        byte[] cdb = new byte[12];
        BurnCommands.BuildReadFormatCapacities(cdb, 252);
        byte[] response = new byte[252];
        _transport.Execute(cdb, response, ScsiDirection.In);

        var (formatted, blocks) = BurnCommands.ParseFormatCapacities(response);

        if (formatted)
        {
            return;
        }

        // Background format (IMMED): the drive returns immediately and formats
        // while we write. Writes near the start are rejected until the format
        // frontier reaches them — WriteDvdPlusRwBatchAsync retries for that.
        byte[] fmtCdb = new byte[6];
        BurnCommands.BuildFormatUnit(fmtCdb);

        byte[] parameters = new byte[12];
        BurnCommands.BuildDvdPlusRwFormatParameters(parameters, blocks, immediate: true);

        _transport.Execute(fmtCdb, parameters.AsSpan(0, 12), ScsiDirection.Out);
    }

    private async Task WriteDvdPlusRwBatchAsync(uint lba, byte[] buffer, int byteCount, int sectors, CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                byte[] cdb = new byte[10];
                BurnCommands.BuildWrite10(cdb, lba, (ushort)sectors);
                _transport.Execute(cdb, buffer.AsSpan(0, byteCount), ScsiDirection.Out);
                return;
            }
            catch (DriveNotReadyException)
            {
                if (attempt >= FormatWriteRetryAttempts)
                {
                    throw;
                }

                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ── Shared streaming and helpers ─────────────────────────────

    private delegate Task WriteBatchAsync(uint lba, byte[] buffer, int byteCount, int sectors, CancellationToken cancellationToken);

    private async Task StreamImageAsync(DiscImage image, IProgress<BurnProgress>? progress, WriteBatchAsync writeBatch, CancellationToken cancellationToken)
    {
        long totalSectors = image.SectorCount;
        long totalWritten = 0;
        uint currentLba = 0;

        int batchSectors = Math.Max(1, _options.SectorsPerWrite);
        byte[] buffer = new byte[batchSectors * SectorSize];

        var stream = image.OpenRead();

        await using (stream.ConfigureAwait(false))
        {
            while (totalWritten < totalSectors)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int batch = (int)Math.Min(batchSectors, totalSectors - totalWritten);
                int byteCount = batch * SectorSize;

                await FillAsync(stream, buffer, byteCount, cancellationToken).ConfigureAwait(false);
                await writeBatch(currentLba, buffer, byteCount, batch, cancellationToken).ConfigureAwait(false);

                currentLba += (uint)batch;
                totalWritten += batch;

                progress?.Report(new BurnProgress
                {
                    TrackNumber = 1,
                    TrackSectors = (int)totalSectors,
                    SectorsWritten = (int)totalWritten,
                    TotalDiscSectors = totalSectors,
                    TotalSectorsWritten = totalWritten,
                });
            }
        }
    }

    private void SynchronizeCache()
    {
        byte[] cdb = new byte[10];
        BurnCommands.BuildSynchronizeCache(cdb);

        try
        {
            _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
        }
        catch (DriveNotReadyException)
        {
            // Still flushing — the readiness wait that follows blocks until done.
        }
    }

    private async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        const int maxAttempts = 600; // up to ~5 minutes at 500 ms

        for (int attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] cdb = new byte[6];
            BurnCommands.BuildTestUnitReady(cdb);

            try
            {
                _transport.Execute(cdb, Span<byte>.Empty, ScsiDirection.None);
                return;
            }
            catch (DriveNotReadyException)
            {
                if (attempt >= maxAttempts)
                {
                    throw;
                }

                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async Task FillAsync(Stream stream, byte[] buffer, int count, CancellationToken cancellationToken)
    {
        int filled = 0;

        while (filled < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(filled, count - filled), cancellationToken).ConfigureAwait(false);

            if (n == 0)
            {
                // The image is shorter than expected; pad the rest with zeros.
                Array.Clear(buffer, filled, count - filled);
                break;
            }

            filled += n;
        }
    }
}
