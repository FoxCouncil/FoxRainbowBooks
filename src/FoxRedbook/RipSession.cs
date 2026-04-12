using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FoxRedbook;

/// <summary>
/// Orchestrates verified CD-DA extraction. One session per disc.
/// Tracks can be ripped sequentially via <see cref="RipTrackAsync"/>;
/// verification state is preserved across track boundaries.
/// </summary>
public sealed class RipSession : IDisposable, IAsyncDisposable
{
    private readonly IOpticalDrive _drive;
    private readonly WiggleEngine _engine;
    private readonly Dictionary<int, AccurateRipState> _accurateRipStates = new();
    private TableOfContents? _cachedToc;
    private bool _disposed;

    /// <summary>
    /// Creates a new rip session for the given drive.
    /// </summary>
    /// <param name="drive">
    /// The drive to read from. If offset correction is needed, wrap it in
    /// <see cref="OffsetCorrectingDrive"/> before passing it here, or use
    /// <see cref="CreateAutoCorrected"/> which does the lookup automatically.
    /// </param>
    /// <param name="options">Verification options. Uses defaults if null.</param>
    public RipSession(IOpticalDrive drive, RipOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(drive);
        _drive = drive;
        _engine = new WiggleEngine(drive, options ?? new RipOptions());
    }

    /// <summary>
    /// Creates a rip session with automatic drive offset correction applied
    /// when the drive is recognized in the embedded AccurateRip drive offset
    /// database. For unknown drives, returns an uncorrected session — rips
    /// will still work but won't match AccurateRip checksums exactly.
    /// </summary>
    /// <param name="drive">The drive to read from.</param>
    /// <param name="options">Verification options. Uses defaults if null.</param>
    /// <returns>
    /// A rip session. If the drive is known, its reads are offset-corrected
    /// transparently. Dispose the session to release the drive.
    /// </returns>
    public static RipSession CreateAutoCorrected(
        IOpticalDrive drive,
        RipOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(drive);

        int? offset = KnownDriveOffsets.Lookup(drive.Inquiry);

        // The AccurateRip database stores offsets in EAC convention:
        // +N means the drive's output is shifted forward by N samples
        // (disc_position[n - N] appears at buffer position n).
        // OffsetCorrectingDrive uses the opposite convention: +N means
        // "drive reads ahead, shift reads backward." Negate to bridge.
        IOpticalDrive effectiveDrive = offset is int o
            ? new OffsetCorrectingDrive(drive, -o)
            : drive;

        return new RipSession(effectiveDrive, options);
    }

    /// <summary>
    /// Rips a single track as an async stream of verified audio sectors.
    /// Sectors are yielded in LBA order. The memory backing each
    /// <see cref="AudioSector.Pcm"/> is recycled on the next iteration —
    /// copy it if you need to retain it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calling <see cref="RipTrackAsync"/> more than once for the same track
    /// in a single session is legitimate (e.g., to re-rip after excessive
    /// re-reads on the first pass). Each call resets the AccurateRip state
    /// for that track — previously computed checksums for the same track
    /// number are discarded.
    /// </para>
    /// <para>
    /// The AccurateRip v1/v2 checksums accumulate as sectors are yielded.
    /// They become available via <see cref="GetAccurateRipV1Crc"/> and
    /// <see cref="GetAccurateRipV2Crc"/> only after the track has been
    /// fully consumed. If the caller breaks out of the async enumeration
    /// early (e.g., cancellation), the checksums remain unfinalized and
    /// the accessors will throw.
    /// </para>
    /// </remarks>
    public async IAsyncEnumerable<AudioSector> RipTrackAsync(
        TrackInfo track,
        IProgress<RipProgress>? progress = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Lazily load the TOC so we know the total track count. Required
        // for the AccurateRip skip rule, which depends on whether the
        // current track is the last one on the disc.
        _cachedToc ??= await _drive.ReadTocAsync(cancellationToken).ConfigureAwait(false);

        // Initialize (or reset) AccurateRip state for this track
        uint trackDwords = (uint)(track.SectorCount * (CdConstants.SectorSize / sizeof(uint)));
        var arState = AccurateRipChecksum.CreateState(
            track.Number,
            _cachedToc.LastTrackNumber,
            trackDwords);
        _accurateRipStates[track.Number] = arState;

        // Seek the engine to the start of the track
        _engine.Seek(track.StartLba * WiggleConstants.WordsPerSector);

        byte[] sectorBuffer = ArrayPool<byte>.Shared.Rent(CdConstants.SectorSize);

        try
        {
            for (int i = 0; i < track.SectorCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                long lba = track.StartLba + i;

                _engine.ReadVerifiedSector(
                    sectorBuffer.AsSpan(0, CdConstants.SectorSize),
                    lba,
                    out SectorStatus status,
                    out int reReadCount,
                    cancellationToken);

                // Fold this sector's samples into the AccurateRip accumulator.
                // The span cast is stack-scoped — does not cross the yield return.
                AccurateRipChecksum.Update(
                    arState,
                    MemoryMarshal.Cast<byte, uint>(sectorBuffer.AsSpan(0, CdConstants.SectorSize)));

                progress?.Report(new RipProgress
                {
                    Lba = lba,
                    TotalSectors = track.SectorCount,
                    RetryCount = reReadCount,
                    Status = status,
                });

                yield return new AudioSector
                {
                    Lba = lba,
                    Pcm = new ReadOnlyMemory<byte>(sectorBuffer, 0, CdConstants.SectorSize),
                    Status = status,
                    ReReadCount = reReadCount,
                };

                // Yield back to caller — they process the sector, then we
                // recycle the buffer on the next MoveNextAsync call.
                await Task.CompletedTask.ConfigureAwait(false);
            }

            // Mark the AccurateRip state finalized only after all sectors
            // have been successfully yielded. Early termination leaves the
            // state non-finalized, so the checksum accessors throw.
            arState.IsFinalized = true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(sectorBuffer);
        }
    }

    /// <summary>
    /// Returns the AccurateRip v1 checksum for a fully-ripped track.
    /// </summary>
    /// <param name="track">The track whose checksum is requested.</param>
    /// <returns>The 32-bit v1 checksum value.</returns>
    /// <exception cref="InvalidOperationException">
    /// The track has not been fully ripped in this session. Call
    /// <see cref="RipTrackAsync"/> and consume it to completion first.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Named "CRC" for ecosystem consistency with the public AccurateRip
    /// database, forum posts, and existing ripper output. The underlying
    /// algorithm is a position-weighted sum of 32-bit sample pairs, not
    /// a cyclic redundancy check.
    /// </para>
    /// <para>
    /// Per-track checksums persist for the lifetime of the session.
    /// Disposing the session clears all cached checksums.
    /// </para>
    /// </remarks>
    public uint GetAccurateRipV1Crc(TrackInfo track)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return GetFinalizedState(track).V1;
    }

    /// <summary>
    /// Returns the AccurateRip v2 checksum for a fully-ripped track.
    /// </summary>
    /// <param name="track">The track whose checksum is requested.</param>
    /// <returns>The 32-bit v2 checksum value.</returns>
    /// <exception cref="InvalidOperationException">
    /// The track has not been fully ripped in this session.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Named "CRC" for ecosystem consistency with the public AccurateRip
    /// database. The underlying algorithm is a position-weighted sum of
    /// 32-bit sample pairs with 64-bit product folding, not a cyclic
    /// redundancy check.
    /// </para>
    /// <para>
    /// Per-track checksums persist for the lifetime of the session.
    /// Disposing the session clears all cached checksums.
    /// </para>
    /// </remarks>
    public uint GetAccurateRipV2Crc(TrackInfo track)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return GetFinalizedState(track).V2;
    }

    private AccurateRipState GetFinalizedState(TrackInfo track)
    {
        if (!_accurateRipStates.TryGetValue(track.Number, out AccurateRipState? state))
        {
            throw new InvalidOperationException(
                $"Track {track.Number} has not been ripped in this session.");
        }

        if (!state.IsFinalized)
        {
            throw new InvalidOperationException(
                $"Track {track.Number} rip is not complete — the async enumeration was not consumed to the end.");
        }

        return state;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_disposed)
        {
            _engine.Dispose();
            _accurateRipStates.Clear();
            _disposed = true;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
