namespace FoxRedbook;

/// <summary>
/// A single verified CD-DA audio sector as produced by the verification engine.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Pcm"/> contains exactly <see cref="CdConstants.SectorSize"/> bytes (2,352)
/// of 16-bit signed little-endian stereo PCM at 44,100 Hz.
/// </para>
/// <para>
/// <b>Lifetime:</b> When consumed from <c>RipSession.RipTrackAsync</c>, the memory
/// backing <see cref="Pcm"/> is only valid for the current iteration of the async
/// enumerable. The verification engine recycles the underlying buffer on the next
/// <c>MoveNextAsync</c> call. Copy the data if you need to retain it.
/// </para>
/// </remarks>
public readonly record struct AudioSector
{
    /// <summary>
    /// Logical Block Address of this sector on the disc.
    /// </summary>
    public required long Lba { get; init; }

    /// <summary>
    /// Raw PCM audio data for this sector (2,352 bytes).
    /// See remarks on <see cref="AudioSector"/> for lifetime constraints.
    /// </summary>
    public required ReadOnlyMemory<byte> Pcm { get; init; }

    /// <summary>
    /// Flags describing what corrections the verification engine applied to produce this sector.
    /// <see cref="SectorStatus.None"/> means no corrections were needed.
    /// </summary>
    public required SectorStatus Status { get; init; }

    /// <summary>
    /// Number of times the drive re-read this sector before the verification engine
    /// accepted the result. A value of 0 means a single read was sufficient.
    /// </summary>
    public required int ReReadCount { get; init; }

    /// <summary>
    /// <see langword="true"/> if the sector required any corrections or encountered
    /// errors during ripping. Convenience check for any non-<see cref="SectorStatus.None"/> flags.
    /// </summary>
    public bool HadErrors => Status != SectorStatus.None;
}
