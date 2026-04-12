namespace FoxRedbook.Tests;

/// <summary>
/// Configures the C2 error pointer data for sectors in the given LBA range.
/// When <see cref="ReadOptions.C2ErrorPointers"/> is requested, the corresponding
/// bits in the 294-byte C2 block are set to indicate unreliable bytes.
/// </summary>
/// <remarks>
/// C2 error pointers are a drive-reported quality signal — they always fire regardless
/// of attempt count. This matches real drive behavior: the disc surface defect doesn't
/// go away between reads, so the drive always reports the same C2 flags.
/// </remarks>
public readonly record struct C2ErrorFault
{
    /// <summary>
    /// First LBA affected by this fault (inclusive).
    /// </summary>
    public required long StartLba { get; init; }

    /// <summary>
    /// First LBA past the affected range (exclusive). Range is [StartLba, EndLba).
    /// </summary>
    public required long EndLba { get; init; }

    /// <summary>
    /// First byte position in the sector flagged as a C2 error (0–2351).
    /// </summary>
    public required int ByteOffset { get; init; }

    /// <summary>
    /// Number of consecutive bytes flagged as C2 errors starting at <see cref="ByteOffset"/>.
    /// </summary>
    public required int ByteCount { get; init; }
}
