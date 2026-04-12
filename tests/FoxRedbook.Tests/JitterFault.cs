namespace FoxRedbook.Tests;

/// <summary>
/// Simulates sub-sector positioning error (jitter). Instead of reading from the
/// nominal sector start, data is read from an offset position, as if the drive's
/// laser landed slightly before or after the correct position.
/// </summary>
/// <remarks>
/// <para>
/// Reads data starting at <c>(lba × 2352) + (SampleFrameShift × 4)</c> bytes into the
/// backing file. The returned buffer therefore straddles two physical sectors when
/// the shift is non-zero. A positive shift reads data from later in the stream
/// (the drive overshot); a negative shift reads from earlier (the drive undershot).
/// </para>
/// <para>
/// This is applied on the first <see cref="JitterReads"/> attempts; subsequent reads
/// return correctly positioned data. The verification engine's overlap-and-compare logic
/// detects and corrects this by cross-correlating overlapping reads.
/// </para>
/// <para>
/// Bit flips and C2 faults are evaluated against the data that was actually returned
/// (post-jitter), matching real drive behavior.
/// </para>
/// </remarks>
public readonly record struct JitterFault
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
    /// Offset in sample frames (each frame = 4 bytes: 2 channels × 16-bit) from the
    /// nominal sector start. Positive = read from later in the stream, negative = earlier.
    /// </summary>
    public required int SampleFrameShift { get; init; }

    /// <summary>
    /// Number of reads that return jittered data. After this many reads of a given
    /// sector, subsequent reads return correctly positioned data.
    /// </summary>
    public required int JitterReads { get; init; }
}
