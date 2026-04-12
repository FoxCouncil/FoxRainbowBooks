namespace FoxRedbook.Tests;

/// <summary>
/// Simulates transient read failures where the drive returns a SCSI error on the
/// first <see cref="FailureCount"/> attempts and succeeds on subsequent reads.
/// </summary>
/// <remarks>
/// Throws <see cref="OpticalDriveException"/> before any data is written to the buffer,
/// matching real drive behavior where a failed read produces no usable output.
/// This is evaluated before all other faults — if this throws, jitter/bitflip/C2
/// faults are not applied for that attempt.
/// </remarks>
public readonly record struct TransientFault
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
    /// Number of read attempts that throw before the drive "recovers." The first
    /// successful read is attempt number <c>FailureCount + 1</c>.
    /// </summary>
    public required int FailureCount { get; init; }
}
