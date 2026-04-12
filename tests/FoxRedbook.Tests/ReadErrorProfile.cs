namespace FoxRedbook.Tests;

/// <summary>
/// Configures error simulation for <see cref="FileBackedOpticalDrive"/>. Each fault
/// list targets a specific failure mode; faults within a list are applied in order.
/// </summary>
/// <remarks>
/// <para>
/// Fault application order per sector read:
/// <list type="number">
///   <item><see cref="TransientFailures"/> — throw before touching the buffer.</item>
///   <item><see cref="JitterFaults"/> — redirect which byte offset in the backing file is read.</item>
///   <item><see cref="BitFlips"/> — corrupt bytes in the returned data.</item>
///   <item><see cref="C2Errors"/> — set bits in the C2 error pointer block (if requested).</item>
/// </list>
/// </para>
/// <para>
/// All currently defined faults are fully deterministic. The <see cref="RandomSeed"/>
/// field is reserved for future probabilistic faults (e.g., "10% of reads in this
/// range fail transiently") and is not used by the current fault types.
/// </para>
/// </remarks>
public sealed record ReadErrorProfile
{
    /// <summary>
    /// Bit-level corruption faults. Applied after jitter resolution, targeting the
    /// data that was actually returned.
    /// </summary>
    public IReadOnlyList<BitFlipFault> BitFlips { get; init; } = [];

    /// <summary>
    /// C2 error pointer faults. Always reported regardless of attempt count.
    /// Only effective when <see cref="ReadOptions.C2ErrorPointers"/> is set.
    /// </summary>
    public IReadOnlyList<C2ErrorFault> C2Errors { get; init; } = [];

    /// <summary>
    /// Sub-sector positioning faults simulating drive jitter.
    /// </summary>
    public IReadOnlyList<JitterFault> JitterFaults { get; init; } = [];

    /// <summary>
    /// Transient read failure faults. Evaluated first — a throw prevents all other
    /// fault processing for that attempt.
    /// </summary>
    public IReadOnlyList<TransientFault> TransientFailures { get; init; } = [];

    /// <summary>
    /// Seed for future probabilistic fault types. Currently unused — all defined faults
    /// are fully deterministic. Exists to avoid a breaking change when probabilistic
    /// faults are added. Default is 0.
    /// </summary>
    public int RandomSeed { get; init; }
}
