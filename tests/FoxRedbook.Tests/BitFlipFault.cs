namespace FoxRedbook.Tests;

/// <summary>
/// Simulates bit-level corruption at a specific byte position within sectors in
/// the given LBA range. The XOR mask is applied to the byte at <see cref="ByteOffset"/>
/// on the first <see cref="CorruptReads"/> attempts; subsequent reads return clean data.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="ByteOffset"/> targets the data that was actually returned after jitter
/// resolution, not the originally requested LBA. This matches real drive behavior: a
/// scratch at a physical disc position corrupts whatever data the laser reads there.
/// </para>
/// <para>
/// Multiple <see cref="BitFlipFault"/> entries may overlap on the same LBA and byte offset.
/// All applicable faults are applied in list order (XOR is commutative, so order only
/// matters for readability).
/// </para>
/// </remarks>
public readonly record struct BitFlipFault
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
    /// Byte position within the 2,352-byte sector to corrupt (0–2351).
    /// </summary>
    public required int ByteOffset { get; init; }

    /// <summary>
    /// Bitmask XOR'd with the byte at <see cref="ByteOffset"/>. A mask of <c>0xFF</c>
    /// flips all bits; <c>0x01</c> flips only the LSB.
    /// </summary>
    public required byte XorMask { get; init; }

    /// <summary>
    /// Number of reads that return corrupted data. After this many reads of a given
    /// sector, subsequent reads return clean data. This lets the verification engine
    /// converge by re-reading.
    /// </summary>
    public required int CorruptReads { get; init; }
}
