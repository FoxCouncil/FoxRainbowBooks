namespace FoxRedbook;

/// <summary>
/// Tracks jitter offset measurements for dynamic overlap adjustment.
/// Separate instances for Stage 1 (random jitter) and Stage 2 (systematic drift).
/// </summary>
/// <remarks>
/// This is a mutable reference type (sealed class, not struct) to avoid
/// the silent-mutation-on-copy footgun that mutable value types cause.
/// The one-time allocation is negligible; the safety win is not.
/// </remarks>
internal sealed class JitterStatistics
{
    /// <summary>Number of offset measurements recorded since last reset.</summary>
    internal int OffsetPoints { get; private set; }

    /// <summary>Sum of offsets (for computing average drift).</summary>
    internal long OffsetAccum { get; private set; }

    /// <summary>Sum of |offset| (for computing average jitter spread).</summary>
    internal long OffsetDiff { get; private set; }

    /// <summary>Most negative offset observed since last reset.</summary>
    internal int OffsetMin { get; private set; }

    /// <summary>Most positive offset observed since last reset.</summary>
    internal int OffsetMax { get; private set; }

    /// <summary>
    /// Records a new jitter measurement.
    /// </summary>
    /// <param name="offset">The measured offset in samples between two aligned blocks.</param>
    internal void AddMeasurement(int offset)
    {
        OffsetPoints++;
        OffsetAccum += offset;
        OffsetDiff += Math.Abs(offset);

        if (offset < OffsetMin)
        {
            OffsetMin = offset;
        }

        if (offset > OffsetMax)
        {
            OffsetMax = offset;
        }
    }

    /// <summary>
    /// Clears all accumulated statistics.
    /// </summary>
    internal void Reset()
    {
        OffsetPoints = 0;
        OffsetAccum = 0;
        OffsetDiff = 0;
        OffsetMin = 0;
        OffsetMax = 0;
    }
}
