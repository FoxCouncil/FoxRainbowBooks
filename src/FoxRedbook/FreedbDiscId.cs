namespace FoxRedbook;

/// <summary>
/// Computes the freedb / CDDB 32-bit disc ID from a <see cref="TableOfContents"/>.
/// </summary>
/// <remarks>
/// <para>
/// Despite the "CRC" terminology used in some freedb documentation, this
/// is not a cyclic redundancy check in any formal sense — it is a 32-bit
/// value assembled from three components: an 8-bit checksum (a modular
/// sum of decimal-digit-sums of track times), a 16-bit disc duration,
/// and an 8-bit last-track-number. The value has enough collision
/// resistance for a lookup-by-hash database of real CDs but should not
/// be used as a cryptographic or data-integrity checksum.
/// </para>
/// <para>
/// This implementation matches libdiscid's <c>create_freedb_disc_id</c>
/// function byte-for-byte, verified against four test vectors from
/// python-discid's test suite covering discs with first_track=1, a
/// first_track=2 disc, a minimal single-track disc, and a disc with
/// pregap audio. libdiscid's formula differs subtly from some other
/// freedb implementations in three places that only become visible on
/// non-standard discs: the low byte stores <c>last_track_num</c> (not
/// track count), the iteration runs over <c>track_offsets[1..last_track_num]</c>
/// regardless of which tracks actually exist (missing tracks contribute
/// zero to the digit sum), and T is computed from <c>track_offsets[1]</c>
/// even when no track 1 exists in the TOC.
/// </para>
/// </remarks>
internal static class FreedbDiscId
{
    internal static uint Compute(TableOfContents toc)
    {
        ArgumentNullException.ThrowIfNull(toc);

        if (toc.TrackCount == 0)
        {
            throw new InvalidOperationException("Cannot compute freedb disc ID for a TOC with no tracks.");
        }

        // Build libdiscid's track_offsets array layout:
        //   offsets[0]     = lead-out in MSF form (raw_LBA + 150)
        //   offsets[1..99] = track N's start in MSF form, 0 if missing
        Span<long> offsets = stackalloc long[100];
        offsets[0] = toc.LeadOutLba + CdConstants.MsfLbaOffset;

        int lastTrackNum = 0;

        foreach (var track in toc.Tracks)
        {
            if (track.Number is >= 1 and <= 99)
            {
                offsets[track.Number] = track.StartLba + CdConstants.MsfLbaOffset;

                if (track.Number > lastTrackNum)
                {
                    lastTrackNum = track.Number;
                }
            }
        }

        // N = sum of decimal-digit-sums of each track's start time in seconds.
        // libdiscid iterates over offsets[1..last_track_num] by index, NOT
        // over the actual track list. For a disc with first_track=2, the
        // loop visits offsets[1] (which is 0) and contributes 0 to N —
        // the missing track 1 is a zero contribution, not a skipped slot.
        int n = 0;

        for (int i = 0; i < lastTrackNum; i++)
        {
            int trackSeconds = (int)(offsets[i + 1] / CdConstants.SectorsPerSecond);
            n += DigitSum(trackSeconds);
        }

        // T = disc length in seconds = leadout_MSF_sec - track_1_MSF_sec.
        // When no track 1 exists, offsets[1] is 0 and T equals the full
        // leadout MSF seconds (from absolute disc start including the pregap).
        long leadOutSec = offsets[0] / CdConstants.SectorsPerSecond;
        long firstOffsetSec = offsets[1] / CdConstants.SectorsPerSecond;
        long t = leadOutSec - firstOffsetSec;

        // Low byte is last_track_num (NOT track count). This matters only
        // for discs where first_track != 1, in which case the two values
        // differ — for Lunar with first=2/last=11/10 tracks, the low byte
        // is 11 (0x0B), not 10 (0x0A).
        uint checksum = (uint)(n % 255);
        uint lastNum = (uint)lastTrackNum & 0xFF;
        uint time = (uint)t & 0xFFFF;

        return (checksum << 24) | (time << 8) | lastNum;
    }

    /// <summary>
    /// Sums the decimal digits of a non-negative integer.
    /// e.g. 123 → 1+2+3 = 6; 100 → 1; 9 → 9; 0 → 0.
    /// </summary>
    private static int DigitSum(int value)
    {
        int sum = 0;

        while (value > 0)
        {
            sum += value % 10;
            value /= 10;
        }

        return sum;
    }
}
