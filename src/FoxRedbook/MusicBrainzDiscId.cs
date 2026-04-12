using System.Security.Cryptography;
using System.Text;

namespace FoxRedbook;

/// <summary>
/// Computes the MusicBrainz disc ID from a <see cref="TableOfContents"/>.
/// The algorithm is SHA-1 over a fixed-format ASCII hex string, then
/// base64-encoded with a custom alphabet.
/// </summary>
internal static class MusicBrainzDiscId
{
    /// <summary>
    /// Multi-session gap between the audio session lead-out and the data
    /// track's start LBA on mixed-mode (Enhanced CD) discs. Breakdown:
    /// 6,750 frames audio-session lead-out + 4,500 frames data-session
    /// lead-in + 150 frames data-track pre-gap = 11,400 frames total.
    /// </summary>
    private const int MultiSessionGap = 11400;

    /// <summary>
    /// Computes the MusicBrainz disc ID string for the given TOC.
    /// </summary>
    internal static string Compute(TableOfContents toc)
    {
        ArgumentNullException.ThrowIfNull(toc);

        // Filter to audio tracks only. Data tracks are excluded from both
        // the first/last track number fields and the offset array — the
        // MusicBrainz database is built from audio-only fingerprints.
        int audioCount = 0;

        foreach (var t in toc.Tracks)
        {
            if (t.Type == TrackType.Audio)
            {
                audioCount++;
            }
        }

        if (audioCount == 0)
        {
            throw new InvalidOperationException("Cannot compute MusicBrainz disc ID for a TOC with no audio tracks.");
        }

        // Determine the audio-session lead-out. For mixed-mode discs
        // (audio tracks followed by a data track), the TOC's raw lead-out
        // is the end of the DATA session, which is wrong for MusicBrainz.
        // The audio session's real lead-out is MultiSessionGap frames
        // before the data track's start LBA.
        long audioLeadOut = ComputeAudioSessionLeadOut(toc);

        // Find first and last audio track numbers. Track ordering follows
        // the TOC's track number field, not the array index — a track 1
        // might be preceded by a track 0 (hidden track one audio) in
        // pathological cases, though we don't currently represent that.
        int firstAudio = int.MaxValue;
        int lastAudio = int.MinValue;

        foreach (var t in toc.Tracks)
        {
            if (t.Type != TrackType.Audio)
            {
                continue;
            }

            if (t.Number < firstAudio)
            {
                firstAudio = t.Number;
            }

            if (t.Number > lastAudio)
            {
                lastAudio = t.Number;
            }
        }

        // Build the 804-character ASCII hex input string.
        // Format: "%02X" first + "%02X" last + 100 * "%08X" offsets
        //   offsets[0]   = lead-out LBA + 150
        //   offsets[n>0] = audio track n's LBA + 150, or 0 if missing
        var sb = new StringBuilder(804);
        sb.Append(firstAudio.ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
        sb.Append(lastAudio.ToString("X2", System.Globalization.CultureInfo.InvariantCulture));

        Span<long> offsets = stackalloc long[100];
        offsets[0] = audioLeadOut + CdConstants.MsfLbaOffset;

        foreach (var t in toc.Tracks)
        {
            if (t.Type != TrackType.Audio)
            {
                continue;
            }

            if (t.Number >= 1 && t.Number <= 99)
            {
                offsets[t.Number] = t.StartLba + CdConstants.MsfLbaOffset;
            }
        }

        for (int i = 0; i < 100; i++)
        {
            sb.Append(((uint)offsets[i]).ToString("X8", System.Globalization.CultureInfo.InvariantCulture));
        }

        // SHA-1 the ASCII encoding of that string.
        // SHA-1 is mandated by the MusicBrainz disc ID specification — it
        // is not used here for any cryptographic purpose (integrity,
        // authentication, password hashing, etc.). It is a fingerprinting
        // function fixed by the public database schema. Switching to
        // SHA-256 or similar would produce IDs that 404 against every
        // MusicBrainz entry in the world.
        byte[] inputBytes = Encoding.ASCII.GetBytes(sb.ToString());
#pragma warning disable CA5350 // SHA-1 required for MusicBrainz database compatibility, not used cryptographically
        byte[] digest = SHA1.HashData(inputBytes);
#pragma warning restore CA5350

        // Standard base64, then apply the MusicBrainz custom alphabet
        string b64 = Convert.ToBase64String(digest);
        return b64.Replace('+', '.').Replace('/', '_').Replace('=', '-');
    }

    /// <summary>
    /// Computes the audio-session lead-out LBA, adjusting for mixed-mode
    /// discs where the raw TOC lead-out reflects the end of a data session.
    /// </summary>
    internal static long ComputeAudioSessionLeadOut(TableOfContents toc)
    {
        long firstDataLba = long.MaxValue;
        bool hasDataTrack = false;

        foreach (var t in toc.Tracks)
        {
            if (t.Type == TrackType.Data && t.StartLba < firstDataLba)
            {
                firstDataLba = t.StartLba;
                hasDataTrack = true;
            }
        }

        if (hasDataTrack)
        {
            return firstDataLba - MultiSessionGap;
        }

        return toc.LeadOutLba;
    }
}
