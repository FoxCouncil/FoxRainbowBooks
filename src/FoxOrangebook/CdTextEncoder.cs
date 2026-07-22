using System.Buffers.Binary;
using System.Text;

namespace FoxOrangebook;

/// <summary>
/// Generates CD-TEXT packs for the lead-in of a DAO burn. Produces the
/// 18-byte pack stream (title 0x80, performer 0x81, size info 0x8F with
/// CRC-16 per pack) and the 6-bit subchannel expansion the drive expects
/// when the cue sheet's lead-in entry uses data form 0x41.
/// </summary>
/// <remarks>
/// <para>
/// Pack layout follows the Red Book CD-TEXT annex as implemented by
/// cdrecord and libburn: strings for track 0 (disc level) through the
/// last track are concatenated with null terminators and chopped into
/// 12-byte payloads. Each pack header carries the pack type, the track
/// number of the payload's first character, a block-wide sequence
/// number, and the character position of that first character within
/// its string (capped at 15). The CRC-16 (poly 0x1021, init 0x0000,
/// final XOR 0xFFFF) covers bytes 0–15 and is stored big-endian at
/// bytes 16–17 — the same algorithm FoxRedbook's parser verifies
/// against libcdio's reference data.
/// </para>
/// <para>
/// Only single-byte ISO 8859-1 encoding is produced; characters outside
/// Latin-1 are replaced with '?'. MS-JIS (Shift-JIS) double-byte packs
/// are future work.
/// </para>
/// </remarks>
internal static class CdTextEncoder
{
    internal const int PackSize = 18;
    internal const int TextDataLength = 12;
    internal const int PacksPerLeadInSector = 4;

    /// <summary>Bytes of 6-bit-expanded pack data per lead-in sector (4 packs × 24 bytes).</summary>
    internal const int LeadInSectorSize = 96;

    /// <summary>One 18-byte pack expands to 24 six-bit subchannel bytes.</summary>
    internal const int ExpandedPackSize = 24;

    internal const byte PackTitle = 0x80;
    internal const byte PackPerformer = 0x81;
    internal const byte PackBlockSize = 0x8F;

    /// <summary>ISO 8859-1 character code for the block size info pack.</summary>
    internal const byte CharCodeIso88591 = 0x00;

    /// <summary>Language code for English per the CD-TEXT annex.</summary>
    internal const byte LanguageEnglish = 0x09;

    /// <summary>A block's sequence-number byte limits it to 256 packs.</summary>
    internal const int MaxPacksPerBlock = 256;

    private const int SizeInfoPackCount = 3;

    /// <summary>
    /// Builds the complete pack stream for a disc. Track metadata lists
    /// must have one element per track (element 0 = track 1); null
    /// entries become empty strings. Returns null — with a warning added —
    /// when there is no metadata at all, or when the text does not fit
    /// in a single 256-pack block.
    /// </summary>
    internal static byte[]? GeneratePacks(
        string? albumTitle,
        string? albumPerformer,
        IReadOnlyList<string?> trackTitles,
        IReadOnlyList<string?> trackPerformers,
        ICollection<string>? warnings = null)
    {
        if (trackTitles.Count != trackPerformers.Count)
        {
            throw new ArgumentException("Track title and performer lists must have the same length.", nameof(trackPerformers));
        }

        int trackCount = trackTitles.Count;

        bool anyTitle = !string.IsNullOrEmpty(albumTitle) || trackTitles.Any(t => !string.IsNullOrEmpty(t));
        bool anyPerformer = !string.IsNullOrEmpty(albumPerformer) || trackPerformers.Any(p => !string.IsNullOrEmpty(p));

        if (!anyTitle && !anyPerformer)
        {
            return null;
        }

        var packs = new List<byte[]>();
        int[] packCountsByType = new int[16];

        if (anyTitle)
        {
            int added = AppendTextPacks(packs, PackTitle, BuildStrings(albumTitle, trackTitles));
            packCountsByType[PackTitle - 0x80] = added;
        }

        if (anyPerformer)
        {
            int added = AppendTextPacks(packs, PackPerformer, BuildStrings(albumPerformer, trackPerformers));
            packCountsByType[PackPerformer - 0x80] = added;
        }

        if (packs.Count + SizeInfoPackCount > MaxPacksPerBlock)
        {
            warnings?.Add($"CD-TEXT needs {packs.Count + SizeInfoPackCount} packs but a block holds at most {MaxPacksPerBlock}; CD-TEXT was dropped. Shorten titles/performers.");
            return null;
        }

        packCountsByType[PackBlockSize - 0x80] = SizeInfoPackCount;
        AppendSizeInfoPacks(packs, trackCount, packCountsByType);

        byte[] result = new byte[packs.Count * PackSize];

        for (int i = 0; i < packs.Count; i++)
        {
            byte[] pack = packs[i];
            pack[2] = (byte)i; // block-wide sequence number
            BinaryPrimitives.WriteUInt16BigEndian(pack.AsSpan(16, 2), Crc16(pack.AsSpan(0, 16)));
            pack.CopyTo(result, i * PackSize);
        }

        return result;
    }

    /// <summary>
    /// Expands 18-byte packs into the 6-bit subchannel form streamed to
    /// the drive: every 3 pack bytes become 4 bytes each carrying 6 data
    /// bits, so one pack occupies 24 output bytes.
    /// </summary>
    internal static byte[] ExpandTo6Bit(ReadOnlySpan<byte> packs)
    {
        if (packs.Length % PackSize != 0)
        {
            throw new ArgumentException("Pack data length must be a multiple of 18 bytes.", nameof(packs));
        }

        byte[] subdata = new byte[packs.Length / 3 * 4];

        for (int i = 0; i < packs.Length; i += 3)
        {
            int si = i / 3 * 4;
            subdata[si + 0] = (byte)((packs[i + 0] >> 2) & 0x3F);
            subdata[si + 1] = (byte)(((packs[i + 0] << 4) & 0x30) | ((packs[i + 1] >> 4) & 0x0F));
            subdata[si + 2] = (byte)(((packs[i + 1] << 2) & 0x3C) | ((packs[i + 2] >> 6) & 0x03));
            subdata[si + 3] = (byte)(packs[i + 2] & 0x3F);
        }

        return subdata;
    }

    /// <summary>
    /// CD-Text CRC-16: polynomial 0x1021, initial value 0x0000, no
    /// reflection, final XOR 0xFFFF. Matches FoxRedbook's read-side
    /// implementation, which is locked against libcdio oracle data.
    /// </summary>
    internal static ushort Crc16(ReadOnlySpan<byte> data)
    {
        ushort crc = 0x0000;

        for (int i = 0; i < data.Length; i++)
        {
            crc ^= (ushort)(data[i] << 8);

            for (int j = 0; j < 8; j++)
            {
                if ((crc & 0x8000) != 0)
                {
                    crc = (ushort)((crc << 1) ^ 0x1021);
                }
                else
                {
                    crc = (ushort)(crc << 1);
                }
            }
        }

        return (ushort)(crc ^ 0xFFFF);
    }

    // ── Pack building ────────────────────────────────────────

    private static string[] BuildStrings(string? albumValue, IReadOnlyList<string?> trackValues)
    {
        string[] strings = new string[trackValues.Count + 1];
        strings[0] = albumValue ?? string.Empty;

        for (int i = 0; i < trackValues.Count; i++)
        {
            strings[i + 1] = trackValues[i] ?? string.Empty;
        }

        return strings;
    }

    private static int AppendTextPacks(List<byte[]> packs, byte packType, string[] strings)
    {
        Encoding latin1 = Encoding.GetEncoding(28591, new EncoderReplacementFallback("?"), DecoderFallback.ReplacementFallback);

        // Concatenate all strings (track 0 = disc level) with null
        // terminators, remembering which track and character position
        // every byte belongs to so pack headers can be filled in.
        var bytes = new List<byte>();
        var byteTrack = new List<byte>();
        var byteCharPos = new List<int>();

        for (int track = 0; track < strings.Length; track++)
        {
            byte[] encoded = latin1.GetBytes(strings[track]);

            for (int i = 0; i < encoded.Length; i++)
            {
                bytes.Add(encoded[i]);
                byteTrack.Add((byte)track);
                byteCharPos.Add(i);
            }

            bytes.Add(0x00);
            byteTrack.Add((byte)track);
            byteCharPos.Add(encoded.Length);
        }

        int added = 0;

        for (int offset = 0; offset < bytes.Count; offset += TextDataLength)
        {
            byte[] pack = new byte[PackSize];
            pack[0] = packType;
            pack[1] = byteTrack[offset];
            // pack[2] (sequence) is assigned once all packs exist.
            pack[3] = (byte)Math.Min(byteCharPos[offset], 15);

            for (int j = 0; j < TextDataLength && offset + j < bytes.Count; j++)
            {
                pack[4 + j] = bytes[offset + j];
            }

            packs.Add(pack);
            added++;
        }

        return added;
    }

    private static void AppendSizeInfoPacks(List<byte[]> packs, int trackCount, int[] packCountsByType)
    {
        // 36 bytes of size info spread over three 0x8F packs whose track
        // field carries the pack's index (0-2) within the group. Layout
        // verified against the 0x8F packs of libcdio's cdtext.cdt dump.
        Span<byte> content = stackalloc byte[SizeInfoPackCount * TextDataLength];
        content.Clear();

        content[0] = CharCodeIso88591;
        content[1] = 1;
        content[2] = (byte)trackCount;
        content[3] = 0x00; // copyright flags

        for (int i = 0; i < 16; i++)
        {
            content[4 + i] = (byte)packCountsByType[i];
        }

        content[20] = (byte)(packs.Count + SizeInfoPackCount - 1); // last sequence number, block 0
        content[28] = LanguageEnglish;                             // language code, block 0

        for (int i = 0; i < SizeInfoPackCount; i++)
        {
            byte[] pack = new byte[PackSize];
            pack[0] = PackBlockSize;
            pack[1] = (byte)i;
            content.Slice(i * TextDataLength, TextDataLength).CopyTo(pack.AsSpan(4, TextDataLength));
            packs.Add(pack);
        }
    }
}
