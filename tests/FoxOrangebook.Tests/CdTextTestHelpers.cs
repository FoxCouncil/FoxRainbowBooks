using System.Buffers.Binary;
using FoxOrangebook;
using FoxRedbook;
using FoxRedbook.Platforms.Common;

namespace FoxOrangebook.Tests;

/// <summary>
/// Shared helpers for decoding CD-TEXT lead-in data captured from a burn
/// back into 18-byte packs, and round-tripping them through FoxRedbook's
/// oracle-verified parser.
/// </summary>
internal static class CdTextTestHelpers
{
    /// <summary>
    /// Reverses the 6-bit subchannel expansion: every 4 written bytes
    /// (6 data bits each) collapse back into 3 pack bytes.
    /// </summary>
    internal static byte[] CollapseFrom6Bit(ReadOnlySpan<byte> subdata)
    {
        byte[] packs = new byte[subdata.Length / 4 * 3];

        for (int si = 0; si + 4 <= subdata.Length; si += 4)
        {
            int i = si / 4 * 3;
            packs[i + 0] = (byte)(((subdata[si + 0] & 0x3F) << 2) | ((subdata[si + 1] >> 4) & 0x03));
            packs[i + 1] = (byte)(((subdata[si + 1] & 0x0F) << 4) | ((subdata[si + 2] >> 2) & 0x0F));
            packs[i + 2] = (byte)(((subdata[si + 2] & 0x03) << 6) | (subdata[si + 3] & 0x3F));
        }

        return packs;
    }

    /// <summary>
    /// The lead-in cycles the pack sequence to fill its full length.
    /// Returns just the first cycle: packs up to (excluding) the first
    /// recurrence of sequence number 0.
    /// </summary>
    internal static byte[] TakeFirstCycle(ReadOnlySpan<byte> packs)
    {
        int packCount = packs.Length / CdTextEncoder.PackSize;

        for (int i = 1; i < packCount; i++)
        {
            if (packs[i * CdTextEncoder.PackSize + 2] == 0)
            {
                return packs.Slice(0, i * CdTextEncoder.PackSize).ToArray();
            }
        }

        return packs.ToArray();
    }

    /// <summary>
    /// Runs a raw pack stream through FoxRedbook's CD-TEXT parser (the
    /// read side, locked against libcdio oracle data) by wrapping it in
    /// a synthetic READ TOC format 5 response header.
    /// </summary>
    internal static CdText? ParseWithFoxRedbook(byte[] packs)
    {
        byte[] response = new byte[4 + packs.Length];
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(0, 2), (ushort)(packs.Length + 2));
        packs.CopyTo(response, 4);
        return CdTextCommands.ParseCdText(response);
    }
}
