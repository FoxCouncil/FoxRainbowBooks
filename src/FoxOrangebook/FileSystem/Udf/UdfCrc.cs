namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// CRC-16-CCITT (polynomial 0x1021, initial value 0) as specified for UDF
/// descriptor CRCs (ECMA-167 §7.2.6), plus the descriptor tag checksum.
/// </summary>
internal static class UdfCrc
{
    private static readonly ushort[] Table = BuildTable();

    private static ushort[] BuildTable()
    {
        var table = new ushort[256];

        for (int i = 0; i < 256; i++)
        {
            ushort crc = (ushort)(i << 8);

            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 0x8000) != 0 ? (ushort)((crc << 1) ^ 0x1021) : (ushort)(crc << 1);
            }

            table[i] = crc;
        }

        return table;
    }

    /// <summary>Computes the CRC-16-CCITT over the given bytes.</summary>
    public static ushort Compute(ReadOnlySpan<byte> data)
    {
        ushort crc = 0;

        foreach (byte b in data)
        {
            crc = (ushort)((crc << 8) ^ Table[((crc >> 8) ^ b) & 0xFF]);
        }

        return crc;
    }

    /// <summary>
    /// Computes the descriptor tag checksum: the modulo-256 sum of tag bytes
    /// 0–3 and 5–15 (byte 4, the checksum field itself, is excluded).
    /// </summary>
    public static byte TagChecksum(ReadOnlySpan<byte> tag)
    {
        int sum = 0;

        for (int i = 0; i < 16; i++)
        {
            if (i == 4)
            {
                continue;
            }

            sum += tag[i];
        }

        return (byte)sum;
    }
}
