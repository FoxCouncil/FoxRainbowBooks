using System.Buffers.Binary;

namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// Encoders for the shared UDF / ECMA-167 on-disc primitives: descriptor tags,
/// OSTA compressed-Unicode d-strings, timestamps, entity identifiers, the
/// extent/allocation address structures, and the volume recognition descriptors.
/// </summary>
internal static class UdfEncoding
{
    private const ushort TagSerialNumber = 1;

    /// <summary>
    /// Fills in the 16-byte descriptor tag once the descriptor body (bytes 16..)
    /// has been written. Computes the CRC over <paramref name="crcLength"/> bytes
    /// following the tag and the modulo-256 tag checksum.
    /// </summary>
    public static void WriteTag(Span<byte> descriptor, ushort tagId, uint tagLocation, int crcLength)
    {
        descriptor[0] = (byte)(tagId & 0xFF);
        descriptor[1] = (byte)(tagId >> 8);
        BinaryPrimitives.WriteUInt16LittleEndian(descriptor.Slice(2, 2), UdfConstants.DescriptorVersion);
        descriptor[4] = 0; // checksum, filled last
        descriptor[5] = 0; // reserved
        BinaryPrimitives.WriteUInt16LittleEndian(descriptor.Slice(6, 2), TagSerialNumber);

        ushort crc = UdfCrc.Compute(descriptor.Slice(16, crcLength));
        BinaryPrimitives.WriteUInt16LittleEndian(descriptor.Slice(8, 2), crc);
        BinaryPrimitives.WriteUInt16LittleEndian(descriptor.Slice(10, 2), (ushort)crcLength);
        BinaryPrimitives.WriteUInt32LittleEndian(descriptor.Slice(12, 4), tagLocation);

        descriptor[4] = UdfCrc.TagChecksum(descriptor.Slice(0, 16));
    }

    /// <summary>Writes the default CRC length (whole descriptor minus the tag) tag.</summary>
    public static void WriteTag(Span<byte> descriptor, ushort tagId, uint tagLocation)
    {
        WriteTag(descriptor, tagId, tagLocation, descriptor.Length - 16);
    }

    // ── OSTA compressed Unicode d-strings (UDF §2.1.1) ───────────

    /// <summary>
    /// Writes a fixed-width d-string: compression id (8 for Latin-1, 16 for
    /// UCS-2 BE), the characters, and a trailing length byte at the field end.
    /// </summary>
    public static void WriteDString(Span<byte> field, string? value)
    {
        field.Clear();

        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        bool needsUnicode = false;

        foreach (char c in value)
        {
            if (c > 0xFF)
            {
                needsUnicode = true;
                break;
            }
        }

        int capacity = field.Length - 1; // last byte holds the used length
        int used;

        if (!needsUnicode)
        {
            int maxChars = capacity - 1;
            int n = Math.Min(value.Length, maxChars);
            field[0] = 8;

            for (int i = 0; i < n; i++)
            {
                field[1 + i] = (byte)value[i];
            }

            used = 1 + n;
        }
        else
        {
            int maxChars = (capacity - 1) / 2;
            int n = Math.Min(value.Length, maxChars);
            field[0] = 16;

            for (int i = 0; i < n; i++)
            {
                field[1 + i * 2] = (byte)(value[i] >> 8);
                field[2 + i * 2] = (byte)(value[i] & 0xFF);
            }

            used = 1 + n * 2;
        }

        field[^1] = (byte)used;
    }

    /// <summary>
    /// Encodes an identifier as OSTA compressed Unicode for a File Identifier
    /// Descriptor (compression id + characters, with no trailing length byte).
    /// Returns an empty array for null/empty (the parent "..") identifier.
    /// </summary>
    public static byte[] EncodeCompressedUnicode(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return Array.Empty<byte>();
        }

        bool needsUnicode = false;

        foreach (char c in value)
        {
            if (c > 0xFF)
            {
                needsUnicode = true;
                break;
            }
        }

        if (!needsUnicode)
        {
            var bytes = new byte[1 + value.Length];
            bytes[0] = 8;

            for (int i = 0; i < value.Length; i++)
            {
                bytes[1 + i] = (byte)value[i];
            }

            return bytes;
        }

        var wide = new byte[1 + value.Length * 2];
        wide[0] = 16;

        for (int i = 0; i < value.Length; i++)
        {
            wide[1 + i * 2] = (byte)(value[i] >> 8);
            wide[2 + i * 2] = (byte)(value[i] & 0xFF);
        }

        return wide;
    }

    // ── Timestamp (ECMA-167 §1.7.3) ──────────────────────────────

    public static void WriteTimestamp(Span<byte> field, DateTimeOffset when)
    {
        // Type 1 (local time) in the high nibble; timezone in minutes in the
        // low 12 bits as a signed value.
        int tz = (int)when.Offset.TotalMinutes;
        ushort typeAndTz = (ushort)((1 << 12) | (tz & 0x0FFF));
        BinaryPrimitives.WriteUInt16LittleEndian(field.Slice(0, 2), typeAndTz);
        BinaryPrimitives.WriteInt16LittleEndian(field.Slice(2, 2), (short)when.Year);
        field[4] = (byte)when.Month;
        field[5] = (byte)when.Day;
        field[6] = (byte)when.Hour;
        field[7] = (byte)when.Minute;
        field[8] = (byte)when.Second;
        field[9] = (byte)(when.Millisecond / 10);
        field[10] = 0;
        field[11] = 0;
    }

    // ── Entity identifiers / regid (ECMA-167 §1.7.4) ─────────────

    public static void WriteDomainEntityId(Span<byte> field, string identifier)
    {
        WriteEntityId(field, identifier, suffix =>
        {
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.Slice(0, 2), UdfConstants.UdfRevision);
            suffix[2] = 0; // domain flags (no write protection)
        });
    }

    public static void WriteUdfEntityId(Span<byte> field, string identifier)
    {
        WriteEntityId(field, identifier, suffix =>
        {
            BinaryPrimitives.WriteUInt16LittleEndian(suffix.Slice(0, 2), UdfConstants.UdfRevision);
            suffix[2] = UdfConstants.OsClassUndefined;
            suffix[3] = UdfConstants.OsIdentifierUndefined;
        });
    }

    public static void WriteImplementationEntityId(Span<byte> field, string identifier)
    {
        WriteEntityId(field, identifier, suffix =>
        {
            suffix[0] = UdfConstants.OsClassUndefined;
            suffix[1] = UdfConstants.OsIdentifierUndefined;
        });
    }

    /// <summary>Writes an entity identifier with an all-zero suffix (e.g. "+NSR02").</summary>
    public static void WriteRawEntityId(Span<byte> field, string identifier)
    {
        WriteEntityId(field, identifier, _ => { });
    }

    /// <summary>Writes an OSTA CS0 charspec: character set type 0 + "OSTA Compressed Unicode".</summary>
    public static void WriteCharSpec(Span<byte> field)
    {
        field.Slice(0, 64).Clear();
        field[0] = 0; // CS0
        "OSTA Compressed Unicode"u8.CopyTo(field.Slice(1));
    }

    private static void WriteEntityId(Span<byte> field, string identifier, SuffixWriter suffixWriter)
    {
        field.Slice(0, 32).Clear();
        field[0] = 0; // flags

        int n = Math.Min(identifier.Length, 23);

        for (int i = 0; i < n; i++)
        {
            field[1 + i] = (byte)identifier[i];
        }

        suffixWriter(field.Slice(24, 8));
    }

    private delegate void SuffixWriter(Span<byte> suffix);

    // ── Address structures (ECMA-167 §7.1, §14.14.1) ─────────────

    /// <summary>extent_ad: 32-bit length (bytes) + 32-bit logical sector location.</summary>
    public static void WriteExtentAd(Span<byte> field, uint lengthBytes, uint location)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(0, 4), lengthBytes);
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(4, 4), location);
    }

    /// <summary>lb_addr: 32-bit partition-relative block + 16-bit partition reference.</summary>
    public static void WriteLbAddr(Span<byte> field, uint logicalBlock, ushort partitionReference)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(0, 4), logicalBlock);
        BinaryPrimitives.WriteUInt16LittleEndian(field.Slice(4, 2), partitionReference);
    }

    /// <summary>short_ad: 32-bit extent length (type in high 2 bits) + 32-bit partition-relative position.</summary>
    public static void WriteShortAd(Span<byte> field, uint lengthBytes, uint position)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(0, 4), lengthBytes);
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(4, 4), position);
    }

    /// <summary>long_ad: 32-bit extent length + lb_addr + 6-byte implementation use.</summary>
    public static void WriteLongAd(Span<byte> field, uint lengthBytes, uint logicalBlock, ushort partitionReference)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(field.Slice(0, 4), lengthBytes);
        WriteLbAddr(field.Slice(4, 6), logicalBlock, partitionReference);
        field.Slice(10, 6).Clear();
    }

    // ── Volume recognition sequence (ECMA-167 §2 / UDF §2.1.7) ───

    public static byte[] BuildRecognitionDescriptor(string standardIdentifier)
    {
        var sector = new byte[IsoConstants.LogicalSectorSize];
        sector[0] = 0; // structure type

        for (int i = 0; i < 5; i++)
        {
            sector[1 + i] = (byte)standardIdentifier[i];
        }

        sector[6] = 1; // structure version
        return sector;
    }
}
