using System.Buffers.Binary;
using System.Reflection;
using System.Text;

namespace FoxRedbook;

/// <summary>
/// Read-only lookup against the embedded AccurateRip drive offset database.
/// The database is a build-time snapshot produced by the
/// <c>tools/drive-offsets-snapshot</c> tool and shipped as an embedded
/// resource in the NuGet package.
/// </summary>
public static class KnownDriveOffsets
{
    private static readonly Lazy<OffsetDatabase> Database = new(LoadDatabase);

    /// <summary>
    /// Looks up the AccurateRip read offset for the drive described by
    /// <paramref name="inquiry"/>. Returns the offset in sample frames, or
    /// <see langword="null"/> if the drive is not in the embedded database.
    /// </summary>
    public static int? Lookup(DriveInquiry inquiry)
    {
        return Lookup(inquiry.Vendor, inquiry.Product);
    }

    /// <summary>
    /// Looks up the read offset by explicit vendor and product strings.
    /// Applies the same normalization as <see cref="Lookup(DriveInquiry)"/>.
    /// </summary>
    public static int? Lookup(string vendor, string product)
    {
        string key = DriveNameNormalizer.BuildKey(
            DriveNameNormalizer.Normalize(vendor),
            DriveNameNormalizer.Normalize(product));

        var db = Database.Value;

        if (db.Offsets.TryGetValue(key, out int offset))
        {
            return offset;
        }

        return null;
    }

    /// <summary>Snapshot date of the embedded database (yyyy-MM-dd).</summary>
    public static string DatabaseDate => Database.Value.SnapshotDate;

    /// <summary>Number of drives in the embedded database.</summary>
    public static int EntryCount => Database.Value.Offsets.Count;

    // ── Binary format ─────────────────────────────────────────

    internal const string ResourceName = "FoxRedbook.Resources.drive-offsets.bin";
    internal static readonly byte[] Magic = "FRDO"u8.ToArray();
    internal const ushort CurrentFormatVersion = 1;

    internal sealed class OffsetDatabase
    {
        public required Dictionary<string, int> Offsets { get; init; }
        public required string SnapshotDate { get; init; }
    }

    private static OffsetDatabase LoadDatabase()
    {
        var assembly = typeof(KnownDriveOffsets).Assembly;
        using Stream? stream = assembly.GetManifestResourceStream(ResourceName);

        if (stream is null)
        {
            return new OffsetDatabase
            {
                Offsets = new Dictionary<string, int>(StringComparer.Ordinal),
                SnapshotDate = "none",
            };
        }

        byte[] data = new byte[stream.Length];
        stream.ReadExactly(data);

        return ParseBinary(data);
    }

    internal static OffsetDatabase ParseBinary(ReadOnlySpan<byte> data)
    {
        if (data.Length < 14)
        {
            throw new InvalidOperationException("Drive offset database is too short.");
        }

        if (!data.Slice(0, 4).SequenceEqual(Magic))
        {
            throw new InvalidOperationException("Drive offset database has invalid magic bytes.");
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(4));

        if (version != CurrentFormatVersion)
        {
            throw new InvalidOperationException(
                $"Drive offset database version {version} is not supported (expected {CurrentFormatVersion}).");
        }

        uint daysSince2000 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(6));
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(10));

        DateTime epoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        string snapshotDate = epoch.AddDays(daysSince2000).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

        var offsets = new Dictionary<string, int>((int)count, StringComparer.Ordinal);
        int pos = 14;

        for (uint i = 0; i < count; i++)
        {
            if (pos >= data.Length)
            {
                break;
            }

            int vendorLen = data[pos++];
            string vendor = Encoding.ASCII.GetString(data.Slice(pos, vendorLen));
            pos += vendorLen;

            int productLen = data[pos++];
            string product = Encoding.ASCII.GetString(data.Slice(pos, productLen));
            pos += productLen;

            short offset = BinaryPrimitives.ReadInt16LittleEndian(data.Slice(pos));
            pos += 2;

            // uint submissions (4 bytes) — read but not stored at runtime.
            pos += 4;

            string key = DriveNameNormalizer.BuildKey(vendor, product);
            offsets.TryAdd(key, offset);
        }

        return new OffsetDatabase
        {
            Offsets = offsets,
            SnapshotDate = snapshotDate,
        };
    }

    /// <summary>
    /// Serializes a set of entries into the binary format. Used by the
    /// snapshot tool and by round-trip tests.
    /// </summary>
    internal static byte[] SerializeBinary(
        IReadOnlyList<(string Vendor, string Product, short Offset, uint Submissions)> entries,
        DateTime snapshotDate)
    {
        using var ms = new MemoryStream();

        // Header
        ms.Write(Magic);
        Span<byte> buf = stackalloc byte[4];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, CurrentFormatVersion);
        ms.Write(buf.Slice(0, 2));

        DateTime epoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        uint daysSince2000 = (uint)(snapshotDate.Date - epoch).TotalDays;
        BinaryPrimitives.WriteUInt32LittleEndian(buf, daysSince2000);
        ms.Write(buf);

        BinaryPrimitives.WriteUInt32LittleEndian(buf, (uint)entries.Count);
        ms.Write(buf);

        // Entries
        foreach (var (vendor, product, offset, submissions) in entries)
        {
            byte[] vendorBytes = Encoding.ASCII.GetBytes(vendor);
            ms.WriteByte((byte)vendorBytes.Length);
            ms.Write(vendorBytes);

            byte[] productBytes = Encoding.ASCII.GetBytes(product);
            ms.WriteByte((byte)productBytes.Length);
            ms.Write(productBytes);

            BinaryPrimitives.WriteInt16LittleEndian(buf, offset);
            ms.Write(buf.Slice(0, 2));

            BinaryPrimitives.WriteUInt32LittleEndian(buf, submissions);
            ms.Write(buf);
        }

        return ms.ToArray();
    }
}
