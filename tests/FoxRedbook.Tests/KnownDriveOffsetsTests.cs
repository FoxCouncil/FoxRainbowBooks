using System.Buffers.Binary;

namespace FoxRedbook.Tests;

public sealed class KnownDriveOffsetsTests
{
    // ── Binary format round-trip ──────────────────────────────

    [Fact]
    public void SerializeAndParse_RoundTrips()
    {
        var entries = new List<(string Vendor, string Product, short Offset, uint Submissions)>
        {
            ("PIONEER", "BD-RW BDR-XS07U", 667, 42),
            ("PLEXTOR", "CD-R PX-W1210A", 99, 1000),
            ("TSSTCORP", "CDDVDW SH-S223C", -6, 500),
        };

        var date = new DateTime(2026, 4, 12, 0, 0, 0, DateTimeKind.Utc);
        byte[] binary = KnownDriveOffsets.SerializeBinary(entries, date);

        // Parse it back.
        var parsed = KnownDriveOffsets.ParseBinary(binary);

        Assert.Equal(3, parsed.Offsets.Count);
        Assert.Equal("2026-04-12", parsed.SnapshotDate);

        Assert.Equal(667, parsed.Offsets["PIONEER|BD-RW BDR-XS07U"]);
        Assert.Equal(99, parsed.Offsets["PLEXTOR|CD-R PX-W1210A"]);
        Assert.Equal(-6, parsed.Offsets["TSSTCORP|CDDVDW SH-S223C"]);
    }

    [Fact]
    public void SerializeBinary_Header_HasCorrectMagicAndVersion()
    {
        var entries = new List<(string Vendor, string Product, short Offset, uint Submissions)>
        {
            ("TEST", "DRIVE", 0, 0),
        };

        byte[] binary = KnownDriveOffsets.SerializeBinary(entries, DateTime.UtcNow);

        Assert.True(binary.Length >= 14);
        Assert.Equal((byte)'F', binary[0]);
        Assert.Equal((byte)'R', binary[1]);
        Assert.Equal((byte)'D', binary[2]);
        Assert.Equal((byte)'O', binary[3]);

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(binary.AsSpan(4, 2));
        Assert.Equal(1, version);

        uint count = BinaryPrimitives.ReadUInt32LittleEndian(binary.AsSpan(10, 4));
        Assert.Equal(1u, count);
    }

    [Fact]
    public void ParseBinary_NegativeOffset_PreservedCorrectly()
    {
        var entries = new List<(string Vendor, string Product, short Offset, uint Submissions)>
        {
            ("VENDOR", "PRODUCT", -355, 100),
        };

        byte[] binary = KnownDriveOffsets.SerializeBinary(entries, DateTime.UtcNow);
        var parsed = KnownDriveOffsets.ParseBinary(binary);

        Assert.Equal(-355, parsed.Offsets["VENDOR|PRODUCT"]);
    }

    // ── Lookup API ───────────────────────────────────────────

    [Fact]
    public void Lookup_UnknownDrive_ReturnsNull()
    {
        Assert.Null(KnownDriveOffsets.Lookup("NONEXISTENT", "DRIVE MODEL XYZ"));
    }

    [Fact]
    public void Lookup_ByDriveInquiry_UnknownReturnsNull()
    {
        var inquiry = new DriveInquiry
        {
            Vendor = "NONEXISTENT",
            Product = "UNKNOWN",
            Revision = "1.0",
        };

        Assert.Null(KnownDriveOffsets.Lookup(inquiry));
    }

    [Fact]
    public void Lookup_CaseInsensitive_ViaNormalization()
    {
        // If the embedded DB has the drive in any case, normalization
        // should match. We can't test against the real DB reliably here
        // (it might not contain a specific drive), so we test that the
        // normalization path is exercised without throwing.
        int? result1 = KnownDriveOffsets.Lookup("pioneer", "bd-rw  bdr-xs07u");
        int? result2 = KnownDriveOffsets.Lookup("PIONEER", "BD-RW BDR-XS07U");

        // Both should return the same result (either both null or both the same offset).
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void DatabaseDate_IsNonEmpty()
    {
        // If no embedded resource exists, returns "none".
        Assert.False(string.IsNullOrEmpty(KnownDriveOffsets.DatabaseDate));
    }

    // ── RipSession.CreateAutoCorrected ───────────────────────

    [Fact]
    public void CreateAutoCorrected_NullDrive_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => RipSession.CreateAutoCorrected(null!));
    }
}
