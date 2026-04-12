using System.Buffers.Binary;
using FoxRedbook.Platforms.Common;

namespace FoxRedbook.Tests;

/// <summary>
/// Pure-function tests for <see cref="ScsiCommands"/>. These verify CDB byte
/// layouts against the MMC-6 spec, parse synthetic response buffers, and
/// map synthetic sense data to the correct exception types. No hardware
/// required — all tests run in CI.
/// </summary>
public sealed class ScsiCommandsTests
{
    // ── INQUIRY CDB ───────────────────────────────────────────────

    [Fact]
    public void BuildInquiry_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[6];
        ScsiCommands.BuildInquiry(cdb);

        Assert.Equal(0x12, cdb[0]); // opcode INQUIRY
        Assert.Equal(0x00, cdb[1]); // EVPD=0
        Assert.Equal(0x00, cdb[2]); // page code (ignored)
        Assert.Equal(0x00, cdb[3]); // allocation length high byte
        Assert.Equal(0x24, cdb[4]); // allocation length low byte = 36
        Assert.Equal(0x00, cdb[5]); // control
    }

    [Fact]
    public void BuildInquiry_BufferTooSmall_Throws()
    {
        byte[] cdb = new byte[5];
        Assert.Throws<ArgumentException>(() => ScsiCommands.BuildInquiry(cdb));
    }

    // ── INQUIRY response parser ──────────────────────────────────

    [Fact]
    public void ParseInquiry_StandardResponse_ExtractsTrimmedFields()
    {
        byte[] response = new byte[36];
        // Byte 0: CD-ROM device type (0x05)
        response[0] = 0x05;
        // Vendor = "PLEXTOR " (8 chars with trailing space)
        WriteAscii(response, 8, "PLEXTOR ");
        // Product = "PX-716A         " (16 chars, space-padded)
        WriteAscii(response, 16, "PX-716A         ");
        // Revision = "1.04"
        WriteAscii(response, 32, "1.04");

        var inquiry = ScsiCommands.ParseInquiry(response);

        Assert.Equal("PLEXTOR", inquiry.Vendor);
        Assert.Equal("PX-716A", inquiry.Product);
        Assert.Equal("1.04", inquiry.Revision);
    }

    [Fact]
    public void ParseInquiry_ResponseTooShort_Throws()
    {
        byte[] response = new byte[35];
        Assert.Throws<ArgumentException>(() => ScsiCommands.ParseInquiry(response));
    }

    [Fact]
    public void ParseInquiry_OffsetDatabaseKey_Composed()
    {
        byte[] response = new byte[36];
        WriteAscii(response, 8, "LG      ");
        WriteAscii(response, 16, "GH24NSD1        ");
        WriteAscii(response, 32, "1.00");

        var inquiry = ScsiCommands.ParseInquiry(response);

        Assert.Equal("LG - GH24NSD1", inquiry.OffsetDatabaseKey);
    }

    // ── READ TOC CDB ─────────────────────────────────────────────

    [Fact]
    public void BuildReadToc_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        ScsiCommands.BuildReadToc(cdb);

        Assert.Equal(0x43, cdb[0]); // READ TOC opcode
        Assert.Equal(0x00, cdb[1]); // MSF=0 (LBA mode)
        Assert.Equal(0x00, cdb[2]); // format 0 (standard TOC)
        Assert.Equal(0x00, cdb[3]);
        Assert.Equal(0x00, cdb[4]);
        Assert.Equal(0x00, cdb[5]);
        Assert.Equal(0x01, cdb[6]); // starting track = 1
        // Allocation length = 804 = 0x0324, big-endian
        Assert.Equal(0x03, cdb[7]);
        Assert.Equal(0x24, cdb[8]);
        Assert.Equal(0x00, cdb[9]); // control
    }

    // ── READ TOC response parser ─────────────────────────────────

    [Fact]
    public void ParseReadTocResponse_SingleAudioTrack()
    {
        // 4-byte header + 2 descriptors (1 track + lead-out) = 20 bytes
        // Data length (excluding itself) = 18
        byte[] response = BuildTocResponse(
            firstTrack: 1,
            lastTrack: 1,
            descriptors:
            [
                (1, 0x10, 150),        // track 1, audio, LBA 150
                (0xAA, 0x10, 200000),  // lead-out
            ]);

        var toc = ScsiCommands.ParseReadTocResponse(response);

        Assert.Equal(1, toc.FirstTrackNumber);
        Assert.Equal(1, toc.LastTrackNumber);
        Assert.Equal(200000L, toc.LeadOutLba);
        Assert.Single(toc.Tracks);

        var t = toc.Tracks[0];
        Assert.Equal(1, t.Number);
        Assert.Equal(150L, t.StartLba);
        Assert.Equal(199850, t.SectorCount); // 200000 - 150
        Assert.Equal(TrackType.Audio, t.Type);
    }

    [Fact]
    public void ParseReadTocResponse_ThirteenAudioTracks()
    {
        var descriptors = new List<(byte trackNumber, byte control, uint lba)>();
        uint lba = 150;

        for (int i = 1; i <= 13; i++)
        {
            descriptors.Add(((byte)i, 0x10, lba));
            lba += 20000;
        }

        descriptors.Add((0xAA, 0x10, lba));

        byte[] response = BuildTocResponse(1, 13, descriptors);
        var toc = ScsiCommands.ParseReadTocResponse(response);

        Assert.Equal(13, toc.Tracks.Count);
        Assert.All(toc.Tracks, t => Assert.Equal(TrackType.Audio, t.Type));
        Assert.Equal(lba, (uint)toc.LeadOutLba);
    }

    [Fact]
    public void ParseReadTocResponse_MixedModeDetectsDataTrack()
    {
        // Audio tracks followed by a data track (Enhanced CD pattern).
        // Control nibble with bit 2 set (0x04) indicates data track.
        byte[] response = BuildTocResponse(
            firstTrack: 1,
            lastTrack: 3,
            descriptors:
            [
                (1,    0x10, 150),     // audio
                (2,    0x10, 50000),   // audio
                (3,    0x14, 283535),  // data track (bit 2 set)
                (0xAA, 0x14, 383535),
            ]);

        var toc = ScsiCommands.ParseReadTocResponse(response);

        Assert.Equal(3, toc.Tracks.Count);
        Assert.Equal(TrackType.Audio, toc.Tracks[0].Type);
        Assert.Equal(TrackType.Audio, toc.Tracks[1].Type);
        Assert.Equal(TrackType.Data, toc.Tracks[2].Type);
        Assert.True((toc.Tracks[2].Control & TrackControl.DataTrack) != 0);
    }

    [Fact]
    public void ParseReadTocResponse_OutOfOrderTracks_SortedByNumber()
    {
        // Drive reports descriptors out of order — parser should sort.
        byte[] response = BuildTocResponse(
            firstTrack: 1,
            lastTrack: 3,
            descriptors:
            [
                (3,    0x10, 100000),
                (1,    0x10, 150),
                (2,    0x10, 50000),
                (0xAA, 0x10, 150000),
            ]);

        var toc = ScsiCommands.ParseReadTocResponse(response);

        Assert.Equal(1, toc.Tracks[0].Number);
        Assert.Equal(2, toc.Tracks[1].Number);
        Assert.Equal(3, toc.Tracks[2].Number);
    }

    [Fact]
    public void ParseReadTocResponse_MissingLeadOut_Throws()
    {
        byte[] response = BuildTocResponse(
            firstTrack: 1,
            lastTrack: 1,
            descriptors: [(1, 0x10, 150)]);

        Assert.Throws<ArgumentException>(() => ScsiCommands.ParseReadTocResponse(response));
    }

    [Fact]
    public void ParseReadTocResponse_PreEmphasisFlag_Parsed()
    {
        // Control nibble bit 0 = PreEmphasis
        byte[] response = BuildTocResponse(
            firstTrack: 1,
            lastTrack: 1,
            descriptors:
            [
                (1, 0x11, 150), // PreEmphasis | audio
                (0xAA, 0x10, 200000),
            ]);

        var toc = ScsiCommands.ParseReadTocResponse(response);

        Assert.True((toc.Tracks[0].Control & TrackControl.PreEmphasis) != 0);
        Assert.Equal(TrackType.Audio, toc.Tracks[0].Type);
    }

    // ── READ CD CDB ──────────────────────────────────────────────

    [Fact]
    public void BuildReadCd_AudioOnly_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[12];
        ScsiCommands.BuildReadCd(cdb, lba: 0x12345678, sectorCount: 10, ReadOptions.None);

        Assert.Equal(0xBE, cdb[0]);  // opcode
        Assert.Equal(0x00, cdb[1]);  // any sector type (compatibility default)
        // LBA big-endian
        Assert.Equal(0x12, cdb[2]);
        Assert.Equal(0x34, cdb[3]);
        Assert.Equal(0x56, cdb[4]);
        Assert.Equal(0x78, cdb[5]);
        // Transfer length 10 in 24-bit big-endian
        Assert.Equal(0x00, cdb[6]);
        Assert.Equal(0x00, cdb[7]);
        Assert.Equal(0x0A, cdb[8]);
        Assert.Equal(0x10, cdb[9]);  // MCSB: user data only (bit 4)
        Assert.Equal(0x00, cdb[10]); // subchannel: none
        Assert.Equal(0x00, cdb[11]); // control
    }

    [Fact]
    public void BuildReadCd_WithC2ErrorPointers_SetsBit1()
    {
        Span<byte> cdb = stackalloc byte[12];
        ScsiCommands.BuildReadCd(cdb, lba: 0, sectorCount: 1, ReadOptions.C2ErrorPointers);

        Assert.Equal(0x12, cdb[9]);  // 0x10 (user data) | 0x02 (C2 pointers)
        Assert.Equal(0x00, cdb[10]); // no subchannel
    }

    [Fact]
    public void BuildReadCd_WithSubchannel_SetsByte10()
    {
        Span<byte> cdb = stackalloc byte[12];
        ScsiCommands.BuildReadCd(cdb, lba: 0, sectorCount: 1, ReadOptions.SubchannelData);

        Assert.Equal(0x10, cdb[9]);  // user data only
        Assert.Equal(0x01, cdb[10]); // raw P-W subchannel
    }

    [Fact]
    public void BuildReadCd_WithC2AndSubchannel_CombinesBoth()
    {
        Span<byte> cdb = stackalloc byte[12];
        ScsiCommands.BuildReadCd(
            cdb,
            lba: 0,
            sectorCount: 1,
            ReadOptions.C2ErrorPointers | ReadOptions.SubchannelData);

        Assert.Equal(0x12, cdb[9]);
        Assert.Equal(0x01, cdb[10]);
    }

    [Fact]
    public void BuildReadCd_MaxTransferLength_EncodesFull24Bits()
    {
        Span<byte> cdb = stackalloc byte[12];
        ScsiCommands.BuildReadCd(cdb, lba: 0, sectorCount: 0xFFFFFF, ReadOptions.None);

        Assert.Equal(0xFF, cdb[6]);
        Assert.Equal(0xFF, cdb[7]);
        Assert.Equal(0xFF, cdb[8]);
    }

    [Fact]
    public void BuildReadCd_NegativeLba_Throws()
    {
        byte[] cdb = new byte[12];
        Assert.Throws<ArgumentOutOfRangeException>(
            () => ScsiCommands.BuildReadCd(cdb, lba: -1, sectorCount: 1, ReadOptions.None));
    }

    [Fact]
    public void BuildReadCd_SectorCountOverflow_Throws()
    {
        byte[] cdb = new byte[12];
        Assert.Throws<ArgumentOutOfRangeException>(
            () => ScsiCommands.BuildReadCd(cdb, lba: 0, sectorCount: 0x1000000, ReadOptions.None));
    }

    // ── Sense data mapping ──────────────────────────────────────

    [Fact]
    public void MapSenseData_MediumNotPresent_ReturnsMediaNotPresentException()
    {
        byte[] sense = BuildSense(key: 0x02, asc: 0x3A, ascq: 0x00);
        var ex = ScsiCommands.MapSenseData(sense);

        Assert.IsType<MediaNotPresentException>(ex);
    }

    [Fact]
    public void MapSenseData_NotReady_BecomingReady_ReturnsDriveNotReadyException()
    {
        byte[] sense = BuildSense(key: 0x02, asc: 0x04, ascq: 0x01);
        var ex = ScsiCommands.MapSenseData(sense);

        Assert.IsType<DriveNotReadyException>(ex);
    }

    [Fact]
    public void MapSenseData_MediumError_ReturnsGenericOpticalDriveException()
    {
        byte[] sense = BuildSense(key: 0x03, asc: 0x11, ascq: 0x00);
        var ex = ScsiCommands.MapSenseData(sense);

        Assert.IsType<OpticalDriveException>(ex);
        Assert.False(ex is MediaNotPresentException or DriveNotReadyException);
    }

    [Fact]
    public void MapSenseData_EmptyBuffer_ReturnsGenericException()
    {
        var ex = ScsiCommands.MapSenseData(ReadOnlySpan<byte>.Empty);

        Assert.IsType<OpticalDriveException>(ex);
    }

    [Fact]
    public void MapSenseData_DescriptorFormat_ReturnsGenericException()
    {
        byte[] sense = new byte[18];
        sense[0] = 0x72; // descriptor-format current error

        var ex = ScsiCommands.MapSenseData(sense);

        Assert.IsType<OpticalDriveException>(ex);
    }

    [Fact]
    public void MapSenseData_TruncatedFixedFormat_ReturnsGenericException()
    {
        byte[] sense = new byte[10]; // too short for ASC/ASCQ access
        sense[0] = 0x70;
        sense[2] = 0x02;

        var ex = ScsiCommands.MapSenseData(sense);

        Assert.IsType<OpticalDriveException>(ex);
    }

    // ── Helpers ─────────────────────────────────────────────────

    private static void WriteAscii(byte[] buffer, int offset, string text)
    {
        for (int i = 0; i < text.Length; i++)
        {
            buffer[offset + i] = (byte)text[i];
        }
    }

    private static byte[] BuildTocResponse(
        byte firstTrack,
        byte lastTrack,
        List<(byte trackNumber, byte adrControl, uint lba)> descriptors)
    {
        int descriptorBytes = descriptors.Count * 8;
        int totalBytes = 4 + descriptorBytes;
        byte[] response = new byte[totalBytes];

        // Data length does NOT include its own 2 bytes
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(0, 2), (ushort)(totalBytes - 2));
        response[2] = firstTrack;
        response[3] = lastTrack;

        for (int i = 0; i < descriptors.Count; i++)
        {
            int offset = 4 + i * 8;
            // byte 0: reserved
            response[offset + 1] = descriptors[i].adrControl;
            response[offset + 2] = descriptors[i].trackNumber;
            // byte 3: reserved
            BinaryPrimitives.WriteUInt32BigEndian(
                response.AsSpan(offset + 4, 4),
                descriptors[i].lba);
        }

        return response;
    }

    private static byte[] BuildSense(byte key, byte asc, byte ascq)
    {
        byte[] sense = new byte[18];
        sense[0] = 0x70; // fixed-format current error
        sense[2] = (byte)(key & 0x0F);
        sense[7] = 10; // additional sense length
        sense[12] = asc;
        sense[13] = ascq;
        return sense;
    }
}
