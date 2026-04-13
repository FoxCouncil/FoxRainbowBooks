using System.Buffers.Binary;
using FoxOrangebook;

namespace FoxOrangebook.Tests;

public sealed class BurnCommandsTests
{
    // ── GET CONFIGURATION ────────────────────────────────────

    [Fact]
    public void BuildGetConfiguration_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildGetConfiguration(cdb, BurnCommands.FeatureCdMastering, 16);

        Assert.Equal(0x46, cdb[0]);
        Assert.Equal(0x02, cdb[1]); // RT = one feature
        Assert.Equal(0x00, cdb[2]); // feature high
        Assert.Equal(0x2F, cdb[3]); // feature low = CD Mastering
        Assert.Equal(0x00, cdb[7]); // alloc high
        Assert.Equal(0x10, cdb[8]); // alloc low = 16
    }

    [Fact]
    public void BuildGetConfiguration_BufferTooSmall_Throws()
    {
        byte[] cdb = new byte[9];
        Assert.Throws<ArgumentException>(() => BurnCommands.BuildGetConfiguration(cdb, 0x002F, 16));
    }

    [Fact]
    public void ParseGetConfiguration_FeaturePresent_ReturnsTrue()
    {
        byte[] response = new byte[16];
        BinaryPrimitives.WriteUInt32BigEndian(response, 12); // data length
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(8, 2), BurnCommands.FeatureCdMastering);

        Assert.True(BurnCommands.ParseGetConfigurationHasFeature(response, BurnCommands.FeatureCdMastering));
    }

    [Fact]
    public void ParseGetConfiguration_DifferentFeature_ReturnsFalse()
    {
        byte[] response = new byte[16];
        BinaryPrimitives.WriteUInt32BigEndian(response, 12);
        BinaryPrimitives.WriteUInt16BigEndian(response.AsSpan(8, 2), 0x0010); // wrong feature

        Assert.False(BurnCommands.ParseGetConfigurationHasFeature(response, BurnCommands.FeatureCdMastering));
    }

    [Fact]
    public void ParseGetConfiguration_TruncatedResponse_ReturnsFalse()
    {
        Assert.False(BurnCommands.ParseGetConfigurationHasFeature(new byte[4], BurnCommands.FeatureCdMastering));
    }

    // ── READ DISC INFORMATION ────────────────────────────────

    [Fact]
    public void BuildReadDiscInformation_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildReadDiscInformation(cdb);

        Assert.Equal(0x51, cdb[0]);
        Assert.Equal(0x00, cdb[7]); // alloc high
        Assert.Equal(0x22, cdb[8]); // alloc low = 34
    }

    [Fact]
    public void ParseReadDiscInformation_BlankDisc()
    {
        byte[] response = new byte[34];
        response[2] = 0x00; // blank, not erasable

        var info = BurnCommands.ParseReadDiscInformation(response);

        Assert.Equal(DiscStatus.Blank, info.Status);
        Assert.False(info.Erasable);
    }

    [Fact]
    public void ParseReadDiscInformation_ErasableAppendable()
    {
        byte[] response = new byte[34];
        response[2] = 0x11; // appendable (0x01) + erasable (0x10)
        response[3] = 1;    // first track
        response[6] = 3;    // last track

        var info = BurnCommands.ParseReadDiscInformation(response);

        Assert.Equal(DiscStatus.Appendable, info.Status);
        Assert.True(info.Erasable);
        Assert.Equal(1, info.FirstTrack);
        Assert.Equal(3, info.LastTrack);
    }

    [Fact]
    public void ParseReadDiscInformation_ResponseTooShort_Throws()
    {
        Assert.Throws<ArgumentException>(() => BurnCommands.ParseReadDiscInformation(new byte[10]));
    }

    // ── MODE SENSE / MODE SELECT ─────────────────────────────

    [Fact]
    public void BuildModeSense10_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildModeSense10(cdb, BurnCommands.WriteParametersPageCode, 0xFF);

        Assert.Equal(0x5A, cdb[0]);
        Assert.Equal(0x05, cdb[2]); // page code
        Assert.Equal(0x00, cdb[7]);
        Assert.Equal(0xFF, cdb[8]);
    }

    [Fact]
    public void BuildModeSelect10_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildModeSelect10(cdb, 60);

        Assert.Equal(0x55, cdb[0]);
        Assert.Equal(0x10, cdb[1]); // PF = 1
        Assert.Equal(0x00, cdb[7]);
        Assert.Equal(0x3C, cdb[8]); // 60
    }

    [Fact]
    public void BuildWriteParametersPage_DaoAudio_CorrectValues()
    {
        byte[] buffer = new byte[60];
        int len = BurnCommands.BuildWriteParametersPage(buffer, testWrite: false, bufferUnderrunProtection: false);

        Assert.Equal(60, len);
        Assert.Equal(0x05, buffer[8]);  // page code
        Assert.Equal(0x32, buffer[9]);  // page length
        Assert.Equal(0x02, buffer[10]); // write type = DAO
        Assert.Equal(0x00, buffer[11]); // track mode = audio
        Assert.Equal(0x00, buffer[12]); // data block type = raw 2352
    }

    [Fact]
    public void BuildWriteParametersPage_TestWrite_SetsBit()
    {
        byte[] buffer = new byte[60];
        BurnCommands.BuildWriteParametersPage(buffer, testWrite: true, bufferUnderrunProtection: false);

        Assert.Equal(0x12, buffer[10]); // DAO (0x02) | test write (0x10)
    }

    [Fact]
    public void BuildWriteParametersPage_Bufe_SetsBit()
    {
        byte[] buffer = new byte[60];
        BurnCommands.BuildWriteParametersPage(buffer, testWrite: false, bufferUnderrunProtection: true);

        Assert.Equal(0x42, buffer[10]); // DAO (0x02) | BUFE (0x40)
    }

    // ── SEND CUE SHEET ───────────────────────────────────────

    [Fact]
    public void BuildSendCueSheet_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildSendCueSheet(cdb, 32); // 4 entries × 8 bytes

        Assert.Equal(0x5D, cdb[0]);
        Assert.Equal(0x00, cdb[6]); // size high
        Assert.Equal(0x00, cdb[7]); // size mid
        Assert.Equal(0x20, cdb[8]); // size low = 32
    }

    [Fact]
    public void SerializeCueSheet_RoundTrips()
    {
        var entries = new List<CueSheetEntry>
        {
            CueSheetEntry.LeadIn(),
            CueSheetEntry.TrackPregap(1, 0, 0, 0),
            CueSheetEntry.TrackStart(1, 0, 2, 0),
            CueSheetEntry.LeadOut(5, 30, 0),
        };

        byte[] data = BurnCommands.SerializeCueSheet(entries);

        Assert.Equal(32, data.Length);
        Assert.Equal(CueSheetEntry.LeadInTrack, data[1]);   // entry 0 track = lead-in
        Assert.Equal(0x01, data[8 + 1]);                     // entry 1 track = 1
        Assert.Equal(0x00, data[8 + 2]);                     // entry 1 index = 0 (pregap)
        Assert.Equal(0x01, data[16 + 2]);                    // entry 2 index = 1 (start)
        Assert.Equal(CueSheetEntry.LeadOutTrack, data[24 + 1]); // entry 3 = lead-out
    }

    // ── WRITE (10) ───────────────────────────────────────────

    [Fact]
    public void BuildWrite10_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildWrite10(cdb, lba: 0x00001234, sectorCount: 25);

        Assert.Equal(0x2A, cdb[0]);
        Assert.Equal(0x00, cdb[2]);
        Assert.Equal(0x00, cdb[3]);
        Assert.Equal(0x12, cdb[4]);
        Assert.Equal(0x34, cdb[5]);
        Assert.Equal(0x00, cdb[7]);
        Assert.Equal(0x19, cdb[8]); // 25
    }

    // ── CLOSE TRACK/SESSION ──────────────────────────────────

    [Fact]
    public void BuildCloseSession_Immediate_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildCloseSession(cdb, immediate: true);

        Assert.Equal(0x5B, cdb[0]);
        Assert.Equal(0x01, cdb[1]); // immediate
        Assert.Equal(0x02, cdb[2]); // close session
    }

    [Fact]
    public void BuildCloseSession_Blocking_NoImmediateBit()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildCloseSession(cdb, immediate: false);

        Assert.Equal(0x00, cdb[1]);
    }

    // ── BLANK ────────────────────────────────────────────────

    [Fact]
    public void BuildBlank_Full_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[12];
        BurnCommands.BuildBlank(cdb, minimal: false, immediate: true);

        Assert.Equal(0xA1, cdb[0]);
        Assert.Equal(0x10, cdb[1]); // immediate only, full blank
    }

    [Fact]
    public void BuildBlank_Minimal_SetsBit()
    {
        Span<byte> cdb = stackalloc byte[12];
        BurnCommands.BuildBlank(cdb, minimal: true, immediate: false);

        Assert.Equal(0x01, cdb[1]); // minimal, not immediate
    }

    // ── SEND OPC ─────────────────────────────────────────────

    [Fact]
    public void BuildSendOpc_ExactByteLayout()
    {
        Span<byte> cdb = stackalloc byte[10];
        BurnCommands.BuildSendOpc(cdb);

        Assert.Equal(0x54, cdb[0]);
        Assert.Equal(0x01, cdb[1]); // DoOPC = 1
    }

    // ── MSF conversion ───────────────────────────────────────

    [Fact]
    public void LbaToMsf_Lba0_Returns00_02_00()
    {
        var (min, sec, frame) = BurnCommands.LbaToMsf(0);

        Assert.Equal(0, min);
        Assert.Equal(2, sec);
        Assert.Equal(0, frame);
    }

    [Fact]
    public void LbaToMsf_Lba150_Returns00_04_00()
    {
        // 150 + 150 = 300 frames = 4 seconds exactly
        var (min, sec, frame) = BurnCommands.LbaToMsf(150);

        Assert.Equal(0, min);
        Assert.Equal(4, sec);
        Assert.Equal(0, frame);
    }

    [Fact]
    public void LbaToMsf_OneMinute_Returns01_02_00()
    {
        // 1 minute of audio = 75 * 60 = 4500 sectors
        // 4500 + 150 = 4650 frames = 62 sec = 1:02:00
        var (min, sec, frame) = BurnCommands.LbaToMsf(4500);

        Assert.Equal(1, min);
        Assert.Equal(2, sec);
        Assert.Equal(0, frame);
    }

    [Theory]
    [InlineData(-150, 0, 0, 0)]  // LBA -150 = MSF 00:00:00 (start of lead-in)
    [InlineData(74, 0, 2, 74)]   // fractional frame
    public void LbaToMsf_EdgeCases(long lba, byte expectedMin, byte expectedSec, byte expectedFrame)
    {
        var (min, sec, frame) = BurnCommands.LbaToMsf(lba);

        Assert.Equal(expectedMin, min);
        Assert.Equal(expectedSec, sec);
        Assert.Equal(expectedFrame, frame);
    }
}
