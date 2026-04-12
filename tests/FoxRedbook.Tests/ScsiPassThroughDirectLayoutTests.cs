using System.Runtime.InteropServices;
using FoxRedbook.Platforms.Windows;

// These tests verify the Windows SCSI_PASS_THROUGH_DIRECT struct layout
// and IOCTL constants against the Windows ABI. The struct's field offsets
// are portable data (static layout, independent of runtime OS), so the
// checks are meaningful on any development host, not just Windows.
#pragma warning disable CA1416

namespace FoxRedbook.Tests;

public sealed class ScsiPassThroughDirectLayoutTests
{
    // ── SCSI_PASS_THROUGH_DIRECT (56 bytes on x64) ────────────

    [Fact]
    public void ScsiPassThroughDirect_Size_Is56Bytes()
    {
        Assert.Equal(56, Marshal.SizeOf<ScsiPassThroughDirect>());
        Assert.Equal(56, Win32Native.ExpectedScsiPassThroughDirectSize);
    }

    [Fact]
    public void ScsiPassThroughDirect_FieldOffsets_MatchWindowsAbi()
    {
        // Verified against offsetof() output from a C program compiled
        // with clang --target=x86_64-pc-windows-msvc.
        Assert.Equal(0,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.Length)));
        Assert.Equal(2,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.ScsiStatus)));
        Assert.Equal(3,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.PathId)));
        Assert.Equal(4,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.TargetId)));
        Assert.Equal(5,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.Lun)));
        Assert.Equal(6,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.CdbLength)));
        Assert.Equal(7,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.SenseInfoLength)));
        Assert.Equal(8,  (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.DataIn)));
        Assert.Equal(12, (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.DataTransferLength)));
        Assert.Equal(16, (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.TimeOutValue)));
        Assert.Equal(24, (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.DataBuffer)));
        Assert.Equal(32, (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.SenseInfoOffset)));
        Assert.Equal(36, (int)Marshal.OffsetOf<ScsiPassThroughDirect>(nameof(ScsiPassThroughDirect.Cdb)));
    }

    // ── Wrapped buffer struct (80 bytes) ─────────────────────

    [Fact]
    public void ScsiPassThroughDirectWithBuffer_Size_Is96Bytes()
    {
        // 56 (Spt) + 4 (Filler) + 32 (SenseBuf) = 92, rounded up to 96 for
        // 8-byte struct alignment (required by the IntPtr DataBuffer field).
        Assert.Equal(96, Marshal.SizeOf<ScsiPassThroughDirectWithBuffer>());
        Assert.Equal(96, Win32Native.ExpectedScsiPassThroughDirectWithBufferSize);
    }

    [Fact]
    public void ScsiPassThroughDirectWithBuffer_FieldOffsets_MatchSptiSample()
    {
        Assert.Equal(0,  (int)Marshal.OffsetOf<ScsiPassThroughDirectWithBuffer>(nameof(ScsiPassThroughDirectWithBuffer.Spt)));
        Assert.Equal(56, (int)Marshal.OffsetOf<ScsiPassThroughDirectWithBuffer>(nameof(ScsiPassThroughDirectWithBuffer.Filler)));
        Assert.Equal(60, (int)Marshal.OffsetOf<ScsiPassThroughDirectWithBuffer>(nameof(ScsiPassThroughDirectWithBuffer.SenseBuf)));
    }

    // ── IOCTL constant ──────────────────────────────────────

    [Fact]
    public void IoctlScsiPassThroughDirect_Is0x4D014()
    {
        // CTL_CODE(IOCTL_SCSI_BASE=0x04, 0x0405, METHOD_BUFFERED=0, FILE_READ_ACCESS|FILE_WRITE_ACCESS=0x3)
        // = (0x04 << 16) | (0x3 << 14) | (0x0405 << 2) | 0
        // = 0x00040000 | 0x0000C000 | 0x00001014 | 0
        // = 0x0004D014
        Assert.Equal(0x0004D014u, Win32Native.IOCTL_SCSI_PASS_THROUGH_DIRECT);

        // Recompute via the CTL_CODE formula inside the test to lock the logic.
        const uint deviceType = 0x04;    // IOCTL_SCSI_BASE / FILE_DEVICE_CONTROLLER
        const uint function = 0x0405;
        const uint method = 0;           // METHOD_BUFFERED
        const uint access = 0x3;         // FILE_READ_ACCESS | FILE_WRITE_ACCESS
        uint computed = (deviceType << 16) | (access << 14) | (function << 2) | method;
        Assert.Equal(Win32Native.IOCTL_SCSI_PASS_THROUGH_DIRECT, computed);
    }

    // ── CreateFile access flags ─────────────────────────────

    [Fact]
    public void GenericReadWrite_HasCorrectBits()
    {
        Assert.Equal(0x80000000u, Win32Native.GENERIC_READ);
        Assert.Equal(0x40000000u, Win32Native.GENERIC_WRITE);
        Assert.Equal(0xC0000000u, Win32Native.GENERIC_READ | Win32Native.GENERIC_WRITE);
    }

    // ── Inline array basic sanity ───────────────────────────

    [Fact]
    public void CdbInlineArray_SupportsIndexerAssignment()
    {
        var spt = default(ScsiPassThroughDirect);
        spt.Cdb[0] = 0xBE;
        spt.Cdb[15] = 0xFF;

        Assert.Equal(0xBE, spt.Cdb[0]);
        Assert.Equal(0xFF, spt.Cdb[15]);
    }

    [Fact]
    public void SenseBufferInlineArray_HasThirtyTwoBytes()
    {
        var wrapper = default(ScsiPassThroughDirectWithBuffer);
        wrapper.SenseBuf[0] = 0x70;
        wrapper.SenseBuf[31] = 0xFF;

        Assert.Equal(0x70, wrapper.SenseBuf[0]);
        Assert.Equal(0xFF, wrapper.SenseBuf[31]);
    }
}
