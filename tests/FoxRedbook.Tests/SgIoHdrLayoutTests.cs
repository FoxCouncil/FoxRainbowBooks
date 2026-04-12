using System.Runtime.InteropServices;
using FoxRedbook.Platforms.Linux;

// These tests verify the struct layout and constants at compile/load time
// against the Linux kernel ABI. The actual sg_io_hdr_t struct is portable
// data — its field offsets are the same regardless of which OS runs the
// test — so the layout checks are meaningful on any development host,
// not just Linux. Suppress CA1416 for this file.
#pragma warning disable CA1416

namespace FoxRedbook.Tests;

/// <summary>
/// Verifies the managed <see cref="SgIoHdr"/> struct matches the Linux
/// kernel's <c>sg_io_hdr_t</c> ABI on x86_64. The verification is done
/// three ways: (1) a static type-load assertion inside
/// <see cref="SgIoNative"/>'s static constructor that fails fast if
/// <see cref="Marshal.SizeOf"/> disagrees with the expected 88 bytes,
/// (2) the unit tests here that exercise both the constant and the
/// actual layout, and (3) a field-by-field offset check using
/// <see cref="Marshal.OffsetOf"/>.
/// </summary>
public sealed class SgIoHdrLayoutTests
{
    [Fact]
    public void StructSize_Is88Bytes()
    {
        Assert.Equal(88, Marshal.SizeOf<SgIoHdr>());
        Assert.Equal(88, SgIoNative.ExpectedSgIoHdrSize);
    }

    [Fact]
    public void FieldOffsets_MatchKernelAbi()
    {
        // Verified against offsetof() output from a C program compiled
        // with clang -O2 on x86_64 Linux. These values come from the
        // Linux UAPI header <scsi/sg.h>.
        Assert.Equal(0,  (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.InterfaceId)));
        Assert.Equal(4,  (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.DxferDirection)));
        Assert.Equal(8,  (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.CmdLen)));
        Assert.Equal(9,  (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.MxSbLen)));
        Assert.Equal(10, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.IovecCount)));
        Assert.Equal(12, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.DxferLen)));
        Assert.Equal(16, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Dxferp)));
        Assert.Equal(24, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Cmdp)));
        Assert.Equal(32, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Sbp)));
        Assert.Equal(40, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Timeout)));
        Assert.Equal(44, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Flags)));
        Assert.Equal(48, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.PackId)));
        Assert.Equal(56, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.UsrPtr)));
        Assert.Equal(64, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Status)));
        Assert.Equal(65, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.MaskedStatus)));
        Assert.Equal(66, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.MsgStatus)));
        Assert.Equal(67, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.SbLenWr)));
        Assert.Equal(68, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.HostStatus)));
        Assert.Equal(70, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.DriverStatus)));
        Assert.Equal(72, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Resid)));
        Assert.Equal(76, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Duration)));
        Assert.Equal(80, (int)Marshal.OffsetOf<SgIoHdr>(nameof(SgIoHdr.Info)));
    }

    [Fact]
    public void SgIoConstant_Matches0x2285()
    {
        // _IOWR('S', 0x85, sizeof(sg_io_hdr_t)) — kernel-encoded ioctl number.
        Assert.Equal((nuint)0x2285, SgIoNative.SG_IO);
    }

    [Fact]
    public void InterfaceId_IsAsciiS()
    {
        Assert.Equal('S', SgIoNative.SG_INTERFACE_ID_ORIG);
    }
}
