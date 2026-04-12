using System.Runtime.InteropServices;
using FoxRedbook.Platforms.MacOS;

// Struct layouts and constants are portable data (static, independent of
// the host OS), so these tests are meaningful on any development box.
// Suppress CA1416 the same way SgIoHdrLayoutTests does.
#pragma warning disable CA1416

namespace FoxRedbook.Tests;

public sealed class IoKitLayoutTests
{
    // ── Vtable sizes ───────────────────────────────────────────

    [Fact]
    public void SCSITaskDeviceInterface_Size_Is88Bytes()
    {
        Assert.Equal(88, Marshal.SizeOf<SCSITaskDeviceInterface>());
        Assert.Equal(88, IoKitNative.ExpectedSCSITaskDeviceInterfaceSize);
    }

    [Fact]
    public void SCSITaskInterface_Size_Is192Bytes()
    {
        Assert.Equal(192, Marshal.SizeOf<SCSITaskInterface>());
        Assert.Equal(192, IoKitNative.ExpectedSCSITaskInterfaceSize);
    }

    [Fact]
    public void IOVirtualRange_Size_Is16Bytes()
    {
        Assert.Equal(16, Marshal.SizeOf<IOVirtualRange>());
    }

    // ── SCSITaskDeviceInterface field offsets ─────────────────

    [Fact]
    public void SCSITaskDeviceInterface_FieldOffsets_MatchScsiTaskLib()
    {
        // Verified by compiling a sizeof/offsetof test in plain C on x64 —
        // LP64 struct layout rules are identical between Darwin and Windows
        // x64 for function pointer structs of this shape.
        Assert.Equal(0,  (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.Reserved)));
        Assert.Equal(8,  (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.QueryInterface)));
        Assert.Equal(16, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.AddRef)));
        Assert.Equal(24, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.Release)));
        Assert.Equal(32, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.Version)));
        Assert.Equal(34, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.Revision)));
        // 4 bytes implicit padding at offsets 36..39
        Assert.Equal(40, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.IsExclusiveAccessAvailable)));
        Assert.Equal(48, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.AddCallbackDispatcherToRunLoop)));
        Assert.Equal(56, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.RemoveCallbackDispatcherFromRunLoop)));
        Assert.Equal(64, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.ObtainExclusiveAccess)));
        Assert.Equal(72, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.ReleaseExclusiveAccess)));
        Assert.Equal(80, (int)Marshal.OffsetOf<SCSITaskDeviceInterface>(nameof(SCSITaskDeviceInterface.CreateSCSITask)));
    }

    // ── SCSITaskInterface field offsets ───────────────────────

    [Fact]
    public void SCSITaskInterface_FieldOffsets_MatchScsiTaskLib()
    {
        Assert.Equal(0,   (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.Reserved)));
        Assert.Equal(8,   (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.QueryInterface)));
        Assert.Equal(16,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.AddRef)));
        Assert.Equal(24,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.Release)));
        Assert.Equal(32,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.Version)));
        Assert.Equal(34,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.Revision)));
        // 4 bytes implicit padding at offsets 36..39
        Assert.Equal(40,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.IsTaskActive)));
        Assert.Equal(48,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetTaskAttribute)));
        Assert.Equal(56,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetTaskAttribute)));
        Assert.Equal(64,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetCommandDescriptorBlock)));
        Assert.Equal(72,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetCommandDescriptorBlockSize)));
        Assert.Equal(80,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetCommandDescriptorBlock)));
        Assert.Equal(88,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetScatterGatherEntries)));
        Assert.Equal(96,  (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetTimeoutDuration)));
        Assert.Equal(104, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetTimeoutDuration)));
        Assert.Equal(112, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetTaskCompletionCallback)));
        Assert.Equal(120, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.ExecuteTaskAsync)));
        Assert.Equal(128, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.ExecuteTaskSync)));
        Assert.Equal(136, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.AbortTask)));
        Assert.Equal(144, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetSCSIServiceResponse)));
        Assert.Equal(152, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetTaskState)));
        Assert.Equal(160, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetTaskStatus)));
        Assert.Equal(168, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetRealizedDataTransferCount)));
        Assert.Equal(176, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.GetAutoSenseData)));
        Assert.Equal(184, (int)Marshal.OffsetOf<SCSITaskInterface>(nameof(SCSITaskInterface.SetAutoSenseDataBuffer)));
    }

    // ── IOKit GUIDs ───────────────────────────────────────────

    [Fact]
    public void IOKitGuids_AreExactly16BytesEach()
    {
        Assert.Equal(16, IoKitNative.kIOCFPlugInInterfaceID.Length);
        Assert.Equal(16, IoKitNative.kIOMMCDeviceUserClientTypeID.Length);
        Assert.Equal(16, IoKitNative.kIOMMCDeviceInterfaceID.Length);
    }

    [Fact]
    public void IOCFPlugInInterfaceID_MatchesHeaderValue()
    {
        // C244E858-109C-11D4-91D4-0050E4C6426F
        byte[] expected =
        [
            0xC2, 0x44, 0xE8, 0x58, 0x10, 0x9C, 0x11, 0xD4,
            0x91, 0xD4, 0x00, 0x50, 0xE4, 0xC6, 0x42, 0x6F,
        ];
        Assert.True(expected.AsSpan().SequenceEqual(IoKitNative.kIOCFPlugInInterfaceID));
    }

    [Fact]
    public void IOMMCDeviceUserClientTypeID_MatchesHeaderValue()
    {
        // 97ABCF2C-23CC-11D5-A0E8-003065704866
        byte[] expected =
        [
            0x97, 0xAB, 0xCF, 0x2C, 0x23, 0xCC, 0x11, 0xD5,
            0xA0, 0xE8, 0x00, 0x30, 0x65, 0x70, 0x48, 0x66,
        ];
        Assert.True(expected.AsSpan().SequenceEqual(IoKitNative.kIOMMCDeviceUserClientTypeID));
    }

    [Fact]
    public void IOMMCDeviceInterfaceID_MatchesHeaderValue()
    {
        // 1F651106-23CC-11D5-BBDB-003065704866
        byte[] expected =
        [
            0x1F, 0x65, 0x11, 0x06, 0x23, 0xCC, 0x11, 0xD5,
            0xBB, 0xDB, 0x00, 0x30, 0x65, 0x70, 0x48, 0x66,
        ];
        Assert.True(expected.AsSpan().SequenceEqual(IoKitNative.kIOMMCDeviceInterfaceID));
    }

    // ── IOReturn constants (verified against Apple's xnu source) ──

    [Fact]
    public void IOReturnConstants_MatchAppleXnuHeader()
    {
        // Values from https://github.com/apple-oss-distributions/xnu/blob/main/iokit/IOKit/IOReturn.h
        //   kIOReturnError           = iokit_common_err(0x2BC)
        //   kIOReturnExclusiveAccess = iokit_common_err(0x2C5)
        //   kIOReturnBusy            = iokit_common_err(0x2D5)
        //   kIOReturnNotReady        = iokit_common_err(0x2D8)
        //   kIOReturnNotPermitted    = iokit_common_err(0x2E2)
        //   kIOReturnNoMedia         = iokit_common_err(0x2E4)
        //
        // iokit_common_err(x) = (sys_iokit << 26) | (sub_iokit_common << 14) | x
        //                    = (0x38 << 26) | (0 << 14) | x
        //                    = 0xE0000000 | x
        Assert.Equal(0, IoKitNative.kIOReturnSuccess);
        Assert.Equal(unchecked((int)0xE00002BC), IoKitNative.kIOReturnError);
        Assert.Equal(unchecked((int)0xE00002C5), IoKitNative.kIOReturnExclusiveAccess);
        Assert.Equal(unchecked((int)0xE00002D5), IoKitNative.kIOReturnBusy);
        Assert.Equal(unchecked((int)0xE00002D8), IoKitNative.kIOReturnNotReady);
        Assert.Equal(unchecked((int)0xE00002E2), IoKitNative.kIOReturnNotPermitted);
        Assert.Equal(unchecked((int)0xE00002E4), IoKitNative.kIOReturnNoMedia);
    }

    // ── SCSITaskStatus / data direction constants ─────────────

    [Fact]
    public void SCSITaskStatus_Constants_AreCorrect()
    {
        Assert.Equal(0x00, IoKitNative.kSCSITaskStatus_GOOD);
        Assert.Equal(0x02, IoKitNative.kSCSITaskStatus_CHECK_CONDITION);
        Assert.Equal(0x04, IoKitNative.kSCSITaskStatus_CONDITION_MET);
        Assert.Equal(0x08, IoKitNative.kSCSITaskStatus_BUSY);
    }

    [Fact]
    public void SCSIDataTransfer_Direction_Constants_AreCorrect()
    {
        Assert.Equal(0, IoKitNative.kSCSIDataTransfer_NoDataTransfer);
        Assert.Equal(1, IoKitNative.kSCSIDataTransfer_FromInitiatorToTarget);
        Assert.Equal(2, IoKitNative.kSCSIDataTransfer_FromTargetToInitiator);
    }
}
