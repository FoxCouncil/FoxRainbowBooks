using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// IOKit interop for the macOS SCSI passthrough backend. Contains the
/// P/Invoke declarations, the SCSITaskDeviceInterface and SCSITaskInterface
/// vtable struct definitions, CFUUIDBytes GUIDs, and IOReturn error constants.
/// </summary>
/// <remarks>
/// <para>
/// The two vtable structs are 88 bytes and 192 bytes respectively on x64.
/// Field offsets were verified by compiling a <c>sizeof</c>/<c>offsetof</c>
/// test with clang — the research that preceded this file had incorrect
/// offsets for both structs because it forgot the 4-byte implicit padding
/// between the <c>revision</c> field and the first function pointer (a
/// function pointer needs 8-byte alignment, and <c>revision</c> ends at
/// offset 36, so 4 bytes of padding push the next field to offset 40).
/// A startup assertion in this class locks both struct sizes at type load.
/// </para>
/// <para>
/// The IOReturn constants below were transcribed directly from Apple's
/// open-source <c>xnu/iokit/IOKit/IOReturn.h</c>. Two of the values the
/// preliminary research suggested were wrong (<c>kIOReturnNoMedia</c> and
/// <c>kIOReturnNotReady</c>), caught by cross-checking against Apple's
/// authoritative source before any code shipped.
/// </para>
/// </remarks>
[SupportedOSPlatform("macos")]
internal static partial class IoKitNative
{
#pragma warning disable CA1065 // Deliberate: vtable layout mismatches against the Apple ABI
                               // are fatal and must fail loudly at type load, not silently
                               // invoke random function pointers on every SCSI command.
    static IoKitNative()
    {
        int deviceIfaceSize = Marshal.SizeOf<SCSITaskDeviceInterface>();

        if (deviceIfaceSize != ExpectedSCSITaskDeviceInterfaceSize)
        {
            throw new InvalidOperationException(
                $"SCSITaskDeviceInterface layout mismatch: expected {ExpectedSCSITaskDeviceInterfaceSize} bytes, got {deviceIfaceSize}. " +
                "The managed struct no longer matches Apple's <IOKit/scsi/SCSITaskLib.h> ABI. " +
                "Most common cause: missing padding between the uint16 version/revision fields " +
                "and the first function pointer (need 4 bytes of padding for 8-byte alignment).");
        }

        int taskIfaceSize = Marshal.SizeOf<SCSITaskInterface>();

        if (taskIfaceSize != ExpectedSCSITaskInterfaceSize)
        {
            throw new InvalidOperationException(
                $"SCSITaskInterface layout mismatch: expected {ExpectedSCSITaskInterfaceSize} bytes, got {taskIfaceSize}.");
        }
    }
#pragma warning restore CA1065

    internal const int ExpectedSCSITaskDeviceInterfaceSize = 88;
    internal const int ExpectedSCSITaskInterfaceSize = 192;

    // ── IOKit GUIDs (CFUUIDBytes, 16 bytes big-endian byte order) ──

    /// <summary>
    /// <c>kIOCFPlugInInterfaceID</c> = <c>C244E858-109C-11D4-91D4-0050E4C6426F</c>
    /// from <c>IOKit/IOCFPlugIn.h</c>.
    /// </summary>
    internal static ReadOnlySpan<byte> kIOCFPlugInInterfaceID =>
    [
        0xC2, 0x44, 0xE8, 0x58, 0x10, 0x9C, 0x11, 0xD4,
        0x91, 0xD4, 0x00, 0x50, 0xE4, 0xC6, 0x42, 0x6F,
    ];

    /// <summary>
    /// <c>kIOMMCDeviceUserClientTypeID</c> = <c>97ABCF2C-23CC-11D5-A0E8-003065704866</c>
    /// from <c>IOKit/scsi/SCSITaskLib.h</c>. This is the plug-in type UUID
    /// registered in the <c>IOCFPlugInTypes</c> dictionary on
    /// <c>IOCompactDiscServices</c> / <c>IODVDServices</c> / <c>IOBDServices</c>
    /// nodes, and must be passed to <see cref="IOCreatePlugInInterfaceForService"/>
    /// as the <c>pluginType</c> parameter. The resulting plug-in supports
    /// <c>QueryInterface</c> for <see cref="kIOMMCDeviceInterfaceID"/>.
    /// </summary>
    internal static ReadOnlySpan<byte> kIOMMCDeviceUserClientTypeID =>
    [
        0x97, 0xAB, 0xCF, 0x2C, 0x23, 0xCC, 0x11, 0xD5,
        0xA0, 0xE8, 0x00, 0x30, 0x65, 0x70, 0x48, 0x66,
    ];

    /// <summary>
    /// <c>kIOMMCDeviceInterfaceID</c> = <c>1F651106-23CC-11D5-BBDB-003065704866</c>
    /// from <c>IOKit/scsi/SCSITaskLib.h</c>. Passed to <c>QueryInterface</c>
    /// on the plug-in loaded via <see cref="kIOMMCDeviceUserClientTypeID"/> to
    /// obtain an <c>MMCDeviceInterface</c> vtable. The MMC interface exposes a
    /// <c>GetSCSITaskDeviceInterface</c> method (vtable offset 136) that returns
    /// a <see cref="SCSITaskDeviceInterface"/> pointer for raw SCSI passthrough.
    /// </summary>
    internal static ReadOnlySpan<byte> kIOMMCDeviceInterfaceID =>
    [
        0x1F, 0x65, 0x11, 0x06, 0x23, 0xCC, 0x11, 0xD5,
        0xBB, 0xDB, 0x00, 0x30, 0x65, 0x70, 0x48, 0x66,
    ];

    // ── IOReturn error codes (from xnu/iokit/IOKit/IOReturn.h) ──────
    // Each kIOReturn* constant = iokit_common_err(subcode)
    //                          = (sys_iokit | sub_iokit_common | subcode)
    //                          = (0x38 << 26) | (0 << 14) | subcode
    //                          = 0xE0000000 | subcode
    //
    // VERIFIED against Apple's authoritative xnu source at
    // github.com/apple-oss-distributions/xnu (not from research-agent claims).

    internal const int kIOReturnSuccess = 0;

    /// <summary>General I/O error (subcode 0x2BC).</summary>
    internal const int kIOReturnError = unchecked((int)0xE00002BC);

    /// <summary>Another process holds exclusive access to the device (subcode 0x2C5).</summary>
    internal const int kIOReturnExclusiveAccess = unchecked((int)0xE00002C5);

    /// <summary>Device is busy — often because the volume is still mounted (subcode 0x2D5).</summary>
    internal const int kIOReturnBusy = unchecked((int)0xE00002D5);

    /// <summary>Device not ready (spinning up, loading, etc.) (subcode 0x2D8).</summary>
    internal const int kIOReturnNotReady = unchecked((int)0xE00002D8);

    /// <summary>Permission denied (subcode 0x2E2).</summary>
    internal const int kIOReturnNotPermitted = unchecked((int)0xE00002E2);

    /// <summary>No media in drive (subcode 0x2E4).</summary>
    internal const int kIOReturnNoMedia = unchecked((int)0xE00002E4);

    // ── SCSITaskStatus values (from IOKit/scsi/SCSITask.h) ──────────

    internal const byte kSCSITaskStatus_GOOD = 0x00;
    internal const byte kSCSITaskStatus_CHECK_CONDITION = 0x02;
    internal const byte kSCSITaskStatus_CONDITION_MET = 0x04;
    internal const byte kSCSITaskStatus_BUSY = 0x08;

    // ── SCSI data direction (from IOKit/scsi/SCSITask.h) ────────────

    internal const byte kSCSIDataTransfer_NoDataTransfer = 0;
    internal const byte kSCSIDataTransfer_FromInitiatorToTarget = 1;
    internal const byte kSCSIDataTransfer_FromTargetToInitiator = 2;

    /// <summary>
    /// <c>kIOMasterPortDefault</c> from <c>IOKit/IOKitLib.h</c>. Value is
    /// 0 on all modern macOS versions.
    /// </summary>
    internal const uint kIOMasterPortDefault = 0;

    // ── IOKit framework P/Invokes ───────────────────────────────────

    private const string IOKitFramework = "/System/Library/Frameworks/IOKit.framework/IOKit";

    /// <summary>
    /// Builds a CFMutableDictionaryRef matching a specific BSD device name
    /// (e.g., "disk1"). The returned dictionary is consumed by
    /// <see cref="IOServiceGetMatchingServices"/> — do NOT CFRelease it separately.
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOBSDNameMatching", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial IntPtr IOBSDNameMatching(uint masterPort, uint options, string bsdName);

    /// <summary>
    /// Returns an iterator over IOKit services matching the given dictionary.
    /// Consumes the <paramref name="matching"/> dictionary regardless of success.
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOServiceGetMatchingServices")]
    internal static partial int IOServiceGetMatchingServices(uint masterPort, IntPtr matching, out IntPtr iterator);

    /// <summary>
    /// Returns the next io_object_t from an iterator, or 0 when exhausted.
    /// Each non-zero return is +1 refcounted and must be released via
    /// <see cref="IOObjectRelease"/>.
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOIteratorNext")]
    internal static partial IntPtr IOIteratorNext(IntPtr iterator);

    /// <summary>Releases an io_object_t (mach port). Returns <c>KERN_SUCCESS</c> (0) on success.</summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOObjectRelease")]
    internal static partial int IOObjectRelease(IntPtr ioObject);

    /// <summary>
    /// Returns the parent of an IORegistry entry in the specified plane.
    /// The parent is +1 retained and must be released via <see cref="IOObjectRelease"/>.
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IORegistryEntryGetParentEntry", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int IORegistryEntryGetParentEntry(
        IntPtr entry, string plane, out IntPtr parent);

    /// <summary>
    /// Returns <see langword="true"/> if the given IOKit object conforms to
    /// the named IOKit class (e.g., <c>IOSCSIPeripheralDeviceType05</c>).
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOObjectConformsTo", StringMarshalling = StringMarshalling.Utf8)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static partial bool IOObjectConformsTo(IntPtr ioObject, string className);

    /// <summary>
    /// Fills <paramref name="className"/> with the C-string class name of an
    /// IOKit object (e.g. <c>"IOCDMedia"</c>). The buffer must be at least
    /// 128 bytes per IOKit/IOTypes.h <c>io_name_t</c>.
    /// </summary>
    [LibraryImport(IOKitFramework, EntryPoint = "IOObjectGetClass")]
    internal static unsafe partial int IOObjectGetClass(IntPtr ioObject, byte* className);

    /// <summary>IOService plane name for IORegistry traversal.</summary>
    internal const string kIOServicePlane = "IOService";

    /// <summary>
    /// IOKit class names for the optical-disc services layer. These are
    /// parallel siblings (not an inheritance chain): a CD-only drive registers
    /// <c>IOCompactDiscServices</c>, a DVD drive registers <c>IODVDServices</c>,
    /// and a BD drive registers <c>IOBDServices</c>. The services node (not
    /// the peripheral device nub above it) is the one that exposes the
    /// <c>IOCFPlugInTypes</c> dictionary needed by
    /// <see cref="IOCreatePlugInInterfaceForService"/>.
    /// </summary>
    internal static readonly string[] OpticalServicesClasses =
    [
        "IOBDServices",
        "IODVDServices",
        "IOCompactDiscServices",
    ];

    /// <summary>
    /// Loads the IOKit plug-in interface for a given service and returns a
    /// pointer to a <c>IOCFPlugInInterface**</c> (an interface pointer to a
    /// pointer to a vtable — COM convention). The score parameter is
    /// typically discarded.
    /// </summary>
    /// <remarks>
    /// Both <paramref name="pluginType"/> and <paramref name="interfaceType"/>
    /// are <c>CFUUIDRef</c> objects — opaque CoreFoundation pointers created via
    /// <c>CFUUIDCreateFromUUIDBytes</c>. Passing raw <c>byte*</c> pointers
    /// instead crashes in IOKit's error-logging path on macOS 26: the os_log
    /// formatter calls <c>objc_msgSend</c> on the "CFUUIDRef" to get its
    /// description, which dereferences the raw UUID bytes as an isa pointer.
    /// </remarks>
    [LibraryImport(IOKitFramework, EntryPoint = "IOCreatePlugInInterfaceForService")]
    internal static partial int IOCreatePlugInInterfaceForService(
        IntPtr service,
        IntPtr pluginType,
        IntPtr interfaceType,
        out IntPtr plugInInterface,
        out int score);
}

/// <summary>
/// Managed mirror of Apple's <c>SCSITaskDeviceInterface</c> vtable. Every
/// field is a function pointer (8 bytes on x64) except <c>Version</c> and
/// <c>Revision</c> (2 bytes each), followed by 4 bytes of implicit padding
/// inserted by the runtime to bring the next pointer to 8-byte alignment.
/// Total size: 88 bytes on x64.
/// </summary>
/// <remarks>
/// <para>
/// <b>COM-style vtable conventions</b>: in C, calling a method through this
/// vtable looks like <c>(*iface)-&gt;CreateSCSITask(iface)</c> where
/// <c>iface</c> is a pointer to a pointer to the vtable. The outer pointer
/// is the "this" / self value passed as the first argument.
/// </para>
/// <para>
/// <b>Do not confuse this with IOObjectRelease or CFRelease</b>: the
/// <c>Release</c> field at offset 24 is the vtable's own reference-counting
/// method, reached by invoking the function pointer with the outer
/// interface pointer as the self argument. Neither IOKit's <c>IOObjectRelease</c>
/// nor CoreFoundation's <c>CFRelease</c> applies to COM-style interfaces —
/// they have their own lifetime managed by the vtable's <c>Release</c> slot.
/// See <see cref="MacScsiTaskInterface"/> for the wrapper that handles
/// the dereference-twice pattern correctly.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct SCSITaskDeviceInterface
{
    public IntPtr Reserved;
    public IntPtr QueryInterface;
    public IntPtr AddRef;
    public IntPtr Release;
    public ushort Version;
    public ushort Revision;
    // 4 bytes implicit padding to align IsExclusiveAccessAvailable to 8 bytes
    public IntPtr IsExclusiveAccessAvailable;
    public IntPtr AddCallbackDispatcherToRunLoop;
    public IntPtr RemoveCallbackDispatcherFromRunLoop;
    public IntPtr ObtainExclusiveAccess;
    public IntPtr ReleaseExclusiveAccess;
    public IntPtr CreateSCSITask;
}

/// <summary>
/// Managed mirror of Apple's <c>SCSITaskInterface</c> vtable. Returned by
/// calling <see cref="SCSITaskDeviceInterface.CreateSCSITask"/>. Total size:
/// 192 bytes on x64 — 21 function pointers after the IUnknown header and
/// version/revision fields.
/// </summary>
/// <remarks>
/// Only 6 methods are actually called by FoxRedbook: <c>SetCommandDescriptorBlock</c>,
/// <c>SetScatterGatherEntries</c>, <c>SetTimeoutDuration</c>, <c>ExecuteTaskSync</c>,
/// <c>GetAutoSenseData</c>, and <c>Release</c>. The remaining slots are
/// declared as <see cref="IntPtr"/> so the field offsets stay correct but
/// we never invoke them.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct SCSITaskInterface
{
    public IntPtr Reserved;
    public IntPtr QueryInterface;
    public IntPtr AddRef;
    public IntPtr Release;
    public ushort Version;
    public ushort Revision;
    // 4 bytes implicit padding
    public IntPtr IsTaskActive;
    public IntPtr SetTaskAttribute;
    public IntPtr GetTaskAttribute;
    public IntPtr SetCommandDescriptorBlock;
    public IntPtr GetCommandDescriptorBlockSize;
    public IntPtr GetCommandDescriptorBlock;
    public IntPtr SetScatterGatherEntries;
    public IntPtr SetTimeoutDuration;
    public IntPtr GetTimeoutDuration;
    public IntPtr SetTaskCompletionCallback;
    public IntPtr ExecuteTaskAsync;
    public IntPtr ExecuteTaskSync;
    public IntPtr AbortTask;
    public IntPtr GetSCSIServiceResponse;
    public IntPtr GetTaskState;
    public IntPtr GetTaskStatus;
    public IntPtr GetRealizedDataTransferCount;
    public IntPtr GetAutoSenseData;
    public IntPtr SetAutoSenseDataBuffer;
}

/// <summary>
/// Scatter-gather list entry for <c>SetScatterGatherEntries</c>. Each
/// entry describes one contiguous buffer: address and length. We always
/// pass a single entry representing the caller's data buffer.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct IOVirtualRange
{
    public IntPtr Address;
    public IntPtr Length;
}
