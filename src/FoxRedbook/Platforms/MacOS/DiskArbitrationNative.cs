using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// DiskArbitration and CoreFoundation interop for unmounting an optical
/// disc before claiming exclusive SCSI access, and for the run-loop
/// helpers needed to block on DA callback completion.
/// </summary>
/// <remarks>
/// <para>
/// macOS auto-mounts audio CDs as a virtual filesystem through AudioCD.kext.
/// To issue raw SCSI commands via IOKit we must first unmount the disc via
/// DiskArbitration; otherwise <c>ObtainExclusiveAccess</c> fails with
/// <c>kIOReturnBusy</c>. The unmount is non-destructive — only the
/// filesystem layer detaches; the disc remains physically in the drive.
/// On Dispose we remount via <c>DADiskMount</c> so the user gets their
/// AIFF virtual file view back.
/// </para>
/// <para>
/// DiskArbitration callbacks dispatch through a CFRunLoop, which means we
/// schedule the session on the current thread's run loop and block briefly
/// with <c>CFRunLoopRunInMode</c> until the callback fires. Step 11's
/// watcher will need a dedicated background run-loop thread for persistent
/// hot-plug events; for step 8's synchronous lifecycle, inline blocking
/// matches the Linux and Windows backend semantics.
/// </para>
/// </remarks>
[SupportedOSPlatform("macos")]
internal static partial class DiskArbitrationNative
{
    private const string DiskArbitrationFramework =
        "/System/Library/Frameworks/DiskArbitration.framework/DiskArbitration";

    private const string CoreFoundationFramework =
        "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation";

    // libdispatch ships in libSystem.dylib on macOS — same library libc resolves to.
    private const string LibSystem = "/usr/lib/libSystem.dylib";

    /// <summary><c>DISPATCH_QUEUE_PRIORITY_DEFAULT</c> for use with <see cref="dispatch_get_global_queue"/>.</summary>
    internal const long DISPATCH_QUEUE_PRIORITY_DEFAULT = 0;

    /// <summary><c>kDADiskUnmountOptionDefault</c> = 0.</summary>
    internal const uint kDADiskUnmountOptionDefault = 0;

    /// <summary>
    /// Unmounts the whole disc, cascading to any mounted filesystem children.
    /// Required for hybrid audio/data CDs where an Apple_HFS or ISO9660 partition
    /// is mounted underneath the whole-disk identifier. The default option fails
    /// with opaque errors on these discs.
    /// </summary>
    internal const uint kDADiskUnmountOptionWhole = 0x00000001;

    /// <summary><c>kDADiskMountOptionDefault</c> = 0.</summary>
    internal const uint kDADiskMountOptionDefault = 0;

    /// <summary>
    /// Return value of <c>CFRunLoopRunInMode</c> when the timeout expires
    /// without a source being handled.
    /// </summary>
    internal const int kCFRunLoopRunTimedOut = 3;

    // ── DiskArbitration P/Invoke ───────────────────────────────

    /// <summary>
    /// Creates a new DiskArbitration session. Must be scheduled with a
    /// run loop via <see cref="DASessionScheduleWithRunLoop"/> before use.
    /// Released via <c>CFRelease</c>.
    /// </summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DASessionCreate")]
    internal static partial IntPtr DASessionCreate(IntPtr allocator);

    /// <summary>
    /// Routes DA callbacks to a libdispatch queue. Preferred over the legacy
    /// <c>DASessionScheduleWithRunLoop</c> path: callback delivery doesn't depend
    /// on the caller pumping a CFRunLoop, so it survives being invoked from
    /// inside hosts that own the main run loop (Avalonia, Cocoa apps, etc.).
    /// Pass <see cref="IntPtr.Zero"/> to detach.
    /// </summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DASessionSetDispatchQueue")]
    internal static partial void DASessionSetDispatchQueue(IntPtr session, IntPtr queue);

    /// <summary>
    /// Returns a global libdispatch concurrent queue. We use the default-priority
    /// global queue so DA callbacks fire on a worker thread libdispatch owns,
    /// independent of the caller's thread.
    /// </summary>
    [LibraryImport(LibSystem, EntryPoint = "dispatch_get_global_queue")]
    internal static partial IntPtr dispatch_get_global_queue(long identifier, ulong flags);

    /// <summary>
    /// Creates a DADiskRef for a given BSD name (e.g., "disk1"). The
    /// returned reference is +1 retained and must be released via <c>CFRelease</c>.
    /// </summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DADiskCreateFromBSDName", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial IntPtr DADiskCreateFromBSDName(IntPtr allocator, IntPtr session, string bsdName);

    /// <summary>
    /// Async unmount. The callback fires once the unmount completes or
    /// fails; the <c>dissenter</c> argument is NULL on success, non-NULL on
    /// failure. Caller is responsible for blocking on the run loop until
    /// the callback fires.
    /// </summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DADiskUnmount")]
    internal static partial void DADiskUnmount(
        IntPtr disk,
        uint options,
        IntPtr callback,
        IntPtr context);

    /// <summary>Async mount. Same callback semantics as <see cref="DADiskUnmount"/>.</summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DADiskMount")]
    internal static partial void DADiskMount(
        IntPtr disk,
        IntPtr path,
        uint options,
        IntPtr callback,
        IntPtr context);

    /// <summary>Gets a human-readable status string from a DA dissenter object, or NULL if none.</summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DADissenterGetStatusString")]
    internal static partial IntPtr DADissenterGetStatusString(IntPtr dissenter);

    /// <summary>
    /// Gets the numeric <c>DAReturn</c> status code from a DA dissenter object.
    /// Always present, unlike <see cref="DADissenterGetStatusString"/> which may
    /// return NULL. Common values: <c>0xF8DA0002</c> (busy),
    /// <c>0xF8DA0008</c> (not permitted), <c>0xF8DA0009</c> (not privileged).
    /// </summary>
    [LibraryImport(DiskArbitrationFramework, EntryPoint = "DADissenterGetStatus")]
    internal static partial uint DADissenterGetStatus(IntPtr dissenter);

    // ── CoreFoundation P/Invoke ────────────────────────────────

    /// <summary>
    /// Releases a CFTypeRef. Safe to call with <see cref="IntPtr.Zero"/>
    /// (CoreFoundation's <c>CFRelease</c> tolerates NULL).
    /// </summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFRelease")]
    internal static partial void CFRelease(IntPtr cf);

    /// <summary>Returns the current thread's run loop (borrowed reference — do NOT release).</summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFRunLoopGetCurrent")]
    internal static partial IntPtr CFRunLoopGetCurrent();

    /// <summary>
    /// Runs the current run loop in the given mode for up to <paramref name="seconds"/>.
    /// Returns one of the <c>kCFRunLoopRun*</c> constants.
    /// </summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFRunLoopRunInMode")]
    internal static partial int CFRunLoopRunInMode(
        IntPtr mode,
        double seconds,
        [MarshalAs(UnmanagedType.I1)] bool returnAfterSourceHandled);

    /// <summary>Stops the given run loop, causing any nested <c>CFRunLoopRunInMode</c> to return.</summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFRunLoopStop")]
    internal static partial void CFRunLoopStop(IntPtr runLoop);

    /// <summary>
    /// Creates a <c>CFUUIDRef</c> from raw 16-byte UUID data. The returned
    /// object is +1 retained and must be released via <see cref="CFRelease"/>.
    /// CoreFoundation may return an existing singleton with a bumped ref count.
    /// </summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFUUIDCreateFromUUIDBytes")]
    internal static partial IntPtr CFUUIDCreateFromUUIDBytes(IntPtr allocator, CFUUIDBytes bytes);

    /// <summary><c>kCFStringEncodingUTF8</c> = 0x08000100.</summary>
    internal const uint kCFStringEncodingUTF8 = 0x08000100;

    /// <summary>
    /// Copies the contents of a <c>CFStringRef</c> into a caller-supplied
    /// buffer as a null-terminated C string in the given encoding.
    /// Returns <see langword="true"/> on success.
    /// </summary>
    [LibraryImport(CoreFoundationFramework, EntryPoint = "CFStringGetCString")]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static partial bool CFStringGetCString(
        IntPtr theString, IntPtr buffer, long bufferSize, uint encoding);

    /// <summary>
    /// Global string constant <c>kCFRunLoopDefaultMode</c>. Exposed as a
    /// <see cref="IntPtr"/> because dereferencing CoreFoundation string
    /// constants requires <see cref="NativeLibrary.GetExport"/>.
    /// </summary>
    internal static IntPtr GetCFRunLoopDefaultMode()
    {
        IntPtr handle = NativeLibrary.Load(CoreFoundationFramework);
        IntPtr symbolAddress = NativeLibrary.GetExport(handle, "kCFRunLoopDefaultMode");
        // The symbol is a pointer TO a CFStringRef, so we need one deref.
        return Marshal.ReadIntPtr(symbolAddress);
    }
}

/// <summary>
/// Managed delegate matching the C callback signature
/// <c>void (*)(DADiskRef disk, DADissenterRef dissenter, void *context)</c>.
/// Used for both <c>DADiskUnmount</c> and <c>DADiskMount</c> completion.
/// </summary>
/// <remarks>
/// The delegate must be kept alive for the duration of the unmanaged call.
/// <c>GC.KeepAlive(callback)</c> after <c>CFRunLoopRunInMode</c> returns is
/// the documented pattern — "local variable in scope" is not equivalent to
/// "GC sees the delegate as reachable across unmanaged boundaries" under
/// release JIT optimization.
/// </remarks>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void DADiskCallback(IntPtr disk, IntPtr dissenter, IntPtr context);

/// <summary>
/// Managed mirror of Apple's <c>CFUUIDBytes</c> struct — 16 raw bytes that
/// define a UUID. Passed by value to <see cref="DiskArbitrationNative.CFUUIDCreateFromUUIDBytes"/>
/// to create a proper <c>CFUUIDRef</c> object.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CFUUIDBytes
{
    public byte Byte0, Byte1, Byte2, Byte3;
    public byte Byte4, Byte5, Byte6, Byte7;
    public byte Byte8, Byte9, Byte10, Byte11;
    public byte Byte12, Byte13, Byte14, Byte15;

    /// <summary>Creates a <see cref="CFUUIDBytes"/> from a 16-byte span.</summary>
    public static CFUUIDBytes FromSpan(ReadOnlySpan<byte> uuid)
    {
        return new CFUUIDBytes
        {
            Byte0  = uuid[0],  Byte1  = uuid[1],  Byte2  = uuid[2],  Byte3  = uuid[3],
            Byte4  = uuid[4],  Byte5  = uuid[5],  Byte6  = uuid[6],  Byte7  = uuid[7],
            Byte8  = uuid[8],  Byte9  = uuid[9],  Byte10 = uuid[10], Byte11 = uuid[11],
            Byte12 = uuid[12], Byte13 = uuid[13], Byte14 = uuid[14], Byte15 = uuid[15],
        };
    }
}
