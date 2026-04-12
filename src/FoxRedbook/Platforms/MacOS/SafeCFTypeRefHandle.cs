using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// <see cref="SafeHandle"/> wrapper around a CoreFoundation <c>CFTypeRef</c>.
/// Calls <c>CFRelease</c> on cleanup. Used for <c>DASessionRef</c>,
/// <c>DADiskRef</c>, <c>CFDictionaryRef</c>, and any other CF-derived type.
/// </summary>
/// <remarks>
/// <para>
/// CoreFoundation uses <c>NULL</c> as the "no reference" value, so the
/// invalid handle sentinel here is <see cref="IntPtr.Zero"/>.
/// </para>
/// <para>
/// <b>Do not</b> wrap COM-style interfaces (SCSITaskDeviceInterface,
/// SCSITaskInterface) in this handle type. Those have their own
/// <c>Release</c> method on their vtable and must be released via the
/// vtable's function pointer — mixing them with CFRelease corrupts the
/// reference count.
/// </para>
/// </remarks>
[SupportedOSPlatform("macos")]
internal sealed class SafeCFTypeRefHandle : SafeHandle
{
    public SafeCFTypeRefHandle()
        : base(invalidHandleValue: IntPtr.Zero, ownsHandle: true)
    {
    }

    public SafeCFTypeRefHandle(IntPtr cfRef)
        : base(invalidHandleValue: IntPtr.Zero, ownsHandle: true)
    {
        SetHandle(cfRef);
    }

    /// <inheritdoc />
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <inheritdoc />
    protected override bool ReleaseHandle()
    {
        DiskArbitrationNative.CFRelease(handle);
        return true;
    }
}
