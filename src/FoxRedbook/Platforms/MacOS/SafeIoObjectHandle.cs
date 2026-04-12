using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace FoxRedbook.Platforms.MacOS;

/// <summary>
/// <see cref="SafeHandle"/> wrapper around an IOKit <c>io_object_t</c>
/// (which is really a <c>mach_port_t</c> masquerading as a handle).
/// Guarantees the object is released via <c>IOObjectRelease</c> on Dispose,
/// finalization, or exceptional unwind.
/// </summary>
/// <remarks>
/// IOKit objects count from 0, and a mach port of 0 is the invalid sentinel.
/// This matches <see cref="IntPtr.Zero"/> directly, so we use zero as the
/// invalid handle value here.
/// </remarks>
[SupportedOSPlatform("macos")]
internal sealed class SafeIoObjectHandle : SafeHandle
{
    public SafeIoObjectHandle()
        : base(invalidHandleValue: IntPtr.Zero, ownsHandle: true)
    {
    }

    public SafeIoObjectHandle(IntPtr ioObject)
        : base(invalidHandleValue: IntPtr.Zero, ownsHandle: true)
    {
        SetHandle(ioObject);
    }

    /// <inheritdoc />
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <inheritdoc />
    protected override bool ReleaseHandle()
    {
        return IoKitNative.IOObjectRelease(handle) == 0;
    }
}
