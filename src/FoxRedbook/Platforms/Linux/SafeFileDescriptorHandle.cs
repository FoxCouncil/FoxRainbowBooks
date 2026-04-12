using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace FoxRedbook.Platforms.Linux;

/// <summary>
/// <see cref="SafeHandle"/> wrapper around a Linux file descriptor.
/// Guarantees the fd is closed via <c>close(2)</c> on Dispose, finalization,
/// or exceptional unwind — we cannot leak fds in long-running processes.
/// </summary>
/// <remarks>
/// Linux fd 0 is a valid fd (stdin), so the invalid-handle sentinel is -1,
/// NOT zero. This is why we pass <c>new IntPtr(-1)</c> to the base
/// constructor instead of using <see cref="IntPtr.Zero"/>.
/// </remarks>
[SupportedOSPlatform("linux")]
internal sealed class SafeFileDescriptorHandle : SafeHandle
{
    public SafeFileDescriptorHandle()
        : base(invalidHandleValue: new IntPtr(-1), ownsHandle: true)
    {
    }

    public SafeFileDescriptorHandle(int fd)
        : base(invalidHandleValue: new IntPtr(-1), ownsHandle: true)
    {
        SetHandle(new IntPtr(fd));
    }

    /// <inheritdoc />
    public override bool IsInvalid => handle == new IntPtr(-1);

    /// <summary>
    /// Numeric file descriptor for passing to <c>ioctl(2)</c> and other
    /// libc functions that take an <c>int fd</c> argument.
    /// </summary>
    internal int FileDescriptor => (int)handle;

    /// <inheritdoc />
    protected override bool ReleaseHandle()
    {
        // close(2) returns 0 on success, -1 on error. We don't surface
        // close errors (the caller is already done with the fd); return
        // true unless the close syscall itself crashed.
        return SgIoNative.Close((int)handle) == 0;
    }
}
