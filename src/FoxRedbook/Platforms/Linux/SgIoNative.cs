using System.Runtime.InteropServices;
using System.Runtime.Versioning;

// Restrict native library search paths for P/Invoke declarations in this
// assembly. On Linux, libc is loaded via the system dynamic linker and
// this attribute is largely symbolic, but the analyzer rule CA5392 wants
// it present to prevent DLL hijacking on Windows (not applicable here,
// but the rule applies uniformly across the assembly). AssemblyDirectory
// is explicitly disallowed by CA5393 as a DLL-hijack vector.
[assembly: System.Runtime.InteropServices.DefaultDllImportSearchPaths(
    System.Runtime.InteropServices.DllImportSearchPath.System32)]

namespace FoxRedbook.Platforms.Linux;

/// <summary>
/// Linux SCSI generic (sg) driver interop. Contains the P/Invoke
/// declarations, the <c>sg_io_hdr</c> struct, and the kernel ABI
/// constants required to issue SCSI passthrough commands via
/// <c>ioctl(fd, SG_IO, &amp;hdr)</c>.
/// </summary>
/// <remarks>
/// All declarations here mirror <c>&lt;scsi/sg.h&gt;</c> and
/// <c>&lt;fcntl.h&gt;</c> from the Linux UAPI headers. Field offsets
/// and the 88-byte total struct size were verified by compiling a
/// <c>printf(&quot;%zu&quot;, sizeof(sg_io_hdr_t))</c> test with clang
/// on x86_64 — a startup assertion in the static constructor catches
/// any future layout drift before the first SCSI command is issued.
/// </remarks>
[SupportedOSPlatform("linux")]
internal static partial class SgIoNative
{
#pragma warning disable CA1065 // Deliberate: a struct layout mismatch against the kernel ABI
                               // is fatal and must fail loudly at type load, not silently
                               // produce EFAULT or corrupted data in every ioctl.
    static SgIoNative()
    {
        // Guard against future refactors that change field types or
        // introduce padding mistakes. If the struct doesn't match the
        // kernel's expected 88 bytes on x86_64, every SG_IO ioctl will
        // either EFAULT or return garbage — this assertion fails fast
        // at type load time with a clear message instead.
        int actualSize = Marshal.SizeOf<SgIoHdr>();

        if (actualSize != ExpectedSgIoHdrSize)
        {
            throw new InvalidOperationException(
                $"SgIoHdr struct layout mismatch: expected {ExpectedSgIoHdrSize} bytes, got {actualSize}. " +
                "The managed struct no longer matches the Linux kernel's sg_io_hdr_t ABI. " +
                "Check field order, types, and padding — this must match <scsi/sg.h> exactly.");
        }
    }
#pragma warning restore CA1065

    /// <summary>Expected size of <see cref="SgIoHdr"/> on x86_64 Linux (verified via clang).</summary>
    internal const int ExpectedSgIoHdrSize = 88;

    // ── File open flags (from <fcntl.h>) ────────────────────────

    internal const int O_RDONLY = 0x0000;
    internal const int O_NONBLOCK = 0x0800;

    // ── SG driver constants (from <scsi/sg.h>) ──────────────────

    /// <summary>Required value for <see cref="SgIoHdr.InterfaceId"/>. ASCII 'S' = 0x53.</summary>
    internal const int SG_INTERFACE_ID_ORIG = 'S';

    /// <summary>No data transfer.</summary>
    internal const int SG_DXFER_NONE = -1;

    /// <summary>Transfer direction: host → device.</summary>
    internal const int SG_DXFER_TO_DEV = -2;

    /// <summary>Transfer direction: device → host.</summary>
    internal const int SG_DXFER_FROM_DEV = -3;

    /// <summary>Use direct I/O (requires page-aligned buffers — we don't use this).</summary>
    internal const uint SG_FLAG_DIRECT_IO = 0x01;

    /// <summary>
    /// The SG_IO ioctl request code. Computed as <c>_IOWR('S', 0x85, sizeof(sg_io_hdr_t))</c>
    /// by the kernel header — the macro packs direction bits (read+write),
    /// magic type ('S'), number (0x85), and struct size into a 32-bit value.
    /// This is not an arbitrary magic number; it is the kernel-encoded ioctl
    /// identifier defined in <c>&lt;scsi/sg.h&gt;</c>.
    /// </summary>
    internal const nuint SG_IO = 0x2285;

    // ── libc P/Invoke (LibraryImport, source-generated, AOT-safe) ──

    /// <summary>
    /// <c>open(const char *pathname, int flags)</c> — returns an fd or -1 on error.
    /// </summary>
    [LibraryImport("libc", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    internal static partial int Open(string pathname, int flags);

    /// <summary>
    /// <c>close(int fd)</c> — returns 0 on success, -1 on error.
    /// </summary>
    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    internal static partial int Close(int fd);

    /// <summary>
    /// <c>ioctl(int fd, unsigned long request, void *arg)</c> — returns 0 on
    /// success, -1 on error (sets <c>errno</c>; check via <see cref="Marshal.GetLastPInvokeError"/>).
    /// The third argument is a pointer to the ioctl-specific structure; for
    /// SG_IO this is a pinned <see cref="SgIoHdr"/>.
    /// </summary>
    [LibraryImport("libc", EntryPoint = "ioctl", SetLastError = true)]
    internal static unsafe partial int Ioctl(int fd, nuint request, void* arg);
}

/// <summary>
/// Managed mirror of the Linux kernel's <c>sg_io_hdr_t</c> struct. Byte-for-byte
/// compatible with <c>&lt;scsi/sg.h&gt;</c> on x86_64 Linux — 88 bytes total.
/// </summary>
/// <remarks>
/// <para>
/// Do NOT add <c>Pack = N</c> to the StructLayout attribute. The default
/// natural alignment (what C does on x86_64) is what we need here. Adding
/// a pack directive would force every field to the specified alignment
/// and break the layout — specifically, <c>cmd_len</c> at offset 8 (a
/// 1-byte field after two 4-byte ints) would be padded to 8-byte alignment
/// if Pack = 8 were set.
/// </para>
/// <para>
/// The 4-byte implicit padding between <see cref="PackId"/> (offset 48) and
/// <see cref="UsrPtr"/> (offset 56) exists because <see cref="IntPtr"/> on
/// x86_64 requires 8-byte alignment. The final 4 bytes of trailing padding
/// bring the struct total from 84 to 88 (a multiple of 8, the struct's
/// natural alignment as determined by its pointer fields). These paddings
/// are inserted by the runtime's sequential layout engine automatically —
/// we don't need explicit filler fields.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct SgIoHdr
{
    /// <summary>[i] Must be set to <see cref="SgIoNative.SG_INTERFACE_ID_ORIG"/> ('S').</summary>
    public int InterfaceId;

    /// <summary>[i] SG_DXFER_* direction constant.</summary>
    public int DxferDirection;

    /// <summary>[i] SCSI CDB length (typically 6, 10, 12, or 16 bytes).</summary>
    public byte CmdLen;

    /// <summary>[i] Maximum sense buffer length the kernel may write.</summary>
    public byte MxSbLen;

    /// <summary>[i] Number of scatter-gather vectors (0 = single buffer).</summary>
    public ushort IovecCount;

    /// <summary>[i] Number of bytes to transfer.</summary>
    public uint DxferLen;

    /// <summary>[i,*io] Pointer to data buffer for SCSI payload.</summary>
    public IntPtr Dxferp;

    /// <summary>[i,*i] Pointer to the SCSI command descriptor block.</summary>
    public IntPtr Cmdp;

    /// <summary>[i,*o] Pointer to caller-provided sense buffer.</summary>
    public IntPtr Sbp;

    /// <summary>[i] Command timeout in milliseconds (0 = driver default).</summary>
    public uint Timeout;

    /// <summary>[i] SG_FLAG_* combination (0 = indirect I/O, no alignment requirement).</summary>
    public uint Flags;

    /// <summary>[i→o] Caller-assigned packet identifier, passed through unchanged.</summary>
    public int PackId;

    // 4 bytes of implicit padding inserted by the runtime to align UsrPtr
    // to 8 bytes on 64-bit targets. Do not add an explicit filler field —
    // the sequential layout engine handles this automatically.

    /// <summary>[i→o] Caller-defined user pointer, passed through unchanged.</summary>
    public IntPtr UsrPtr;

    /// <summary>[o] SCSI status byte (0 = GOOD).</summary>
    public byte Status;

    /// <summary>[o] Masked SCSI status.</summary>
    public byte MaskedStatus;

    /// <summary>[o] SCSI message status.</summary>
    public byte MsgStatus;

    /// <summary>[o] Number of bytes actually written to the sense buffer
    /// (0 = no sense data, command succeeded).</summary>
    public byte SbLenWr;

    /// <summary>[o] Host adapter / transport error code (0 = no transport error).</summary>
    public ushort HostStatus;

    /// <summary>[o] Kernel SCSI driver error code (0 = no driver error).</summary>
    public ushort DriverStatus;

    /// <summary>[o] Data residual — bytes that were NOT transferred.</summary>
    public int Resid;

    /// <summary>[o] Command execution time in milliseconds.</summary>
    public uint Duration;

    /// <summary>[o] Auxiliary information flags.</summary>
    public uint Info;

    // 4 bytes of trailing padding to bring the struct size from 84 to 88.
    // Runtime inserts this automatically via LayoutKind.Sequential.
}
