using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace FoxRedbook.Platforms.Windows;

/// <summary>
/// Win32 interop for the Windows SCSI passthrough backend. Contains the
/// P/Invoke declarations for <c>CreateFileW</c> and <c>DeviceIoControl</c>,
/// the <c>SCSI_PASS_THROUGH_DIRECT</c> struct (and its wrapped-buffer
/// counterpart), and the constants required to issue <c>IOCTL_SCSI_PASS_THROUGH_DIRECT</c>
/// to an optical drive device handle.
/// </summary>
/// <remarks>
/// <para>
/// <c>SCSI_PASS_THROUGH_DIRECT</c> field offsets and the 56-byte struct
/// size on x64 were verified by compiling an <c>offsetof</c>/<c>sizeof</c>
/// test with <c>clang --target=x86_64-pc-windows-msvc</c>. A static
/// constructor assertion in this class locks the layout at type load time
/// so any future refactor that breaks the ABI fails loudly before the
/// first ioctl is issued.
/// </para>
/// <para>
/// The device handle is a BCL <see cref="SafeFileHandle"/> obtained from
/// <see cref="CreateFileW"/> — no custom <c>SafeHandle</c> subclass is
/// needed on Windows because the runtime already has one tailored for
/// Win32 file handles.
/// </para>
/// </remarks>
[SupportedOSPlatform("windows")]
internal static partial class Win32Native
{
#pragma warning disable CA1065 // Deliberate: a struct layout mismatch against the Windows ABI
                               // is fatal and must fail loudly at type load, not silently
                               // EFAULT or return corrupt sense data on every DeviceIoControl.
    static Win32Native()
    {
        int sptSize = Marshal.SizeOf<ScsiPassThroughDirect>();

        if (sptSize != ExpectedScsiPassThroughDirectSize)
        {
            throw new InvalidOperationException(
                $"ScsiPassThroughDirect struct layout mismatch: expected {ExpectedScsiPassThroughDirectSize} bytes, got {sptSize}. " +
                "The managed struct no longer matches the Windows SCSI_PASS_THROUGH_DIRECT ABI from <ntddscsi.h>. " +
                "Check field order, types (DataBuffer must be IntPtr not uint on 64-bit), and padding.");
        }

        int wrapperSize = Marshal.SizeOf<ScsiPassThroughDirectWithBuffer>();

        if (wrapperSize != ExpectedScsiPassThroughDirectWithBufferSize)
        {
            throw new InvalidOperationException(
                $"ScsiPassThroughDirectWithBuffer struct layout mismatch: expected {ExpectedScsiPassThroughDirectWithBufferSize} bytes, got {wrapperSize}.");
        }
    }
#pragma warning restore CA1065

    /// <summary>Expected size of <see cref="ScsiPassThroughDirect"/> on x64 Windows (verified via clang).</summary>
    internal const int ExpectedScsiPassThroughDirectSize = 56;

    /// <summary>
    /// Expected size of <see cref="ScsiPassThroughDirectWithBuffer"/> on x64 Windows.
    /// 56 (Spt) + 4 (Filler) + 32 (SenseBuf) = 92, rounded up to 96 for the
    /// struct's 8-byte alignment (required by the <c>DataBuffer</c> pointer field).
    /// </summary>
    internal const int ExpectedScsiPassThroughDirectWithBufferSize = 96;

    // ── CreateFile constants (from <WinBase.h> / <FileAPI.h>) ──

    internal const uint GENERIC_READ = 0x80000000;
    internal const uint GENERIC_WRITE = 0x40000000;
    internal const uint FILE_SHARE_READ = 0x00000001;
    internal const uint FILE_SHARE_WRITE = 0x00000002;
    internal const uint OPEN_EXISTING = 3;

    // ── SCSI passthrough constants ────────────────────────────

    /// <summary>
    /// <c>IOCTL_SCSI_PASS_THROUGH_DIRECT</c> from <c>&lt;ntddscsi.h&gt;</c>.
    /// Computed as <c>CTL_CODE(IOCTL_SCSI_BASE=0x04, 0x0405, METHOD_BUFFERED=0,
    /// FILE_READ_ACCESS|FILE_WRITE_ACCESS=0x3)</c> which expands to
    /// <c>(0x04 &lt;&lt; 16) | (0x3 &lt;&lt; 14) | (0x0405 &lt;&lt; 2) | 0 = 0x4D014</c>.
    /// Not an arbitrary magic number — it is the kernel-encoded ioctl
    /// identifier and the value is fixed by the Windows ABI.
    /// </summary>
    internal const uint IOCTL_SCSI_PASS_THROUGH_DIRECT = 0x0004D014;

    /// <summary>SCSI_PASS_THROUGH_DIRECT.DataIn value: no data transfer.</summary>
    internal const byte SCSI_IOCTL_DATA_OUT = 0;

    /// <summary>SCSI_PASS_THROUGH_DIRECT.DataIn value: device → host.</summary>
    internal const byte SCSI_IOCTL_DATA_IN = 1;

    /// <summary>SCSI_PASS_THROUGH_DIRECT.DataIn value: no data transfer.</summary>
    internal const byte SCSI_IOCTL_DATA_UNSPECIFIED = 2;

    // ── Well-known Win32 error codes ──────────────────────────

    internal const int ERROR_NOT_READY = 21;
    internal const int ERROR_ACCESS_DENIED = 5;

    // ── libc/kernel32 P/Invoke (LibraryImport, AOT-safe) ──────

    /// <summary>
    /// <c>CreateFileW</c> — opens a device or file handle.
    /// </summary>
    [LibraryImport("kernel32.dll", EntryPoint = "CreateFileW", StringMarshalling = StringMarshalling.Utf16, SetLastError = true)]
    internal static partial SafeFileHandle CreateFile(
        string lpFileName,
        uint dwDesiredAccess,
        uint dwShareMode,
        nint lpSecurityAttributes,
        uint dwCreationDisposition,
        uint dwFlagsAndAttributes,
        nint hTemplateFile);

    /// <summary>
    /// <c>DeviceIoControl</c> — issues an ioctl to a device handle.
    /// Returns non-zero on success, zero on failure; call
    /// <see cref="Marshal.GetLastPInvokeError"/> for the Win32 error code.
    /// </summary>
    [LibraryImport("kernel32.dll", EntryPoint = "DeviceIoControl", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    internal static unsafe partial bool DeviceIoControl(
        SafeFileHandle hDevice,
        uint dwIoControlCode,
        void* lpInBuffer,
        uint nInBufferSize,
        void* lpOutBuffer,
        uint nOutBufferSize,
        out uint lpBytesReturned,
        nint lpOverlapped);
}

/// <summary>
/// Managed mirror of <c>SCSI_PASS_THROUGH_DIRECT</c> from
/// <c>&lt;ntddscsi.h&gt;</c>. Byte-for-byte compatible with the Windows
/// x64 ABI — 56 bytes total.
/// </summary>
/// <remarks>
/// <para>
/// Do NOT add <c>Pack = N</c> to the StructLayout attribute. Natural
/// alignment (what the C compiler does) is required: <see cref="DataBuffer"/>
/// must land at offset 24 with 4 bytes of implicit padding after
/// <see cref="TimeOutValue"/>. Forcing Pack = 1 would collapse the
/// padding and put DataBuffer at offset 20, producing a 52-byte struct
/// that DeviceIoControl rejects with ERROR_INVALID_PARAMETER.
/// </para>
/// <para>
/// <see cref="DataBuffer"/> is declared as <see cref="nint"/> (IntPtr) —
/// 8 bytes on x64, 4 bytes on x86. This is the field most likely to be
/// wrong if anyone copies a struct definition from a 32-bit-era sample
/// where <c>DataBuffer</c> is <c>ULONG</c> (always 4 bytes). Don't.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct ScsiPassThroughDirect
{
    /// <summary>Size of the struct in bytes. Caller must set to <c>sizeof(SCSI_PASS_THROUGH_DIRECT)</c>.</summary>
    public ushort Length;

    /// <summary>[o] SCSI status returned by the device (0 = GOOD, 0x02 = CHECK CONDITION).</summary>
    public byte ScsiStatus;

    /// <summary>SCSI path ID. 0 for a directly-opened device.</summary>
    public byte PathId;

    /// <summary>SCSI target ID. 0 for a directly-opened device.</summary>
    public byte TargetId;

    /// <summary>SCSI logical unit number. 0 for a directly-opened device.</summary>
    public byte Lun;

    /// <summary>Length of the CDB in bytes (typically 6, 10, 12, or 16).</summary>
    public byte CdbLength;

    /// <summary>Length of the sense buffer (the <c>SenseInfoOffset</c>-pointed region).</summary>
    public byte SenseInfoLength;

    /// <summary>Data transfer direction: 0 = to device, 1 = from device, 2 = unspecified.</summary>
    public byte DataIn;

    /// <summary>Number of bytes to transfer in or out.</summary>
    public uint DataTransferLength;

    /// <summary>Command timeout in seconds.</summary>
    public uint TimeOutValue;

    /// <summary>Pointer to the caller's data buffer (pinned during the call).</summary>
    public nint DataBuffer;

    /// <summary>
    /// Offset from the start of this struct to the sense buffer. In the
    /// wrapped layout used by this library, the sense buffer lives at
    /// offset 60 (end of struct + 4-byte filler).
    /// </summary>
    public uint SenseInfoOffset;

    /// <summary>
    /// Command Descriptor Block, up to 16 bytes. Use the indexer to set
    /// bytes individually — the field is an inline array so <c>Cdb[0]</c>
    /// through <c>Cdb[15]</c> behave like array element access.
    /// </summary>
    public CdbBuffer Cdb;
}

/// <summary>
/// 16-byte inline array for the <c>SCSI_PASS_THROUGH_DIRECT.Cdb</c> field.
/// C# 12 inline arrays are AOT-compatible and compose cleanly with
/// sequential struct layout — verified by a dotnet publish /p:PublishAot=true
/// test before adoption.
/// </summary>
[System.Runtime.CompilerServices.InlineArray(16)]
internal struct CdbBuffer
{
    private byte _element0;
}

/// <summary>
/// Wrapper struct that places a <see cref="ScsiPassThroughDirect"/>
/// followed by a 4-byte filler and a 32-byte sense buffer. Matches
/// Microsoft's reference SPTI sample layout byte-for-byte so driver
/// behavior is bug-compatible with 25+ years of Windows SCSI tools.
/// </summary>
/// <remarks>
/// The <see cref="Filler"/> field is cargo-culted from Microsoft's SPTI
/// sample where its comment claims "realign buffer to double word boundary."
/// The claim is misleading — a 4-byte filler at offset 56 brings the sense
/// buffer to offset 60 (4-byte aligned, but not 8-byte aligned), so no
/// meaningful alignment happens. The real reason the field exists is
/// historical convention: the Microsoft sample has shipped with it since
/// the NT 4 DDK, every SPTI-derived tool in the wild copies the pattern,
/// and optical drive drivers have been tested against exactly this
/// layout for decades. We mirror it verbatim rather than "cleaning up"
/// an apparently-dead field that might turn out to matter on some
/// obscure driver.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct ScsiPassThroughDirectWithBuffer
{
    /// <summary>The SCSI passthrough request header (56 bytes).</summary>
    public ScsiPassThroughDirect Spt;

    /// <summary>
    /// Filler field from the Microsoft SPTI reference sample. Do NOT remove
    /// without reading the type's remarks first — see above.
    /// </summary>
    public uint Filler;

    /// <summary>
    /// Sense buffer (32 bytes). The kernel writes SCSI sense data here
    /// when <see cref="ScsiPassThroughDirect.ScsiStatus"/> indicates
    /// CHECK CONDITION (0x02). 32 bytes matches the Microsoft SPTI
    /// sample's <c>SPT_SENSE_LENGTH</c> — longer than the 18-byte
    /// fixed-format minimum to accommodate drivers that extend the
    /// sense data.
    /// </summary>
    public SenseBuffer SenseBuf;
}

/// <summary>
/// 32-byte inline array for the wrapper's sense data region.
/// </summary>
[System.Runtime.CompilerServices.InlineArray(32)]
internal struct SenseBuffer
{
    private byte _element0;
}
