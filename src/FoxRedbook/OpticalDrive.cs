using System.Runtime.InteropServices;

namespace FoxRedbook;

/// <summary>
/// Static factory for opening a platform-appropriate <see cref="IOpticalDrive"/>
/// by device path. Dispatches at runtime via <see cref="RuntimeInformation"/>
/// so the AOT linker can statically prove which backend is reachable and trim
/// the others when publishing a single-RID executable.
/// </summary>
/// <remarks>
/// Consumers who know their target platform may instantiate a specific backend
/// directly (e.g., <c>new LinuxOpticalDrive("/dev/sr0")</c>) to skip the
/// dispatch. The factory exists for cross-platform consumer code.
/// </remarks>
public static class OpticalDrive
{
    /// <summary>
    /// Opens an optical drive at the given platform-specific device path and
    /// returns an <see cref="IOpticalDrive"/> backed by the current platform's
    /// SCSI passthrough mechanism.
    /// </summary>
    /// <param name="devicePath">
    /// Platform-specific device identifier:
    /// <list type="bullet">
    ///   <item>Linux: <c>/dev/sr0</c>, <c>/dev/cdrom</c>, or similar</item>
    ///   <item>Windows: drive letter (<c>D:</c>), device-namespace drive letter (<c>\\.\D:</c>), or CD-ROM device object (<c>\\.\CdRom0</c>)</item>
    ///   <item>macOS: BSD name (<c>disk1</c>) or full device path (<c>/dev/disk1</c>)</item>
    /// </list>
    /// </param>
    /// <returns>A drive instance ready for TOC and sector reads.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="devicePath"/> is null.</exception>
    /// <exception cref="PlatformNotSupportedException">
    /// Called on a platform that is not Linux, Windows, or macOS.
    /// </exception>
    /// <exception cref="OpticalDriveException">
    /// The device could not be opened.
    /// </exception>
    public static IOpticalDrive Open(string devicePath)
    {
        ArgumentNullException.ThrowIfNull(devicePath);

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return new Platforms.Linux.LinuxOpticalDrive(devicePath);
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return new Platforms.Windows.WindowsOpticalDrive(devicePath);
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return new Platforms.MacOS.MacOpticalDrive(devicePath);
        }

        throw new PlatformNotSupportedException(
            $"No FoxRedbook backend available for {RuntimeInformation.OSDescription}.");
    }
}
