using System.Runtime.InteropServices;

namespace FoxRedbook.Tests;

/// <summary>
/// Shared helper for the hardware test suite. Resolves the optical-drive
/// device path by scanning the system for CD-ROM hardware, with an
/// environment variable override for non-standard configurations.
/// </summary>
internal static class HardwareTestEnvironment
{
    public const string EnvironmentVariableName = "FOXREDBOOK_TEST_DEVICE";

    /// <summary>
    /// Returns the device path to use for hardware tests, or
    /// <see langword="null"/> if no optical drive is found on this host.
    /// </summary>
    /// <remarks>
    /// Resolution order:
    /// <list type="number">
    ///   <item><c>FOXREDBOOK_TEST_DEVICE</c> environment variable (if set and non-empty)</item>
    ///   <item>Auto-detect: scan the system for optical drives and return the first one found</item>
    /// </list>
    /// </remarks>
    public static string? GetDevicePath()
    {
        string? fromEnv = Environment.GetEnvironmentVariable(EnvironmentVariableName);

        if (!string.IsNullOrWhiteSpace(fromEnv))
        {
            return fromEnv.Trim();
        }

        return DetectDrive();
    }

    private static string? DetectDrive()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return DetectWindowsDrive();
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return DetectLinuxDrive();
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return DetectMacDrive();
        }

        return null;
    }

    private static string? DetectWindowsDrive()
    {
        // Enumerate all CD-ROM drives. Prefer one with media ready —
        // and specifically one with an audio CD — over an empty drive.
        try
        {
            string? firstEmpty = null;

            foreach (var drive in DriveInfo.GetDrives())
            {
                if (drive.DriveType != DriveType.CDRom)
                {
                    continue;
                }

                string path = drive.Name.TrimEnd('\\');

                if (!drive.IsReady)
                {
                    firstEmpty ??= path;
                    continue;
                }

                // Drive has media. Try to open it and check for audio tracks.
                try
                {
                    using var optical = OpticalDrive.Open(path);
                    var toc = optical.ReadTocAsync().GetAwaiter().GetResult();

                    if (toc.Tracks.Any(t => t.Type == TrackType.Audio))
                    {
                        return path;
                    }
                }
                catch (OpticalDriveException)
                {
                    // Drive is ready but we can't read the TOC (data-only
                    // disc, permissions, etc.). Still better than an empty
                    // drive — fall through and prefer it over firstEmpty.
                    firstEmpty ??= path;
                }
            }

            return firstEmpty;
        }
        catch (IOException)
        {
            // DriveInfo can throw on restricted environments.
        }
        catch (UnauthorizedAccessException)
        {
            // Locked-down hosts may deny drive enumeration.
        }

        return null;
    }

    private static string? DetectLinuxDrive()
    {
        // Linux optical drives are /dev/sr0, /dev/sr1, etc.
        for (int i = 0; i < 8; i++)
        {
            string path = $"/dev/sr{i}";

            if (File.Exists(path))
            {
                return path;
            }
        }

        return null;
    }

    private static string? DetectMacDrive()
    {
        // macOS optical drives appear as /dev/diskN. Scan for any that
        // exist beyond the typical system disk (disk0). In practice,
        // external USB optical drives get disk1, disk2, etc.
        for (int i = 1; i < 10; i++)
        {
            string bsdName = $"disk{i}";

            if (File.Exists($"/dev/{bsdName}"))
            {
                return bsdName;
            }
        }

        return null;
    }
}
