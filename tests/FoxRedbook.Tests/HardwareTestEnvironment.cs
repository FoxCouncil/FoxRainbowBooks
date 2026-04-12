using System.Runtime.InteropServices;

namespace FoxRedbook.Tests;

/// <summary>
/// Shared helper for the hardware test suite. Resolves the optical-drive
/// device path from the <c>FOXREDBOOK_TEST_DEVICE</c> environment variable
/// first, then falls back to a platform-appropriate default. Exists so
/// tests don't each hand-roll env-var parsing and so the default device
/// path is documented in exactly one place.
/// </summary>
internal static class HardwareTestEnvironment
{
    public const string EnvironmentVariableName = "FOXREDBOOK_TEST_DEVICE";

    /// <summary>
    /// Platform default device path. Linux drives usually live at
    /// <c>/dev/sr0</c>, Windows exposes the first optical drive as
    /// <c>D:</c>, and macOS uses BSD names like <c>disk1</c>.
    /// </summary>
    public static string DefaultDevicePath
    {
        get
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return "/dev/sr0";
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return "D:";
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return "disk1";
            }

            // Unknown OS — there is no sensible default. Callers will
            // skip their tests when GetDevicePath returns null.
            return string.Empty;
        }
    }

    /// <summary>
    /// Returns the device path to use for hardware tests, or
    /// <see langword="null"/> if no drive is available on this host.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Resolution order:
    /// <list type="number">
    ///   <item><c>FOXREDBOOK_TEST_DEVICE</c> environment variable (if set and non-empty)</item>
    ///   <item>Platform default (<see cref="DefaultDevicePath"/>) if it appears to exist</item>
    /// </list>
    /// </para>
    /// <para>
    /// Existence checks: on Linux and macOS we check whether the device
    /// file exists on disk. On Windows, drive-letter existence is harder
    /// to check without side effects, so we return the default and let
    /// the test's <c>OpticalDrive.Open</c> call fail — the test then
    /// catches and skips. Env var values are always returned as-is
    /// without probing, trusting the user who set them.
    /// </para>
    /// </remarks>
    public static string? GetDevicePath()
    {
        string? fromEnv = Environment.GetEnvironmentVariable(EnvironmentVariableName);

        if (!string.IsNullOrWhiteSpace(fromEnv))
        {
            return fromEnv.Trim();
        }

        string defaultPath = DefaultDevicePath;

        if (string.IsNullOrEmpty(defaultPath))
        {
            return null;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return File.Exists(defaultPath) ? defaultPath : null;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            // BSD name like "disk1" — check the corresponding /dev/ path.
            return File.Exists("/dev/" + defaultPath) ? defaultPath : null;
        }

        // Windows: return the default and let the Open call fail-and-skip.
        return defaultPath;
    }
}
