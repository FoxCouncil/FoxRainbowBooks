namespace FoxRedbook;

/// <summary>
/// Data transfer direction for a SCSI command, abstracting the
/// platform-specific constants (<c>SCSI_IOCTL_DATA_IN</c> on Windows,
/// <c>SG_DXFER_FROM_DEV</c> on Linux, etc.) behind a portable enum.
/// </summary>
public enum ScsiDirection
{
    /// <summary>No data transfer (e.g., TEST UNIT READY).</summary>
    None = 0,

    /// <summary>Device to host (reads).</summary>
    In = 1,

    /// <summary>Host to device (writes).</summary>
    Out = 2,
}
