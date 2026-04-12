namespace FoxRedbook;

/// <summary>
/// Base exception for all optical drive errors raised by FoxRedbook backends.
/// Catch this type to handle any drive-related failure uniformly.
/// </summary>
public class OpticalDriveException : Exception
{
    /// <inheritdoc />
    public OpticalDriveException()
    {
    }

    /// <inheritdoc />
    public OpticalDriveException(string? message) : base(message)
    {
    }

    /// <inheritdoc />
    public OpticalDriveException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Thrown when an operation requires a disc but the drive tray is empty or the disc
/// is not readable (e.g., blank media, unfinalized disc).
/// </summary>
public class MediaNotPresentException : OpticalDriveException
{
    /// <inheritdoc />
    public MediaNotPresentException()
    {
    }

    /// <inheritdoc />
    public MediaNotPresentException(string? message) : base(message)
    {
    }

    /// <inheritdoc />
    public MediaNotPresentException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Thrown when the drive exists but is not in a state to accept commands — typically
/// because it is still spinning up, loading a disc, or locked by another process.
/// </summary>
/// <remarks>
/// Maps to SCSI sense key 0x02 (NOT READY). Callers may retry after a delay.
/// </remarks>
public class DriveNotReadyException : OpticalDriveException
{
    /// <inheritdoc />
    public DriveNotReadyException()
    {
    }

    /// <inheritdoc />
    public DriveNotReadyException(string? message) : base(message)
    {
    }

    /// <inheritdoc />
    public DriveNotReadyException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
