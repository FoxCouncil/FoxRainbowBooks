namespace FoxRedbook;

/// <summary>
/// Low-level SCSI command transport. Sends a CDB to a device and transfers
/// data in the specified direction. Platform backends implement this using
/// their native passthrough mechanism; higher-level interfaces like
/// <see cref="IOpticalDrive"/> compose over it.
/// </summary>
/// <remarks>
/// This is the shared foundation that both FoxRedbook (reading) and
/// FoxOrangebook (writing) build on. Consumers who only need to rip
/// audio should use <see cref="IOpticalDrive"/> instead.
/// </remarks>
public interface IScsiTransport : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Executes a SCSI command and transfers data between host and device.
    /// </summary>
    /// <param name="cdb">The Command Descriptor Block to send.</param>
    /// <param name="buffer">
    /// Data buffer for the transfer. For <see cref="ScsiDirection.In"/>,
    /// receives data from the device. For <see cref="ScsiDirection.Out"/>,
    /// contains data to send to the device. For <see cref="ScsiDirection.None"/>,
    /// ignored (pass an empty span).
    /// </param>
    /// <param name="direction">Data transfer direction.</param>
    /// <exception cref="OpticalDriveException">
    /// The command failed — the platform error or SCSI sense data is
    /// included in the exception.
    /// </exception>
    void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction);

    /// <summary>
    /// Drive identification from the SCSI INQUIRY response.
    /// </summary>
    DriveInquiry Inquiry { get; }
}
