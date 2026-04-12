namespace FoxRedbook;

/// <summary>
/// Parsed response from a SCSI INQUIRY command (opcode 0x12).
/// Contains the drive identification strings needed for drive-offset database lookups.
/// </summary>
/// <remarks>
/// Field lengths per SPC-4: Vendor 8 bytes, Product 16 bytes, Revision 4 bytes.
/// Backends should trim trailing spaces from these ASCII fields before constructing this record.
/// </remarks>
public readonly record struct DriveInquiry
{
    /// <summary>
    /// Drive manufacturer identification (up to 8 ASCII characters, trimmed).
    /// </summary>
    public required string Vendor { get; init; }

    /// <summary>
    /// Drive product identification (up to 16 ASCII characters, trimmed).
    /// </summary>
    public required string Product { get; init; }

    /// <summary>
    /// Firmware revision level (up to 4 ASCII characters, trimmed).
    /// </summary>
    public required string Revision { get; init; }

    /// <summary>
    /// Canonical lookup key for the AccurateRip drive offset database, formatted as
    /// <c>"{Vendor} - {Product}"</c>. This is the single authoritative source for
    /// building offset database queries — do not format the key elsewhere.
    /// </summary>
    public string OffsetDatabaseKey => $"{Vendor} - {Product}";
}
