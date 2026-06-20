namespace FoxOrangebook.FileSystem;

/// <summary>
/// Constants for the ISO 9660 / ECMA-119 data-disc filesystem and the
/// 2048-byte logical sectors used by data CDs (Yellow Book Mode 1) and DVDs.
/// </summary>
internal static class IsoConstants
{
    /// <summary>
    /// Logical sector size for data discs, in bytes. CD Mode 1 carries 2048
    /// user bytes inside each 2352-byte physical sector; DVD sectors are 2048
    /// bytes natively. ISO 9660 always addresses 2048-byte logical blocks.
    /// </summary>
    public const int LogicalSectorSize = 2048;

    /// <summary>
    /// Number of sectors in the System Area at the start of the volume
    /// (16 sectors = 32 KiB). Reserved for boot data; zero-filled otherwise.
    /// </summary>
    public const int SystemAreaSectors = 16;

    /// <summary>LBA of the first volume descriptor (immediately after the System Area).</summary>
    public const int VolumeDescriptorSetStart = SystemAreaSectors;

    // ── Volume descriptor types (byte 0 of each descriptor) ──────

    public const byte VolumeDescriptorTypeBootRecord = 0x00;
    public const byte VolumeDescriptorTypePrimary = 0x01;
    public const byte VolumeDescriptorTypeSupplementary = 0x02;
    public const byte VolumeDescriptorTypePartition = 0x03;
    public const byte VolumeDescriptorTypeTerminator = 0xFF;

    /// <summary>Standard identifier "CD001" present in every ISO 9660 volume descriptor.</summary>
    public static ReadOnlySpan<byte> StandardIdentifier => "CD001"u8;

    /// <summary>Volume descriptor version, always 1.</summary>
    public const byte VolumeDescriptorVersion = 0x01;

    // ── Directory record file flags (ISO 9660 §9.1.6) ────────────

    public const byte FileFlagHidden = 0x01;
    public const byte FileFlagDirectory = 0x02;
    public const byte FileFlagAssociated = 0x04;
    public const byte FileFlagRecord = 0x08;
    public const byte FileFlagProtection = 0x10;
    public const byte FileFlagMultiExtent = 0x80;

    /// <summary>Self ("." ) directory record identifier — a single 0x00 byte.</summary>
    public const byte SelfDirectoryIdentifier = 0x00;

    /// <summary>Parent (".." ) directory record identifier — a single 0x01 byte.</summary>
    public const byte ParentDirectoryIdentifier = 0x01;

    /// <summary>Version suffix appended to ISO 9660 file identifiers.</summary>
    public static ReadOnlySpan<byte> FileVersionSuffix => ";1"u8;

    /// <summary>
    /// Joliet UCS-2 escape sequence for the supplementary volume descriptor
    /// (Level 3, ESC 25 2F 45). Selects UCS-2 BE for the secondary hierarchy.
    /// </summary>
    public static ReadOnlySpan<byte> JolietUcs2Escape => "%/E"u8;

    /// <summary>Maximum length of a Joliet file/directory identifier in UCS-2 characters.</summary>
    public const int JolietMaxNameChars = 64;

    /// <summary>
    /// Rounds a byte count up to a whole number of logical sectors.
    /// </summary>
    public static long RoundUpToSector(long bytes)
    {
        return (bytes + LogicalSectorSize - 1) / LogicalSectorSize * LogicalSectorSize;
    }

    /// <summary>
    /// Number of whole logical sectors needed to hold <paramref name="bytes"/>.
    /// </summary>
    public static long SectorsFor(long bytes)
    {
        return (bytes + LogicalSectorSize - 1) / LogicalSectorSize;
    }
}
