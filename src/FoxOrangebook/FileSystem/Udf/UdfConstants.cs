namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// Constants for the UDF 1.02 / ECMA-167 (2nd edition) on-disc structures used
/// to build an ISO 9660 + UDF "bridge" volume.
/// </summary>
internal static class UdfConstants
{
    /// <summary>Descriptor tag version for ECMA-167 2nd edition (UDF 1.02).</summary>
    public const ushort DescriptorVersion = 2;

    /// <summary>UDF revision encoded for entity-identifier suffixes (1.02).</summary>
    public const ushort UdfRevision = 0x0102;

    /// <summary>First sector of the main Anchor Volume Descriptor Pointer.</summary>
    public const int AnchorSector = 256;

    /// <summary>Minimum number of sectors reserved for a volume descriptor sequence.</summary>
    public const int VolumeDescriptorSequenceSectors = 16;

    // ── Descriptor tag identifiers (ECMA-167 §3/4) ───────────────

    public const ushort TagPrimaryVolumeDescriptor = 1;
    public const ushort TagAnchorVolumeDescriptorPointer = 2;
    public const ushort TagVolumeDescriptorPointer = 3;
    public const ushort TagImplementationUseVolumeDescriptor = 4;
    public const ushort TagPartitionDescriptor = 5;
    public const ushort TagLogicalVolumeDescriptor = 6;
    public const ushort TagUnallocatedSpaceDescriptor = 7;
    public const ushort TagTerminatingDescriptor = 8;
    public const ushort TagLogicalVolumeIntegrityDescriptor = 9;
    public const ushort TagFileSetDescriptor = 256;
    public const ushort TagFileIdentifierDescriptor = 257;
    public const ushort TagAllocationExtentDescriptor = 258;
    public const ushort TagIndirectEntry = 259;
    public const ushort TagTerminalEntry = 260;
    public const ushort TagFileEntry = 261;
    public const ushort TagExtendedAttributeHeaderDescriptor = 262;
    public const ushort TagUnallocatedSpaceEntry = 263;
    public const ushort TagSpaceBitmapDescriptor = 264;

    // ── ICB file types (ECMA-167 §14.6.6) ────────────────────────

    public const byte FileTypeDirectory = 4;
    public const byte FileTypeRegular = 5;

    // ── ICB flags / FID flags ────────────────────────────────────

    public const ushort IcbFlagArchive = 0x0020;
    public const byte FileCharacteristicExistence = 0x01;
    public const byte FileCharacteristicDirectory = 0x02;
    public const byte FileCharacteristicParent = 0x08;

    // ── Entity identifiers (OSTA / implementation) ───────────────

    public const string OstaDomain = "*OSTA UDF Compliant";
    public const string OstaLvInfo = "*UDF LV Info";
    public const string ImplementationId = "*FoxOrangebook";

    /// <summary>OS class "undefined" for entity-identifier suffixes.</summary>
    public const byte OsClassUndefined = 0;

    /// <summary>OS identifier "undefined" for entity-identifier suffixes.</summary>
    public const byte OsIdentifierUndefined = 0;
}
