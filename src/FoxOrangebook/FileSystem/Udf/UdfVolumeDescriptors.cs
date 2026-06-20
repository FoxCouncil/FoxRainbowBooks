using System.Buffers.Binary;

namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// Builders for the UDF volume-level descriptors: the Anchor pointer, the main
/// and reserve Volume Descriptor Sequence members (Primary, Implementation Use,
/// Partition, Logical Volume, Unallocated Space, Terminating), and the Logical
/// Volume Integrity Descriptor. Each returns one 2048-byte sector.
/// </summary>
internal static class UdfVolumeDescriptors
{
    private const int Sector = IsoConstants.LogicalSectorSize;

    /// <summary>Anchor Volume Descriptor Pointer (ECMA-167 §10.2).</summary>
    public static byte[] BuildAnchor(uint thisSector, uint mainVdsLocation, uint mainVdsLengthBytes, uint reserveVdsLocation, uint reserveVdsLengthBytes)
    {
        var s = new byte[Sector];
        UdfEncoding.WriteExtentAd(s.AsSpan(16, 8), mainVdsLengthBytes, mainVdsLocation);
        UdfEncoding.WriteExtentAd(s.AsSpan(24, 8), reserveVdsLengthBytes, reserveVdsLocation);
        UdfEncoding.WriteTag(s.AsSpan(0, 512), UdfConstants.TagAnchorVolumeDescriptorPointer, thisSector, 496);
        return s;
    }

    /// <summary>Primary Volume Descriptor (ECMA-167 §10.1).</summary>
    public static byte[] BuildPrimaryVolumeDescriptor(uint location, uint vdsNumber, string volumeId, string volumeSetId, DateTimeOffset when)
    {
        var s = new byte[Sector];
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(16, 4), vdsNumber);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(20, 4), 0); // primary volume descriptor number
        UdfEncoding.WriteDString(s.AsSpan(24, 32), volumeId);
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(56, 2), 1); // volume sequence number
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(58, 2), 1); // maximum volume sequence number
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(60, 2), 2); // interchange level
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(62, 2), 2); // maximum interchange level
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(64, 4), 1); // character set list
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(68, 4), 1); // maximum character set list
        UdfEncoding.WriteDString(s.AsSpan(72, 128), volumeSetId);
        UdfEncoding.WriteCharSpec(s.AsSpan(200, 64)); // descriptor character set
        UdfEncoding.WriteCharSpec(s.AsSpan(264, 64)); // explanatory character set
        // Volume abstract (328) and copyright (336) extents left zero.
        UdfEncoding.WriteImplementationEntityId(s.AsSpan(344, 32), UdfConstants.ImplementationId); // application id
        UdfEncoding.WriteTimestamp(s.AsSpan(376, 12), when);
        UdfEncoding.WriteImplementationEntityId(s.AsSpan(388, 32), UdfConstants.ImplementationId);
        UdfEncoding.WriteTag(s.AsSpan(0, 512), UdfConstants.TagPrimaryVolumeDescriptor, location, 496);
        return s;
    }

    /// <summary>Implementation Use Volume Descriptor with LV Information (UDF §2.2.7).</summary>
    public static byte[] BuildImplementationUseVolumeDescriptor(uint location, uint vdsNumber, string logicalVolumeId)
    {
        var s = new byte[Sector];
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(16, 4), vdsNumber);
        UdfEncoding.WriteUdfEntityId(s.AsSpan(20, 32), UdfConstants.OstaLvInfo);

        // Implementation Use → LVInformation, starting at byte 52.
        var impl = s.AsSpan(52, 460);
        UdfEncoding.WriteCharSpec(impl.Slice(0, 64));
        UdfEncoding.WriteDString(impl.Slice(64, 128), logicalVolumeId);   // LogicalVolumeIdentifier
        // LVInfo1/2/3 (dstrings) left empty.
        UdfEncoding.WriteImplementationEntityId(impl.Slice(248, 32), UdfConstants.ImplementationId);

        UdfEncoding.WriteTag(s.AsSpan(0, 512), UdfConstants.TagImplementationUseVolumeDescriptor, location, 496);
        return s;
    }

    /// <summary>Partition Descriptor (ECMA-167 §10.5).</summary>
    public static byte[] BuildPartitionDescriptor(uint location, uint vdsNumber, uint partitionStart, uint partitionLengthSectors)
    {
        var s = new byte[Sector];
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(16, 4), vdsNumber);
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(20, 2), 1); // partition flags: allocated
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(22, 2), 0); // partition number
        UdfEncoding.WriteRawEntityId(s.AsSpan(24, 32), "+NSR02"); // partition contents
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(184, 4), 1); // access type: read-only
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(188, 4), partitionStart);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(192, 4), partitionLengthSectors);
        UdfEncoding.WriteImplementationEntityId(s.AsSpan(196, 32), UdfConstants.ImplementationId);
        UdfEncoding.WriteTag(s.AsSpan(0, 512), UdfConstants.TagPartitionDescriptor, location, 496);
        return s;
    }

    /// <summary>Logical Volume Descriptor with a single type-1 partition map (ECMA-167 §10.6).</summary>
    public static byte[] BuildLogicalVolumeDescriptor(uint location, uint vdsNumber, string logicalVolumeId, uint fsdPartitionBlock, uint lvidLocation, uint lvidLengthBytes)
    {
        var s = new byte[Sector];
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(16, 4), vdsNumber);
        UdfEncoding.WriteCharSpec(s.AsSpan(20, 64));
        UdfEncoding.WriteDString(s.AsSpan(84, 128), logicalVolumeId);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(212, 4), (uint)Sector); // logical block size
        UdfEncoding.WriteDomainEntityId(s.AsSpan(216, 32), UdfConstants.OstaDomain);
        // LogicalVolumeContentsUse: long_ad to the File Set Descriptor.
        UdfEncoding.WriteLongAd(s.AsSpan(248, 16), (uint)Sector, fsdPartitionBlock, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(264, 4), 6); // map table length
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(268, 4), 1); // number of partition maps
        UdfEncoding.WriteImplementationEntityId(s.AsSpan(272, 32), UdfConstants.ImplementationId);
        UdfEncoding.WriteExtentAd(s.AsSpan(432, 8), lvidLengthBytes, lvidLocation); // integrity sequence extent

        // Partition map type 1 at byte 440.
        s[440] = 1; // map type
        s[441] = 6; // map length
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(442, 2), 1); // volume sequence number
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(444, 2), 0); // partition number

        UdfEncoding.WriteTag(s.AsSpan(0, 446), UdfConstants.TagLogicalVolumeDescriptor, location, 430);
        return s;
    }

    /// <summary>Unallocated Space Descriptor with no free extents (read-only volume).</summary>
    public static byte[] BuildUnallocatedSpaceDescriptor(uint location, uint vdsNumber)
    {
        var s = new byte[Sector];
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(16, 4), vdsNumber);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(20, 4), 0); // number of allocation descriptors
        UdfEncoding.WriteTag(s.AsSpan(0, 24), UdfConstants.TagUnallocatedSpaceDescriptor, location, 8);
        return s;
    }

    /// <summary>Terminating Descriptor (ECMA-167 §10.9).</summary>
    public static byte[] BuildTerminatingDescriptor(uint location)
    {
        var s = new byte[Sector];
        UdfEncoding.WriteTag(s.AsSpan(0, 512), UdfConstants.TagTerminatingDescriptor, location, 496);
        return s;
    }

    /// <summary>Logical Volume Integrity Descriptor, closed (ECMA-167 §10.10, UDF §2.2.6).</summary>
    public static byte[] BuildLogicalVolumeIntegrityDescriptor(uint location, DateTimeOffset when, uint partitionSizeBlocks, uint partitionFreeBlocks, uint numberOfFiles, uint numberOfDirectories, ulong nextUniqueId)
    {
        var s = new byte[Sector];
        UdfEncoding.WriteTimestamp(s.AsSpan(16, 12), when);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(28, 4), 1); // integrity type: close
        // Next integrity extent (32) left zero.
        BinaryPrimitives.WriteUInt64LittleEndian(s.AsSpan(40, 8), nextUniqueId); // logical volume header: next unique id
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(72, 4), 1); // number of partitions
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(76, 4), 46); // length of implementation use

        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(80, 4), partitionFreeBlocks); // free space table
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(84, 4), partitionSizeBlocks); // size table

        var impl = s.AsSpan(88, 46);
        UdfEncoding.WriteImplementationEntityId(impl.Slice(0, 32), UdfConstants.ImplementationId);
        BinaryPrimitives.WriteUInt32LittleEndian(impl.Slice(32, 4), numberOfFiles);
        BinaryPrimitives.WriteUInt32LittleEndian(impl.Slice(36, 4), numberOfDirectories);
        BinaryPrimitives.WriteUInt16LittleEndian(impl.Slice(40, 2), UdfConstants.UdfRevision); // min read
        BinaryPrimitives.WriteUInt16LittleEndian(impl.Slice(42, 2), UdfConstants.UdfRevision); // min write
        BinaryPrimitives.WriteUInt16LittleEndian(impl.Slice(44, 2), UdfConstants.UdfRevision); // max write

        UdfEncoding.WriteTag(s.AsSpan(0, 134), UdfConstants.TagLogicalVolumeIntegrityDescriptor, location, 118);
        return s;
    }
}
