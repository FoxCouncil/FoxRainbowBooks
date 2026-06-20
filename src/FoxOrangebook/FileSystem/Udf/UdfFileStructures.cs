using System.Buffers.Binary;

namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// Builders for the UDF file-structure descriptors that live inside the
/// partition (partition-relative addressing): the File Set Descriptor, File
/// Entries (directories and files), and File Identifier Descriptors.
/// </summary>
internal static class UdfFileStructures
{
    private const int Sector = IsoConstants.LogicalSectorSize;

    // Owner/group/other read; directories additionally executable (ECMA-167 §14.9.5).
    private const uint PermissionsRead = 0x1084;
    private const uint PermissionsExecute = 0x0421;

    /// <summary>A short_ad extent: a partition-relative block and a byte length.</summary>
    public readonly record struct Extent(uint Position, uint Length);

    /// <summary>File Set Descriptor (ECMA-167 §4.3.1), recorded at partition block 0.</summary>
    public static byte[] BuildFileSetDescriptor(uint location, string logicalVolumeId, string fileSetId, uint rootDirectoryBlock, DateTimeOffset when)
    {
        var s = new byte[Sector];
        UdfEncoding.WriteTimestamp(s.AsSpan(16, 12), when);
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(28, 2), 3); // interchange level
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(30, 2), 3); // maximum interchange level
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(32, 4), 1); // character set list
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(36, 4), 1); // maximum character set list
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(40, 4), 0); // file set number
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(44, 4), 0); // file set descriptor number
        UdfEncoding.WriteCharSpec(s.AsSpan(48, 64));
        UdfEncoding.WriteDString(s.AsSpan(112, 128), logicalVolumeId);
        UdfEncoding.WriteCharSpec(s.AsSpan(240, 64));
        UdfEncoding.WriteDString(s.AsSpan(304, 128), fileSetId);
        // Copyright (432) and abstract (464) file identifiers left empty.
        UdfEncoding.WriteLongAd(s.AsSpan(496, 16), (uint)Sector, rootDirectoryBlock, 0); // root directory ICB
        UdfEncoding.WriteDomainEntityId(s.AsSpan(512, 32), UdfConstants.OstaDomain);
        UdfEncoding.WriteTag(s.AsSpan(0, 608), UdfConstants.TagFileSetDescriptor, location, 592);
        return s;
    }

    /// <summary>
    /// File Entry (ECMA-167 §4.3.5), strategy 4 single direct entry, short_ad
    /// allocation. Recorded in one logical block at the given partition location.
    /// </summary>
    public static byte[] BuildFileEntry(uint location, byte fileType, ulong informationLength, ushort linkCount, ulong uniqueId, IReadOnlyList<Extent> extents, DateTimeOffset when)
    {
        bool isDirectory = fileType == UdfConstants.FileTypeDirectory;
        var s = new byte[Sector];

        // ICB Tag (20 bytes at offset 16).
        var icb = s.AsSpan(16, 20);
        BinaryPrimitives.WriteUInt16LittleEndian(icb.Slice(4, 2), 4); // strategy type 4
        BinaryPrimitives.WriteUInt16LittleEndian(icb.Slice(8, 2), 1); // maximum number of entries
        icb[11] = fileType;
        // Parent ICB location (12) left zero; flags (18) = 0 → short_ad allocation.

        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(36, 4), 0xFFFFFFFF); // uid: unset
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(40, 4), 0xFFFFFFFF); // gid: unset

        uint permissions = PermissionsRead | (isDirectory ? PermissionsExecute : 0);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(44, 4), permissions);
        BinaryPrimitives.WriteUInt16LittleEndian(s.AsSpan(48, 2), linkCount);

        BinaryPrimitives.WriteUInt64LittleEndian(s.AsSpan(56, 8), informationLength);
        ulong blocks = (informationLength + (ulong)Sector - 1) / (ulong)Sector;
        BinaryPrimitives.WriteUInt64LittleEndian(s.AsSpan(64, 8), blocks);

        UdfEncoding.WriteTimestamp(s.AsSpan(72, 12), when); // access
        UdfEncoding.WriteTimestamp(s.AsSpan(84, 12), when); // modification
        UdfEncoding.WriteTimestamp(s.AsSpan(96, 12), when); // attribute
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(108, 4), 1); // checkpoint

        UdfEncoding.WriteImplementationEntityId(s.AsSpan(128, 32), UdfConstants.ImplementationId);
        BinaryPrimitives.WriteUInt64LittleEndian(s.AsSpan(160, 8), uniqueId);
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(168, 4), 0); // length of extended attributes

        int adLength = extents.Count * 8;
        BinaryPrimitives.WriteUInt32LittleEndian(s.AsSpan(172, 4), (uint)adLength);

        int total = 176 + adLength;

        if (total > Sector)
        {
            throw new InvalidOperationException("File Entry allocation descriptors exceed one logical block.");
        }

        for (int i = 0; i < extents.Count; i++)
        {
            UdfEncoding.WriteShortAd(s.AsSpan(176 + i * 8, 8), extents[i].Length, extents[i].Position);
        }

        UdfEncoding.WriteTag(s.AsSpan(0, total), UdfConstants.TagFileEntry, location, total - 16);
        return s;
    }

    /// <summary>
    /// Serializes one File Identifier Descriptor (ECMA-167 §14.4) into
    /// <paramref name="dst"/> and returns its length (padded to 4 bytes). The tag
    /// location is the partition-relative block where this FID is recorded.
    /// </summary>
    public static int WriteFileIdentifier(Span<byte> dst, uint tagLocation, bool isDirectory, bool isParent, uint targetFeBlock, string? identifier)
    {
        byte[] idBytes = isParent ? Array.Empty<byte>() : UdfEncoding.EncodeCompressedUnicode(identifier);
        int idLen = idBytes.Length;

        int unpadded = 38 + idLen;
        int total = (unpadded + 3) & ~3; // pad to a 4-byte boundary

        dst.Slice(0, total).Clear();

        BinaryPrimitives.WriteUInt16LittleEndian(dst.Slice(16, 2), 1); // file version number

        byte characteristics = 0;

        if (isDirectory)
        {
            characteristics |= UdfConstants.FileCharacteristicDirectory;
        }

        if (isParent)
        {
            characteristics |= UdfConstants.FileCharacteristicParent;
        }

        dst[18] = characteristics;
        dst[19] = (byte)idLen;
        UdfEncoding.WriteLongAd(dst.Slice(20, 16), (uint)Sector, targetFeBlock, 0); // ICB
        BinaryPrimitives.WriteUInt16LittleEndian(dst.Slice(36, 2), 0); // length of implementation use

        if (idLen > 0)
        {
            idBytes.CopyTo(dst.Slice(38, idLen));
        }

        UdfEncoding.WriteTag(dst.Slice(0, total), UdfConstants.TagFileIdentifierDescriptor, tagLocation, total - 16);
        return total;
    }

    /// <summary>Length a File Identifier Descriptor will occupy (padded to 4 bytes).</summary>
    public static int FileIdentifierLength(bool isParent, string? identifier)
    {
        int idLen = isParent ? 0 : UdfEncoding.EncodeCompressedUnicode(identifier).Length;
        return (38 + idLen + 3) & ~3;
    }
}
