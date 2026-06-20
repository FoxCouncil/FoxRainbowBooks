using System.Buffers.Binary;
using System.Text;
using FoxOrangebook.FileSystem;
using FoxOrangebook.FileSystem.Udf;

namespace FoxOrangebook.Tests;

public sealed class UdfBridgeTests
{
    private const int Sector = 2048;

    // ── Volume recognition sequence ──────────────────────────────

    [Fact]
    public void Bridge_RecognitionSequence_FollowsIsoDescriptors()
    {
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("a.txt", new byte[] { 1 }));

        // PVD(16), Joliet SVD(17), ISO terminator(18), then BEA01/NSR02/TEA01.
        Assert.Equal("BEA01", StandardId(image, 19));
        Assert.Equal("NSR02", StandardId(image, 20));
        Assert.Equal("TEA01", StandardId(image, 21));
    }

    // ── Anchors ──────────────────────────────────────────────────

    [Fact]
    public void Bridge_MainAnchor_AtSector256_IsValid()
    {
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("a.txt", new byte[] { 1 }));

        ushort tagId = VerifyTag(image, UdfConstants.AnchorSector, UdfConstants.AnchorSector);
        Assert.Equal(UdfConstants.TagAnchorVolumeDescriptorPointer, tagId);

        uint mainVdsLocation = U32(image, UdfConstants.AnchorSector * Sector + 20);
        Assert.Equal((uint)(UdfConstants.AnchorSector + 1), mainVdsLocation);
    }

    [Fact]
    public void Bridge_TailAnchor_AtLastSector_IsValid()
    {
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("a.txt", new byte[] { 1 }));

        long lastSector = image.Length / Sector - 1;
        ushort tagId = VerifyTag(image, lastSector, lastSector);
        Assert.Equal(UdfConstants.TagAnchorVolumeDescriptorPointer, tagId);
    }

    // ── Volume descriptor sequence ───────────────────────────────

    [Fact]
    public void Bridge_MainVds_HasExpectedDescriptorsWithValidTags()
    {
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("a.txt", new byte[] { 1 }));

        int vds = UdfConstants.AnchorSector + 1;
        Assert.Equal(UdfConstants.TagPrimaryVolumeDescriptor, VerifyTag(image, vds + 0, vds + 0));
        Assert.Equal(UdfConstants.TagImplementationUseVolumeDescriptor, VerifyTag(image, vds + 1, vds + 1));
        Assert.Equal(UdfConstants.TagPartitionDescriptor, VerifyTag(image, vds + 2, vds + 2));
        Assert.Equal(UdfConstants.TagLogicalVolumeDescriptor, VerifyTag(image, vds + 3, vds + 3));
        Assert.Equal(UdfConstants.TagUnallocatedSpaceDescriptor, VerifyTag(image, vds + 4, vds + 4));
        Assert.Equal(UdfConstants.TagTerminatingDescriptor, VerifyTag(image, vds + 5, vds + 5));
    }

    [Fact]
    public void Bridge_ReserveVds_MirrorsMainVds()
    {
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("a.txt", new byte[] { 1 }));

        int reserve = UdfConstants.AnchorSector + 1 + UdfConstants.VolumeDescriptorSequenceSectors;
        Assert.Equal(UdfConstants.TagPrimaryVolumeDescriptor, VerifyTag(image, reserve + 0, reserve + 0));
        Assert.Equal(UdfConstants.TagPartitionDescriptor, VerifyTag(image, reserve + 2, reserve + 2));
        Assert.Equal(UdfConstants.TagLogicalVolumeDescriptor, VerifyTag(image, reserve + 3, reserve + 3));
    }

    // ── End-to-end walk ──────────────────────────────────────────

    [Fact]
    public void Bridge_FileContent_RoundTripsThroughUdf()
    {
        byte[] payload = Encoding.ASCII.GetBytes("udf bridge payload — round trip");

        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("hello.txt", payload));

        var fs = OpenUdf(image);
        var root = fs.ReadDirectory(fs.RootFeBlock);
        var entry = root.Single(e => e.Name == "hello.txt");

        Assert.False(entry.IsDirectory);
        Assert.Equal(payload, fs.ReadFile(entry.FeBlock));
    }

    [Fact]
    public void Bridge_NestedDirectory_RoundTripsThroughUdf()
    {
        byte[] payload = Encoding.ASCII.GetBytes("deep udf content");

        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile("folder/inner/file.dat", payload));

        var fs = OpenUdf(image);
        var folder = fs.ReadDirectory(fs.RootFeBlock).Single(e => e.Name == "folder" && e.IsDirectory);
        var inner = fs.ReadDirectory(folder.FeBlock).Single(e => e.Name == "inner" && e.IsDirectory);
        var file = fs.ReadDirectory(inner.FeBlock).Single(e => e.Name == "file.dat");

        Assert.Equal(payload, fs.ReadFile(file.FeBlock));
    }

    [Fact]
    public void Bridge_LongUnicodeName_PreservedInUdf()
    {
        const string name = "Naïve café résumé — π.txt";
        byte[] image = BuildImage(new DiscImageOptions(), b => b.AddFile(name, new byte[] { 0xAB }));

        var fs = OpenUdf(image);
        var entry = fs.ReadDirectory(fs.RootFeBlock).Single(e => !e.IsDirectory);
        Assert.Equal(name, entry.Name);
    }

    [Fact]
    public void Bridge_UdfDataPosition_MatchesIsoExtent()
    {
        byte[] payload = Encoding.ASCII.GetBytes("shared between iso and udf");
        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = false }, b => b.AddFile("share.txt", payload));

        // UDF view of the file data.
        var fs = OpenUdf(image);
        var udfEntry = fs.ReadDirectory(fs.RootFeBlock).Single(e => !e.IsDirectory);
        uint udfDataLba = fs.FileDataLba(udfEntry.FeBlock);

        // ISO view: root directory record in the PVD → the file's extent LBA.
        uint isoRootLba = U32Both(image, 16 * Sector + 158);
        uint isoRootLen = U32Both(image, 16 * Sector + 166);
        uint isoDataLba = FindIsoFileLba(image, isoRootLba, isoRootLen);

        Assert.Equal(isoDataLba, udfDataLba);
        Assert.Equal(payload, image.AsSpan((int)(udfDataLba * Sector), payload.Length).ToArray());
    }

    [Fact]
    public void Bridge_DisablingUdf_OmitsAnchor()
    {
        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, b => b.AddFile("a.txt", new byte[] { 1 }));

        // Without UDF the volume is far smaller than the fixed anchor sector.
        Assert.True(image.Length / Sector < UdfConstants.AnchorSector);
    }

    // ── Helpers ──────────────────────────────────────────────────

    private static byte[] BuildImage(DiscImageOptions options, Action<DiscImageBuilder> populate)
    {
        var builder = new DiscImageBuilder(options);
        populate(builder);

        using var ms = new MemoryStream();
        builder.Build().WriteTo(ms);
        return ms.ToArray();
    }

    private static string StandardId(byte[] image, int sector)
    {
        return Encoding.ASCII.GetString(image.AsSpan(sector * Sector + 1, 5));
    }

    private static ushort U16(byte[] image, int offset) => BinaryPrimitives.ReadUInt16LittleEndian(image.AsSpan(offset, 2));

    private static uint U32(byte[] image, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(image.AsSpan(offset, 4));

    private static uint U32Both(byte[] image, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(image.AsSpan(offset, 4));

    /// <summary>Verifies a descriptor tag's checksum, CRC, and recorded location; returns its tag id.</summary>
    private static ushort VerifyTag(byte[] image, long sector, long expectedLocation)
    {
        int baseOff = (int)(sector * Sector);
        var tag = image.AsSpan(baseOff, 16);

        Assert.Equal(tag[4], UdfCrc.TagChecksum(tag));

        ushort storedCrc = U16(image, baseOff + 8);
        ushort crcLength = U16(image, baseOff + 10);
        Assert.Equal(storedCrc, UdfCrc.Compute(image.AsSpan(baseOff + 16, crcLength)));

        Assert.Equal((uint)expectedLocation, U32(image, baseOff + 12));

        return U16(image, baseOff);
    }

    private static uint FindIsoFileLba(byte[] image, uint extentLba, uint extentLength)
    {
        int baseOff = (int)(extentLba * Sector);
        int off = 0;

        while (off < extentLength)
        {
            byte recLen = image[baseOff + off];

            if (recLen == 0)
            {
                off += Sector - (off % Sector);
                continue;
            }

            int rec = baseOff + off;
            byte flags = image[rec + 25];
            byte idLen = image[rec + 32];

            if ((flags & 0x02) == 0 && idLen > 1)
            {
                return U32Both(image, rec + 2);
            }

            off += recLen;
        }

        throw new InvalidOperationException("No file record found in the ISO root directory.");
    }

    private static UdfView OpenUdf(byte[] image)
    {
        uint mainVds = U32(image, UdfConstants.AnchorSector * Sector + 20);

        int pdSector = -1;
        int lvdSector = -1;

        for (int i = 0; i < UdfConstants.VolumeDescriptorSequenceSectors; i++)
        {
            int s = (int)mainVds + i;
            ushort tagId = U16(image, s * Sector);

            if (tagId == UdfConstants.TagPartitionDescriptor)
            {
                pdSector = s;
            }
            else if (tagId == UdfConstants.TagLogicalVolumeDescriptor)
            {
                lvdSector = s;
            }
            else if (tagId == UdfConstants.TagTerminatingDescriptor)
            {
                break;
            }
        }

        uint partitionStart = U32(image, pdSector * Sector + 188);
        uint fsdBlock = U32(image, lvdSector * Sector + 252); // long_ad location in LogicalVolumeContentsUse

        int fsdOff = (int)((partitionStart + fsdBlock) * Sector);
        uint rootFeBlock = U32(image, fsdOff + 500); // root directory ICB long_ad location

        return new UdfView(image, partitionStart, rootFeBlock);
    }

    private sealed class UdfView
    {
        private readonly byte[] _image;
        private readonly uint _partitionStart;

        public UdfView(byte[] image, uint partitionStart, uint rootFeBlock)
        {
            _image = image;
            _partitionStart = partitionStart;
            RootFeBlock = rootFeBlock;
        }

        public uint RootFeBlock { get; }

        public List<UdfEntry> ReadDirectory(uint feBlock)
        {
            var (position, length) = ReadFeExtent(feBlock);
            int baseOff = (int)((_partitionStart + position) * Sector);
            int off = 0;
            var entries = new List<UdfEntry>();

            while (off < length)
            {
                int fid = baseOff + off;
                byte characteristics = _image[fid + 18];
                byte idLen = _image[fid + 19];
                uint icbBlock = BinaryPrimitives.ReadUInt32LittleEndian(_image.AsSpan(fid + 24, 4));
                ushort iuLen = BinaryPrimitives.ReadUInt16LittleEndian(_image.AsSpan(fid + 36, 2));

                int idOff = fid + 38 + iuLen;
                int total = (38 + iuLen + idLen + 3) & ~3;

                if ((characteristics & UdfConstants.FileCharacteristicParent) == 0)
                {
                    bool isDir = (characteristics & UdfConstants.FileCharacteristicDirectory) != 0;
                    entries.Add(new UdfEntry(DecodeIdentifier(idOff, idLen), icbBlock, isDir));
                }

                off += total;
            }

            return entries;
        }

        public byte[] ReadFile(uint feBlock)
        {
            var (position, length) = ReadFeExtent(feBlock);

            if (length == 0)
            {
                return Array.Empty<byte>();
            }

            int dataOff = (int)((_partitionStart + position) * Sector);
            return _image.AsSpan(dataOff, (int)length).ToArray();
        }

        public uint FileDataLba(uint feBlock)
        {
            var (position, _) = ReadFeExtent(feBlock);
            return _partitionStart + position;
        }

        private (uint Position, uint Length) ReadFeExtent(uint feBlock)
        {
            int feOff = (int)((_partitionStart + feBlock) * Sector);
            uint adLength = BinaryPrimitives.ReadUInt32LittleEndian(_image.AsSpan(feOff + 172, 4));

            if (adLength == 0)
            {
                return (0, 0);
            }

            uint length = BinaryPrimitives.ReadUInt32LittleEndian(_image.AsSpan(feOff + 176, 4));
            uint position = BinaryPrimitives.ReadUInt32LittleEndian(_image.AsSpan(feOff + 180, 4));
            return (position, length);
        }

        private string DecodeIdentifier(int offset, int idLen)
        {
            if (idLen == 0)
            {
                return string.Empty;
            }

            byte compression = _image[offset];

            if (compression == 16)
            {
                return Encoding.BigEndianUnicode.GetString(_image.AsSpan(offset + 1, idLen - 1));
            }

            return Encoding.Latin1.GetString(_image.AsSpan(offset + 1, idLen - 1));
        }
    }

    private readonly record struct UdfEntry(string Name, uint FeBlock, bool IsDirectory);
}
