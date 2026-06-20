using System.Buffers.Binary;
using System.Text;
using FoxOrangebook.FileSystem;

namespace FoxOrangebook.Tests;

public sealed class DiscImageBuilderTests
{
    private const int Sector = 2048;

    // ── Geometry ─────────────────────────────────────────────────

    [Fact]
    public void Build_EmptyVolume_HasSystemAreaThenDescriptors()
    {
        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, _ => { });

        // System Area: 16 zero sectors.
        for (int i = 0; i < 16 * Sector; i++)
        {
            Assert.Equal(0, image[i]);
        }

        // PVD at sector 16.
        var pvd = Sector16(image, 16);
        Assert.Equal(0x01, pvd[0]);
        Assert.True(pvd.Slice(1, 5).SequenceEqual("CD001"u8));
        Assert.Equal(0x01, pvd[6]);
    }

    [Fact]
    public void Build_ByteLength_IsWholeSectorsAndMatchesVolumeSpaceSize()
    {
        DiscImage img = BuildBuilder(new DiscImageOptions { EnableUdf = false }, b => b.AddFile("a.bin", new byte[5000])).Build();

        Assert.Equal(0, img.ByteLength % Sector);
        Assert.Equal(img.SectorCount * Sector, img.ByteLength);

        byte[] image = ReadAll(img);
        uint volumeSpace = ReadBothEndian32(image.AsSpan(16 * Sector + 80, 8));
        Assert.Equal((uint)img.SectorCount, volumeSpace);
    }

    [Fact]
    public void OpenRead_And_WriteTo_ProduceIdenticalBytes()
    {
        DiscImage img = BuildBuilder(new DiscImageOptions(), b =>
        {
            b.AddFile("docs/readme.txt", Encoding.ASCII.GetBytes("hello world"));
            b.AddFile("data.bin", Enumerable.Range(0, 4096).Select(i => (byte)i).ToArray());
        }).Build();

        byte[] viaWrite = ReadAll(img);

        byte[] viaOpen;
        using (var s = img.OpenRead())
        using (var ms = new MemoryStream())
        {
            s.CopyTo(ms);
            viaOpen = ms.ToArray();
        }

        Assert.Equal(viaWrite, viaOpen);
        Assert.Equal(img.ByteLength, viaWrite.Length);
    }

    // ── Volume descriptors ───────────────────────────────────────

    [Fact]
    public void Build_VolumeIdentifier_WrittenToPvd()
    {
        byte[] image = BuildImage(new DiscImageOptions { VolumeIdentifier = "MYDISC", EnableUdf = false }, _ => { });

        string volId = Encoding.ASCII.GetString(Sector16(image, 16).Slice(40, 32)).TrimEnd(' ');
        Assert.Equal("MYDISC", volId);
    }

    [Fact]
    public void Build_JolietEnabled_EmitsSupplementaryDescriptorWithEscape()
    {
        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = true, EnableUdf = false }, _ => { });

        var svd = Sector16(image, 17);
        Assert.Equal(0x02, svd[0]);
        Assert.True(svd.Slice(1, 5).SequenceEqual("CD001"u8));
        Assert.True(svd.Slice(88, 3).SequenceEqual("%/E"u8));

        // Terminator follows the SVD.
        Assert.Equal(0xFF, Sector16(image, 18)[0]);
    }

    [Fact]
    public void Build_JolietDisabled_TerminatorImmediatelyAfterPvd()
    {
        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = false, EnableUdf = false }, _ => { });

        Assert.Equal(0x01, Sector16(image, 16)[0]);
        Assert.Equal(0xFF, Sector16(image, 17)[0]);
    }

    // ── Directory tree round-trips ───────────────────────────────

    [Fact]
    public void Build_FileAtRoot_ContentRoundTrips()
    {
        byte[] payload = Encoding.ASCII.GetBytes("The quick brown fox jumps over the lazy dog.");

        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, b => b.AddFile("FOX.TXT", payload));

        var root = ReadRootDirectory(image, joliet: false);
        var entry = root.Single(e => e.Id == "FOX.TXT;1");

        byte[] content = ReadExtent(image, entry.Lba, (int)entry.Length);
        Assert.Equal(payload, content);
        Assert.Equal(0, entry.Flags & 0x02); // not a directory
    }

    [Fact]
    public void Build_NestedDirectories_FileRoundTripsThroughTree()
    {
        byte[] payload = Encoding.ASCII.GetBytes("nested content");

        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, b => b.AddFile("a/b/c/deep.txt", payload));

        var root = ReadRootDirectory(image, joliet: false);
        var a = root.Single(e => e.Id == "A" && IsDir(e));
        var b = ReadDirectory(image, a.Lba, (int)a.Length, joliet: false).Single(e => e.Id == "B" && IsDir(e));
        var c = ReadDirectory(image, b.Lba, (int)b.Length, joliet: false).Single(e => e.Id == "C" && IsDir(e));
        var file = ReadDirectory(image, c.Lba, (int)c.Length, joliet: false).Single(e => e.Id == "DEEP.TXT;1");

        Assert.Equal(payload, ReadExtent(image, file.Lba, (int)file.Length));
    }

    [Fact]
    public void Build_DotAndDotDot_PresentInEveryDirectory()
    {
        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, b => b.AddDirectory("sub"));

        var root = ReadRootDirectoryRaw(image, joliet: false);
        Assert.Equal("\0", root[0].Id);   // "." identifier is a single 0x00
        Assert.Equal("\x01", root[1].Id); // ".." identifier is a single 0x01

        var sub = root.Single(e => e.Id == "SUB" && IsDir(e));
        var subEntries = ReadDirectoryRaw(image, sub.Lba, (int)sub.Length, joliet: false);
        Assert.Equal("\0", subEntries[0].Id);
        Assert.Equal("\x01", subEntries[1].Id);
    }

    [Fact]
    public void Build_MultipleFiles_ExtentsDoNotOverlapAndContentMatches()
    {
        var files = new Dictionary<string, byte[]>
        {
            ["one.bin"] = Pattern(0x11, 3000),
            ["two.bin"] = Pattern(0x22, 100),
            ["three.bin"] = Pattern(0x33, 4096),
        };

        byte[] image = BuildImage(new DiscImageOptions { EnableUdf = false }, b =>
        {
            foreach (var (name, data) in files)
            {
                b.AddFile(name, data);
            }
        });

        var root = ReadRootDirectory(image, joliet: false);

        foreach (var (name, data) in files)
        {
            var entry = root.Single(e => e.Id == name.ToUpperInvariant() + ";1");
            Assert.Equal(data, ReadExtent(image, entry.Lba, (int)entry.Length));
        }
    }

    // ── Joliet ───────────────────────────────────────────────────

    [Fact]
    public void Build_Joliet_PreservesUnicodeLongName()
    {
        const string name = "Café Ω — a rather long file name.txt";
        byte[] payload = Encoding.ASCII.GetBytes("unicode");

        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = true, EnableUdf = false }, b => b.AddFile(name, payload));

        var jolietRoot = ReadRootDirectory(image, joliet: true);
        var entry = jolietRoot.Single(e => e.Id.StartsWith("Café Ω", StringComparison.Ordinal));

        Assert.Equal(name + ";1", entry.Id);
        Assert.Equal(payload, ReadExtent(image, entry.Lba, (int)entry.Length));

        // The primary tree carries the same file with a mangled 8.3-ish name.
        var primaryRoot = ReadRootDirectory(image, joliet: false);
        var primaryEntry = primaryRoot.Single(e => !IsDir(e));
        Assert.Equal(payload, ReadExtent(image, primaryEntry.Lba, (int)primaryEntry.Length));
        Assert.DoesNotContain('é', primaryEntry.Id);
    }

    [Fact]
    public void Build_Joliet_FileSharesExtentWithPrimary()
    {
        byte[] payload = Encoding.ASCII.GetBytes("shared extent");

        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = true, EnableUdf = false }, b => b.AddFile("share.txt", payload));

        var primary = ReadRootDirectory(image, joliet: false).Single(e => !IsDir(e));
        var joliet = ReadRootDirectory(image, joliet: true).Single(e => !IsDir(e));

        Assert.Equal(primary.Lba, joliet.Lba);
        Assert.Equal(primary.Length, joliet.Length);
    }

    // ── Naming rules ─────────────────────────────────────────────

    [Fact]
    public void Build_DuplicateMangledNames_AreDisambiguated()
    {
        // Distinct source names that mangle to the same primary identifier
        // ("R_ADME.TXT") must remain unique on the primary tree.
        byte[] image = BuildImage(new DiscImageOptions { EnableJoliet = false, EnableUdf = false }, b =>
        {
            b.AddFile("réadme.txt", new byte[] { 1 });
            b.AddFile("rëadme.txt", new byte[] { 2 }); // differs only in the accent
        });

        var root = ReadRootDirectory(image, joliet: false).Where(e => !IsDir(e)).ToList();
        Assert.Equal(2, root.Count);
        Assert.NotEqual(root[0].Id, root[1].Id);
    }

    [Fact]
    public void AddFile_DuplicateExactName_Throws()
    {
        var builder = new DiscImageBuilder();
        builder.AddFile("dup.txt", new byte[] { 1 });

        Assert.Throws<InvalidOperationException>(() => builder.AddFile("dup.txt", new byte[] { 2 }));
    }

    [Fact]
    public void AddFile_ParentTraversal_Throws()
    {
        var builder = new DiscImageBuilder();
        Assert.Throws<ArgumentException>(() => builder.AddFile("../escape.txt", new byte[] { 1 }));
    }

    // ── File output ──────────────────────────────────────────────

    [Fact]
    public async Task WriteToFileAsync_ProducesFileMatchingOpenRead()
    {
        DiscImage img = BuildBuilder(new DiscImageOptions(), b => b.AddFile("payload.bin", Pattern(0xAB, 9000))).Build();

        string path = Path.Combine(Path.GetTempPath(), $"foxiso_{Guid.NewGuid():N}.iso");

        try
        {
            await img.WriteToFileAsync(path);

            byte[] onDisk = await File.ReadAllBytesAsync(path);
            Assert.Equal(img.ByteLength, onDisk.Length);
            Assert.Equal(ReadAll(img), onDisk);
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────

    private static DiscImageBuilder BuildBuilder(DiscImageOptions options, Action<DiscImageBuilder> populate)
    {
        var builder = new DiscImageBuilder(options);
        populate(builder);
        return builder;
    }

    private static byte[] BuildImage(DiscImageOptions options, Action<DiscImageBuilder> populate)
    {
        return ReadAll(BuildBuilder(options, populate).Build());
    }

    private static byte[] ReadAll(DiscImage img)
    {
        using var ms = new MemoryStream();
        img.WriteTo(ms);
        return ms.ToArray();
    }

    private static Span<byte> Sector16(byte[] image, int sectorIndex)
    {
        return image.AsSpan(sectorIndex * Sector, Sector);
    }

    private static byte[] Pattern(byte seed, int length)
    {
        var data = new byte[length];

        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)(seed + i);
        }

        return data;
    }

    private static byte[] ReadExtent(byte[] image, uint lba, int length)
    {
        return image.AsSpan((int)(lba * Sector), length).ToArray();
    }

    private static uint ReadBothEndian32(ReadOnlySpan<byte> field)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(field.Slice(0, 4));
    }

    private readonly record struct DirEntry(string Id, uint Lba, uint Length, byte Flags);

    private static bool IsDir(DirEntry e) => (e.Flags & 0x02) != 0;

    private static List<DirEntry> ReadRootDirectory(byte[] image, bool joliet)
    {
        return ReadRootDirectoryRaw(image, joliet).Where(e => e.Id != "\0" && e.Id != "\x01").ToList();
    }

    private static List<DirEntry> ReadRootDirectoryRaw(byte[] image, bool joliet)
    {
        int descriptorSector = joliet ? 17 : 16;
        var descriptor = Sector16(image, descriptorSector);

        // Root directory record sits at offset 156 in the volume descriptor.
        uint rootLba = ReadBothEndian32(descriptor.Slice(158, 8));
        uint rootLen = ReadBothEndian32(descriptor.Slice(166, 8));

        return ReadDirectoryRaw(image, rootLba, (int)rootLen, joliet);
    }

    private static List<DirEntry> ReadDirectory(byte[] image, uint extentLba, int extentLength, bool joliet)
    {
        return ReadDirectoryRaw(image, extentLba, extentLength, joliet).Where(e => e.Id != "\0" && e.Id != "\x01").ToList();
    }

    private static List<DirEntry> ReadDirectoryRaw(byte[] image, uint extentLba, int extentLength, bool joliet)
    {
        var entries = new List<DirEntry>();
        int baseOff = (int)(extentLba * Sector);
        int off = 0;

        while (off < extentLength)
        {
            byte recLen = image[baseOff + off];

            if (recLen == 0)
            {
                // Records never span a sector; jump to the next sector boundary.
                int into = off % Sector;
                off += Sector - into;
                continue;
            }

            int rec = baseOff + off;
            uint lba = ReadBothEndian32(image.AsSpan(rec + 2, 8));
            uint len = ReadBothEndian32(image.AsSpan(rec + 10, 8));
            byte flags = image[rec + 25];
            byte idLen = image[rec + 32];

            string id;

            if (idLen == 1 && (image[rec + 33] == 0x00 || image[rec + 33] == 0x01))
            {
                id = ((char)image[rec + 33]).ToString();
            }
            else if (joliet)
            {
                id = Encoding.BigEndianUnicode.GetString(image.AsSpan(rec + 33, idLen));
            }
            else
            {
                id = Encoding.ASCII.GetString(image.AsSpan(rec + 33, idLen));
            }

            entries.Add(new DirEntry(id, lba, len, flags));
            off += recLen;
        }

        return entries;
    }
}
