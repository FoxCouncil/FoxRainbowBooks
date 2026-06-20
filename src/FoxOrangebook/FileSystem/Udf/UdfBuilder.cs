namespace FoxOrangebook.FileSystem.Udf;

/// <summary>
/// Lays out the UDF partition for a bridge volume: the File Set Descriptor, a
/// File Entry per directory and file, each directory's File Identifier
/// Descriptor stream, and the shared file-data extents. File data is placed
/// inside the partition and its absolute LBA is written back to each
/// <see cref="BuildFile"/> so the ISO 9660 directory records reference the very
/// same sectors.
/// </summary>
internal static class UdfBuilder
{
    private const int Sector = IsoConstants.LogicalSectorSize;
    private const uint MaxExtentLength = 0x3FFFF800; // < 2^30 and a multiple of the block size
    private const ulong FirstUserUniqueId = 16; // UDF reserves unique IDs 0–15 (root is 0)

    public static UdfBuildResult Build(BuildDirectory root, uint partitionStartLba, string logicalVolumeId, string fileSetId, DateTimeOffset when, List<PlacedRegion> regions)
    {
        var rootNode = BuildNodeTree(root, parent: null);

        var allDirs = new List<UdfDirNode>();
        var allFiles = new List<UdfFileNode>();
        Collect(rootNode, allDirs, allFiles);

        // Pass 1: File Entry blocks — FSD at block 0, root FE at 1, then the
        // remaining directory FEs, then the file FEs.
        uint cursor = 1;
        rootNode.FeBlock = cursor++;

        foreach (var dir in allDirs)
        {
            if (dir != rootNode)
            {
                dir.FeBlock = cursor++;
            }
        }

        foreach (var file in allFiles)
        {
            file.FeBlock = cursor++;
        }

        // Pass 2: directory FID streams.
        foreach (var dir in allDirs)
        {
            dir.Children = BuildChildEntries(dir);
            dir.DataLength = ComputeDirectoryLength(dir);
            dir.DataBlock = cursor;
            cursor += (uint)IsoConstants.SectorsFor(dir.DataLength);
        }

        // Pass 3: shared file data — assign partition-relative blocks and the
        // absolute LBA the ISO tree will also point at.
        foreach (var file in allFiles)
        {
            file.DataBlock = cursor;
            file.Source.DataLba = partitionStartLba + file.DataBlock;
            cursor += (uint)IsoConstants.SectorsFor(file.Source.Length);
        }

        uint partitionLengthBlocks = cursor;

        // Pass 4: unique IDs and link counts.
        rootNode.UniqueId = 0;
        ulong nextUniqueId = FirstUserUniqueId;

        foreach (var dir in allDirs)
        {
            if (dir != rootNode)
            {
                dir.UniqueId = nextUniqueId++;
            }

            dir.LinkCount = (ushort)(2 + dir.SubDirs.Count);
        }

        foreach (var file in allFiles)
        {
            file.UniqueId = nextUniqueId++;
        }

        // Pass 5: emit.
        regions.Add(new PlacedRegion(partitionStartLba, UdfFileStructures.BuildFileSetDescriptor(0, logicalVolumeId, fileSetId, rootNode.FeBlock, when)));

        foreach (var dir in allDirs)
        {
            var extents = new List<UdfFileStructures.Extent> { new(dir.DataBlock, (uint)dir.DataLength) };
            byte[] fe = UdfFileStructures.BuildFileEntry(dir.FeBlock, UdfConstants.FileTypeDirectory, (ulong)dir.DataLength, dir.LinkCount, dir.UniqueId, extents, when);
            regions.Add(new PlacedRegion(partitionStartLba + dir.FeBlock, fe));

            regions.Add(new PlacedRegion(partitionStartLba + dir.DataBlock, BuildDirectoryData(dir)));
        }

        foreach (var file in allFiles)
        {
            List<UdfFileStructures.Extent> extents = ChunkExtents(file.DataBlock, file.Source.Length);
            byte[] fe = UdfFileStructures.BuildFileEntry(file.FeBlock, UdfConstants.FileTypeRegular, (ulong)file.Source.Length, 1, file.UniqueId, extents, when);
            regions.Add(new PlacedRegion(partitionStartLba + file.FeBlock, fe));
        }

        return new UdfBuildResult
        {
            PartitionStartLba = partitionStartLba,
            PartitionLengthBlocks = partitionLengthBlocks,
            NumberOfFiles = (uint)allFiles.Count,
            NumberOfDirectories = (uint)allDirs.Count,
            NextUniqueId = nextUniqueId,
        };
    }

    private static byte[] BuildDirectoryData(UdfDirNode dir)
    {
        long total = IsoConstants.RoundUpToSector(dir.DataLength);
        var buf = new byte[total];
        int offset = 0;

        // Parent ("..") entry first; the root's parent is itself.
        uint parentFe = dir.Parent?.FeBlock ?? dir.FeBlock;
        offset += WriteFid(buf, offset, dir.DataBlock, isDirectory: true, isParent: true, parentFe, identifier: null);

        foreach (var child in dir.Children)
        {
            offset += WriteFid(buf, offset, dir.DataBlock, child.IsDirectory, isParent: false, child.FeBlock, child.Identifier);
        }

        return buf;
    }

    private static int WriteFid(byte[] buf, int offset, uint dataBlock, bool isDirectory, bool isParent, uint targetFe, string? identifier)
    {
        uint tagLocation = dataBlock + (uint)(offset / Sector);
        return UdfFileStructures.WriteFileIdentifier(buf.AsSpan(offset), tagLocation, isDirectory, isParent, targetFe, identifier);
    }

    private static long ComputeDirectoryLength(UdfDirNode dir)
    {
        long len = UdfFileStructures.FileIdentifierLength(isParent: true, null);

        foreach (var child in dir.Children)
        {
            len += UdfFileStructures.FileIdentifierLength(isParent: false, child.Identifier);
        }

        return len;
    }

    private static List<ChildEntry> BuildChildEntries(UdfDirNode dir)
    {
        var entries = new List<ChildEntry>(dir.SubDirs.Count + dir.Files.Count);

        foreach (var sub in dir.SubDirs)
        {
            entries.Add(new ChildEntry(ClampName(sub.Source.Name), sub.FeBlock, true));
        }

        foreach (var file in dir.Files)
        {
            entries.Add(new ChildEntry(ClampName(file.Source.Name), file.FeBlock, false));
        }

        return entries;
    }

    private static string ClampName(string name)
    {
        // A File Identifier is capped at 255 bytes including the 1-byte
        // compression id: 254 Latin-1 chars or 127 UCS-2 chars.
        bool wide = false;

        foreach (char c in name)
        {
            if (c > 0xFF)
            {
                wide = true;
                break;
            }
        }

        int max = wide ? 127 : 254;
        return name.Length > max ? name[..max] : name;
    }

    private static List<UdfFileStructures.Extent> ChunkExtents(uint startBlock, long length)
    {
        var extents = new List<UdfFileStructures.Extent>();

        if (length == 0)
        {
            return extents;
        }

        long remaining = length;
        uint block = startBlock;

        while (remaining > 0)
        {
            uint chunk = (uint)Math.Min(remaining, MaxExtentLength);
            extents.Add(new UdfFileStructures.Extent(block, chunk));
            remaining -= chunk;
            block += (uint)IsoConstants.SectorsFor(chunk);
        }

        return extents;
    }

    private static UdfDirNode BuildNodeTree(BuildDirectory source, UdfDirNode? parent)
    {
        var node = new UdfDirNode { Source = source, Parent = parent };

        foreach (var subSource in source.Directories)
        {
            node.SubDirs.Add(BuildNodeTree(subSource, node));
        }

        foreach (var fileSource in source.Files)
        {
            node.Files.Add(new UdfFileNode { Source = fileSource });
        }

        return node;
    }

    private static void Collect(UdfDirNode dir, List<UdfDirNode> dirs, List<UdfFileNode> files)
    {
        dirs.Add(dir);
        files.AddRange(dir.Files);

        foreach (var sub in dir.SubDirs)
        {
            Collect(sub, dirs, files);
        }
    }

    private readonly record struct ChildEntry(string Identifier, uint FeBlock, bool IsDirectory);

    private sealed class UdfDirNode
    {
        public required BuildDirectory Source { get; init; }
        public UdfDirNode? Parent { get; init; }
        public List<UdfDirNode> SubDirs { get; } = new();
        public List<UdfFileNode> Files { get; } = new();
        public List<ChildEntry> Children { get; set; } = new();

        public uint FeBlock { get; set; }
        public uint DataBlock { get; set; }
        public long DataLength { get; set; }
        public ushort LinkCount { get; set; }
        public ulong UniqueId { get; set; }
    }

    private sealed class UdfFileNode
    {
        public required BuildFile Source { get; init; }
        public uint FeBlock { get; set; }
        public uint DataBlock { get; set; }
        public ulong UniqueId { get; set; }
    }
}

/// <summary>The partition geometry and counts produced by <see cref="UdfBuilder"/>.</summary>
internal sealed class UdfBuildResult
{
    public required uint PartitionStartLba { get; init; }
    public required uint PartitionLengthBlocks { get; init; }
    public required uint NumberOfFiles { get; init; }
    public required uint NumberOfDirectories { get; init; }
    public required ulong NextUniqueId { get; init; }
}
