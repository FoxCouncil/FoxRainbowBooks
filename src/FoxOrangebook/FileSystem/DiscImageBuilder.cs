using System.Buffers.Binary;
using System.Globalization;
using FoxOrangebook.FileSystem.Udf;

namespace FoxOrangebook.FileSystem;

/// <summary>
/// Builds a data-disc filesystem image (ISO 9660, optionally with a Joliet
/// supplementary descriptor and a UDF Bridge) and produces a
/// <see cref="DiscImage"/> of 2048-byte logical sectors that can be written
/// to a <c>.iso</c> file or streamed to a burner.
/// </summary>
/// <remarks>
/// <para>
/// Add content with <see cref="AddFile(string, string)"/> /
/// <see cref="AddDirectory"/>, then call <see cref="Build"/>. File contents are
/// referenced lazily — only their lengths are read during layout — so images
/// far larger than memory can be produced.
/// </para>
/// </remarks>
public sealed class DiscImageBuilder
{
    private readonly DiscImageOptions _options;
    private readonly BuildDirectory _root;

    public DiscImageBuilder(DiscImageOptions? options = null)
    {
        _options = options ?? new DiscImageOptions();
        _root = new BuildDirectory { Name = string.Empty, Parent = null };
    }

    /// <summary>Adds a file whose content comes from a path on the local filesystem.</summary>
    /// <param name="discPath">Destination path on the disc, e.g. <c>"docs/readme.txt"</c>.</param>
    /// <param name="sourceFilePath">Path to the source file to read at build time.</param>
    public void AddFile(string discPath, string sourceFilePath)
    {
        ArgumentNullException.ThrowIfNull(sourceFilePath);
        AddFileCore(discPath, new FileContentSource(sourceFilePath));
    }

    /// <summary>Adds a file whose content is the given byte array.</summary>
    /// <param name="discPath">Destination path on the disc, e.g. <c>"docs/readme.txt"</c>.</param>
    /// <param name="content">The file content.</param>
    public void AddFile(string discPath, byte[] content)
    {
        ArgumentNullException.ThrowIfNull(content);
        AddFileCore(discPath, new BytesContentSource(content));
    }

    /// <summary>Creates an (empty) directory on the disc, including any missing parents.</summary>
    public void AddDirectory(string discPath)
    {
        NavigateToDirectory(discPath, create: true);
    }

    private void AddFileCore(string discPath, IContentSource content)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(discPath);

        var parts = SplitPath(discPath);

        if (parts.Count == 0)
        {
            throw new ArgumentException("A file path must include a file name.", nameof(discPath));
        }

        string fileName = parts[^1];
        parts.RemoveAt(parts.Count - 1);

        var dir = NavigateToDirectory(parts);

        foreach (var existing in dir.Files)
        {
            if (string.Equals(existing.Name, fileName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"A file named '{fileName}' already exists in '{discPath}'.");
            }
        }

        dir.Files.Add(new BuildFile { Name = fileName, Content = content });
    }

    private BuildDirectory NavigateToDirectory(string discPath, bool create)
    {
        return NavigateToDirectory(SplitPath(discPath), create);
    }

    private BuildDirectory NavigateToDirectory(List<string> parts, bool create = true)
    {
        var dir = _root;

        foreach (var part in parts)
        {
            if (create)
            {
                dir = dir.GetOrAddDirectory(part);
            }
            else
            {
                dir = dir.FindDirectory(part) ?? throw new DirectoryNotFoundException($"Directory '{part}' does not exist on the disc.");
            }
        }

        return dir;
    }

    private static List<string> SplitPath(string path)
    {
        var parts = new List<string>();

        foreach (var raw in path.Split('/', '\\'))
        {
            string part = raw.Trim();

            if (part.Length == 0 || part == ".")
            {
                continue;
            }

            if (part == "..")
            {
                throw new ArgumentException("Disc paths may not contain '..'.", nameof(path));
            }

            parts.Add(part);
        }

        return parts;
    }

    // ── Layout and emission ──────────────────────────────────────

    /// <summary>
    /// Computes the on-disc layout and returns the finished image. The builder
    /// may be reused afterwards, but the returned image is an independent snapshot.
    /// </summary>
    public DiscImage Build()
    {
        DateTimeOffset when = _options.Timestamp ?? DateTimeOffset.UtcNow;

        var primary = IsoHierarchy.Build(_root, joliet: false);
        IsoHierarchy? joliet = _options.EnableJoliet ? IsoHierarchy.Build(_root, joliet: true) : null;

        return _options.EnableUdf
            ? BuildBridge(primary, joliet, when)
            : BuildIsoOnly(primary, joliet, when);
    }

    // ── Plain ISO 9660 (+ Joliet) layout ─────────────────────────

    private DiscImage BuildIsoOnly(IsoHierarchy primary, IsoHierarchy? joliet, DateTimeOffset when)
    {
        long lba = IsoConstants.VolumeDescriptorSetStart;

        var placement = new IsoPlacement
        {
            Pvd = lba++,
            Svd = joliet is not null ? lba++ : -1,
            Terminator = lba++,
        };

        lba = AssignIsoTables(primary, joliet, placement, lba);
        lba = AssignDirectoryExtents(primary, lba);

        if (joliet is not null)
        {
            lba = AssignDirectoryExtents(joliet, lba);
        }

        lba = AssignFileExtents(lba);
        long totalSectors = lba;

        var regions = new List<PlacedRegion>();
        EmitIsoVolume(totalSectors, primary, joliet, placement, when, regions);
        EmitFileExtents(_root, regions);

        return new DiscImage(totalSectors, regions);
    }

    // ── ISO 9660 + UDF 1.02 bridge layout ────────────────────────

    private DiscImage BuildBridge(IsoHierarchy primary, IsoHierarchy? joliet, DateTimeOffset when)
    {
        const int sector = IsoConstants.LogicalSectorSize;

        long lba = IsoConstants.VolumeDescriptorSetStart;

        var placement = new IsoPlacement
        {
            Pvd = lba++,
            Svd = joliet is not null ? lba++ : -1,
            Terminator = lba++,
        };

        // ECMA-167 volume recognition (extended area) follows the ISO descriptors.
        long beaLba = lba++;
        long nsrLba = lba++;
        long teaLba = lba++;

        // The UDF descriptors occupy a fixed region anchored at sector 256.
        long anchorMainLba = UdfConstants.AnchorSector;
        long mainVdsLba = anchorMainLba + 1;
        long reserveVdsLba = mainVdsLba + UdfConstants.VolumeDescriptorSequenceSectors;
        long lvidLba = reserveVdsLba + UdfConstants.VolumeDescriptorSequenceSectors;
        const long lvidSectors = 2; // LVID + terminating descriptor

        // ISO path tables and directory extents live past the UDF region so
        // that sector 256 is always free for the anchor.
        long isoLba = lvidLba + lvidSectors;
        isoLba = AssignIsoTables(primary, joliet, placement, isoLba);
        isoLba = AssignDirectoryExtents(primary, isoLba);

        if (joliet is not null)
        {
            isoLba = AssignDirectoryExtents(joliet, isoLba);
        }

        long partitionStart = isoLba;

        var regions = new List<PlacedRegion>();
        var udf = UdfBuilder.Build(_root, (uint)partitionStart, _options.VolumeIdentifier, _options.VolumeIdentifier, when, regions);

        long afterPartition = partitionStart + udf.PartitionLengthBlocks;
        long tailAnchorLba = afterPartition;
        long totalSectors = afterPartition + 1;

        // ISO 9660 structures.
        EmitIsoVolume(totalSectors, primary, joliet, placement, when, regions);

        // Volume recognition sequence.
        regions.Add(new PlacedRegion(beaLba, UdfEncoding.BuildRecognitionDescriptor("BEA01")));
        regions.Add(new PlacedRegion(nsrLba, UdfEncoding.BuildRecognitionDescriptor("NSR02")));
        regions.Add(new PlacedRegion(teaLba, UdfEncoding.BuildRecognitionDescriptor("TEA01")));

        // UDF anchors + volume descriptor sequences.
        uint vdsLengthBytes = (uint)(UdfConstants.VolumeDescriptorSequenceSectors * sector);
        uint lvidLengthBytes = (uint)(lvidSectors * sector);

        regions.Add(new PlacedRegion(anchorMainLba, UdfVolumeDescriptors.BuildAnchor((uint)anchorMainLba, (uint)mainVdsLba, vdsLengthBytes, (uint)reserveVdsLba, vdsLengthBytes)));
        regions.Add(new PlacedRegion(tailAnchorLba, UdfVolumeDescriptors.BuildAnchor((uint)tailAnchorLba, (uint)mainVdsLba, vdsLengthBytes, (uint)reserveVdsLba, vdsLengthBytes)));

        string volumeSetId = BuildVolumeSetId(when);
        EmitVolumeDescriptorSequence((uint)mainVdsLba, when, udf, volumeSetId, (uint)lvidLba, lvidLengthBytes, regions);
        EmitVolumeDescriptorSequence((uint)reserveVdsLba, when, udf, volumeSetId, (uint)lvidLba, lvidLengthBytes, regions);

        regions.Add(new PlacedRegion(lvidLba, UdfVolumeDescriptors.BuildLogicalVolumeIntegrityDescriptor(
            (uint)lvidLba, when, udf.PartitionLengthBlocks, 0, udf.NumberOfFiles, udf.NumberOfDirectories, udf.NextUniqueId)));
        regions.Add(new PlacedRegion(lvidLba + 1, UdfVolumeDescriptors.BuildTerminatingDescriptor((uint)(lvidLba + 1))));

        // Shared file data (referenced by both the ISO tree and the UDF file entries).
        EmitFileExtents(_root, regions);

        return new DiscImage(totalSectors, regions);
    }

    private void EmitVolumeDescriptorSequence(uint baseLba, DateTimeOffset when, UdfBuildResult udf, string volumeSetId, uint lvidLba, uint lvidLengthBytes, List<PlacedRegion> regions)
    {
        string lvId = _options.VolumeIdentifier;
        regions.Add(new PlacedRegion(baseLba + 0, UdfVolumeDescriptors.BuildPrimaryVolumeDescriptor(baseLba + 0, 0, _options.VolumeIdentifier, volumeSetId, when)));
        regions.Add(new PlacedRegion(baseLba + 1, UdfVolumeDescriptors.BuildImplementationUseVolumeDescriptor(baseLba + 1, 1, lvId)));
        regions.Add(new PlacedRegion(baseLba + 2, UdfVolumeDescriptors.BuildPartitionDescriptor(baseLba + 2, 2, udf.PartitionStartLba, udf.PartitionLengthBlocks)));
        regions.Add(new PlacedRegion(baseLba + 3, UdfVolumeDescriptors.BuildLogicalVolumeDescriptor(baseLba + 3, 3, lvId, 0, lvidLba, lvidLengthBytes)));
        regions.Add(new PlacedRegion(baseLba + 4, UdfVolumeDescriptors.BuildUnallocatedSpaceDescriptor(baseLba + 4, 4)));
        regions.Add(new PlacedRegion(baseLba + 5, UdfVolumeDescriptors.BuildTerminatingDescriptor(baseLba + 5)));
    }

    private string BuildVolumeSetId(DateTimeOffset when)
    {
        // UDF requires the first 16 characters of the volume set identifier to
        // be unique; a hex timestamp satisfies that.
        ulong stamp = (ulong)when.UtcTicks;
        return stamp.ToString("X16", CultureInfo.InvariantCulture) + _options.VolumeIdentifier;
    }

    // ── Shared ISO emission ──────────────────────────────────────

    private static long AssignIsoTables(IsoHierarchy primary, IsoHierarchy? joliet, IsoPlacement placement, long lba)
    {
        placement.PrimaryLPath = lba;
        lba += IsoConstants.SectorsFor(primary.PathTableSizeBytes);
        placement.PrimaryMPath = lba;
        lba += IsoConstants.SectorsFor(primary.PathTableSizeBytes);

        if (joliet is not null)
        {
            placement.JolietLPath = lba;
            lba += IsoConstants.SectorsFor(joliet.PathTableSizeBytes);
            placement.JolietMPath = lba;
            lba += IsoConstants.SectorsFor(joliet.PathTableSizeBytes);
        }

        return lba;
    }

    private void EmitIsoVolume(long totalSectors, IsoHierarchy primary, IsoHierarchy? joliet, IsoPlacement p, DateTimeOffset when, List<PlacedRegion> regions)
    {
        var pvd = new byte[IsoConstants.LogicalSectorSize];
        WriteVolumeDescriptor(pvd, joliet: false, totalSectors, primary, p.PrimaryLPath, p.PrimaryMPath, when);
        regions.Add(new PlacedRegion(p.Pvd, pvd));

        if (joliet is not null)
        {
            var svd = new byte[IsoConstants.LogicalSectorSize];
            WriteVolumeDescriptor(svd, joliet: true, totalSectors, joliet, p.JolietLPath, p.JolietMPath, when);
            regions.Add(new PlacedRegion(p.Svd, svd));
        }

        regions.Add(new PlacedRegion(p.Terminator, BuildTerminator()));

        regions.Add(new PlacedRegion(p.PrimaryLPath, EmitPathTable(primary, bigEndian: false)));
        regions.Add(new PlacedRegion(p.PrimaryMPath, EmitPathTable(primary, bigEndian: true)));

        if (joliet is not null)
        {
            regions.Add(new PlacedRegion(p.JolietLPath, EmitPathTable(joliet, bigEndian: false)));
            regions.Add(new PlacedRegion(p.JolietMPath, EmitPathTable(joliet, bigEndian: true)));
        }

        EmitDirectoryExtents(primary, when, regions);

        if (joliet is not null)
        {
            EmitDirectoryExtents(joliet, when, regions);
        }
    }

    private sealed class IsoPlacement
    {
        public long Pvd { get; set; }
        public long Svd { get; set; } = -1;
        public long Terminator { get; set; }
        public long PrimaryLPath { get; set; }
        public long PrimaryMPath { get; set; }
        public long JolietLPath { get; set; } = -1;
        public long JolietMPath { get; set; } = -1;
    }

    private static long AssignDirectoryExtents(IsoHierarchy hierarchy, long lba)
    {
        foreach (var dir in hierarchy.DirsInPathTableOrder)
        {
            dir.ExtentLba = lba;
            lba += IsoConstants.SectorsFor(dir.ExtentLength);
        }

        return lba;
    }

    private long AssignFileExtents(long lba)
    {
        foreach (var file in EnumerateFiles(_root))
        {
            file.DataLba = lba;
            lba += IsoConstants.SectorsFor(file.Length);
        }

        return lba;
    }

    private static IEnumerable<BuildFile> EnumerateFiles(BuildDirectory dir)
    {
        foreach (var file in dir.Files)
        {
            yield return file;
        }

        foreach (var sub in dir.Directories)
        {
            foreach (var file in EnumerateFiles(sub))
            {
                yield return file;
            }
        }
    }

    // ── Volume descriptors (ECMA-119 §8.4) ───────────────────────

    private void WriteVolumeDescriptor(Span<byte> s, bool joliet, long totalSectors, IsoHierarchy hierarchy, long lPathLba, long mPathLba, DateTimeOffset when)
    {
        s.Clear();

        s[0] = joliet ? IsoConstants.VolumeDescriptorTypeSupplementary : IsoConstants.VolumeDescriptorTypePrimary;
        IsoConstants.StandardIdentifier.CopyTo(s.Slice(1, 5));
        s[6] = IsoConstants.VolumeDescriptorVersion;

        WriteIdentifier(s.Slice(8, 32), _options.SystemIdentifier, joliet);
        WriteIdentifier(s.Slice(40, 32), _options.VolumeIdentifier, joliet);

        IsoEncoding.Write733(s.Slice(80, 8), (uint)totalSectors);

        if (joliet)
        {
            IsoConstants.JolietUcs2Escape.CopyTo(s.Slice(88, 3));
        }

        IsoEncoding.Write723(s.Slice(120, 4), 1); // volume set size
        IsoEncoding.Write723(s.Slice(124, 4), 1); // volume sequence number
        IsoEncoding.Write723(s.Slice(128, 4), (ushort)IsoConstants.LogicalSectorSize);
        IsoEncoding.Write733(s.Slice(132, 8), (uint)hierarchy.PathTableSizeBytes);
        IsoEncoding.Write731(s.Slice(140, 4), (uint)lPathLba);
        IsoEncoding.Write732(s.Slice(148, 4), (uint)mPathLba);

        Span<byte> rootId = stackalloc byte[] { IsoConstants.SelfDirectoryIdentifier };
        WriteDirectoryRecord(s.Slice(156, 34), hierarchy.Root.ExtentLba, hierarchy.Root.ExtentLength, when, IsoConstants.FileFlagDirectory, rootId);

        WriteIdentifier(s.Slice(190, 128), _options.VolumeSetIdentifier, joliet);
        WriteIdentifier(s.Slice(318, 128), _options.PublisherIdentifier, joliet);
        WriteIdentifier(s.Slice(446, 128), _options.DataPreparerIdentifier, joliet);
        WriteIdentifier(s.Slice(574, 128), _options.ApplicationIdentifier, joliet);
        WriteIdentifier(s.Slice(702, 37), null, joliet); // copyright file id
        WriteIdentifier(s.Slice(739, 37), null, joliet); // abstract file id
        WriteIdentifier(s.Slice(776, 37), null, joliet); // bibliographic file id

        IsoEncoding.WriteVolumeDateTime(s.Slice(813, 17), when);
        IsoEncoding.WriteVolumeDateTime(s.Slice(830, 17), when);
        IsoEncoding.WriteUnsetVolumeDateTime(s.Slice(847, 17));
        IsoEncoding.WriteUnsetVolumeDateTime(s.Slice(864, 17));

        s[881] = 1; // File structure version
    }

    private static void WriteIdentifier(Span<byte> field, string? value, bool joliet)
    {
        if (joliet)
        {
            IsoEncoding.WriteJolietString(field, value);
        }
        else
        {
            IsoEncoding.WriteAString(field, value);
        }
    }

    private static byte[] BuildTerminator()
    {
        var sector = new byte[IsoConstants.LogicalSectorSize];
        sector[0] = IsoConstants.VolumeDescriptorTypeTerminator;
        IsoConstants.StandardIdentifier.CopyTo(sector.AsSpan(1, 5));
        sector[6] = IsoConstants.VolumeDescriptorVersion;
        return sector;
    }

    // ── Path tables (ECMA-119 §9.4) ──────────────────────────────

    private static byte[] EmitPathTable(IsoHierarchy hierarchy, bool bigEndian)
    {
        long size = IsoConstants.RoundUpToSector(hierarchy.PathTableSizeBytes);
        var buf = new byte[size];
        int offset = 0;

        foreach (var dir in hierarchy.DirsInPathTableOrder)
        {
            int idLen = dir.Identifier.Length;
            buf[offset] = (byte)idLen;
            buf[offset + 1] = 0; // extended attribute record length

            if (bigEndian)
            {
                BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(offset + 2, 4), (uint)dir.ExtentLba);
                BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(offset + 6, 2), (ushort)dir.ParentNumber);
            }
            else
            {
                BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(offset + 2, 4), (uint)dir.ExtentLba);
                BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(offset + 6, 2), (ushort)dir.ParentNumber);
            }

            dir.Identifier.CopyTo(buf.AsSpan(offset + 8, idLen));
            offset += IsoHierarchy.PathTableRecordLength(idLen);
        }

        return buf;
    }

    // ── Directory extents (ECMA-119 §9.1, §6.8.1) ────────────────

    private static void EmitDirectoryExtents(IsoHierarchy hierarchy, DateTimeOffset when, List<PlacedRegion> regions)
    {
        foreach (var dir in hierarchy.DirsInPathTableOrder)
        {
            regions.Add(new PlacedRegion(dir.ExtentLba, EmitDirectoryExtent(dir, when)));
        }
    }

    private static byte[] EmitDirectoryExtent(IsoDir dir, DateTimeOffset when)
    {
        var buf = new byte[dir.ExtentLength];
        long offset = 0;

        foreach (var entry in dir.Entries)
        {
            int len = entry.RecordLength;
            int posInSector = (int)(offset % IsoConstants.LogicalSectorSize);

            if (posInSector + len > IsoConstants.LogicalSectorSize)
            {
                offset += IsoConstants.LogicalSectorSize - posInSector;
            }

            long extentLba;
            long dataLength;

            if (entry.File is not null)
            {
                extentLba = entry.File.DataLba;
                dataLength = entry.File.Length;
            }
            else
            {
                extentLba = entry.Dir!.ExtentLba;
                dataLength = entry.Dir!.ExtentLength;
            }

            WriteDirectoryRecord(buf.AsSpan((int)offset, len), extentLba, dataLength, when, entry.FileFlags, entry.Identifier);
            offset += len;
        }

        return buf;
    }

    private static int WriteDirectoryRecord(Span<byte> dst, long extentLba, long dataLength, DateTimeOffset when, byte fileFlags, ReadOnlySpan<byte> identifier)
    {
        int idLen = identifier.Length;
        int recLen = IsoEntry.RecordLengthFor(idLen);

        dst.Slice(0, recLen).Clear();
        dst[0] = (byte)recLen;
        dst[1] = 0; // extended attribute record length
        IsoEncoding.Write733(dst.Slice(2, 8), (uint)extentLba);
        IsoEncoding.Write733(dst.Slice(10, 8), (uint)dataLength);
        IsoEncoding.WriteDirectoryDateTime(dst.Slice(18, 7), when);
        dst[25] = fileFlags;
        dst[26] = 0; // file unit size
        dst[27] = 0; // interleave gap size
        IsoEncoding.Write723(dst.Slice(28, 4), 1); // volume sequence number
        dst[32] = (byte)idLen;
        identifier.CopyTo(dst.Slice(33, idLen));

        return recLen;
    }

    // ── File data ────────────────────────────────────────────────

    private static void EmitFileExtents(BuildDirectory root, List<PlacedRegion> regions)
    {
        foreach (var file in EnumerateFiles(root))
        {
            if (file.Length > 0)
            {
                regions.Add(new PlacedRegion(file.DataLba, file.Content));
            }
        }
    }
}
