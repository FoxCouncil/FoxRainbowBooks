using System.Text;

namespace FoxOrangebook.FileSystem;

/// <summary>
/// One child reference inside a directory extent: the self ("."), parent
/// (".."), a subdirectory, or a file. Carries the on-disc identifier bytes
/// and a reference to whatever the record points at, so the emitter can read
/// the assigned LBA/length after layout.
/// </summary>
internal sealed class IsoEntry
{
    public required byte[] Identifier { get; init; }
    public required byte FileFlags { get; init; }

    /// <summary>Target directory for ".", "..", and subdirectory entries.</summary>
    public IsoDir? Dir { get; init; }

    /// <summary>Target file for file entries.</summary>
    public BuildFile? File { get; init; }

    public int RecordLength => RecordLengthFor(Identifier.Length);

    /// <summary>
    /// Directory record length: 33 fixed bytes + identifier, padded so the
    /// total is even (ECMA-119 §9.1).
    /// </summary>
    public static int RecordLengthFor(int identifierLength)
    {
        int len = 33 + identifierLength;

        if ((len & 1) == 1)
        {
            len++;
        }

        return len;
    }
}

/// <summary>
/// A directory as materialized for a single encoding (primary ISO 9660 or
/// Joliet). Holds the ordered records, the path-table number, and the extent
/// location/length assigned during layout.
/// </summary>
internal sealed class IsoDir
{
    public required BuildDirectory Source { get; init; }

    /// <summary>This directory's identifier in its parent (single 0x00 byte for the root).</summary>
    public required byte[] Identifier { get; set; }

    public IsoDir? Parent { get; init; }
    public int Level { get; init; }

    /// <summary>1-based path table number (root = 1), assigned during layout.</summary>
    public int Number { get; set; }

    /// <summary>First logical block of this directory's record extent.</summary>
    public long ExtentLba { get; set; }

    /// <summary>Length of the directory record extent in bytes (a whole number of sectors).</summary>
    public long ExtentLength { get; set; }

    public List<IsoEntry> Entries { get; } = new();

    public int ParentNumber => Parent?.Number ?? 1;
}

/// <summary>
/// Materializes the build tree into one ISO 9660 directory hierarchy under a
/// chosen encoding. The primary hierarchy uses uppercase d-characters with a
/// ";1" version suffix; the Joliet hierarchy uses UCS-2 big-endian names up to
/// 64 characters. Both hierarchies reference the same shared file-data extents.
/// </summary>
internal sealed class IsoHierarchy
{
    public bool Joliet { get; private init; }
    public IsoDir Root { get; private init; } = null!;
    public IReadOnlyList<IsoDir> DirsInPathTableOrder { get; private init; } = null!;

    /// <summary>Total size of one path table (L or M) in bytes, before sector padding.</summary>
    public long PathTableSizeBytes { get; private init; }

    public static IsoHierarchy Build(BuildDirectory root, bool joliet)
    {
        var rootDir = new IsoDir { Source = root, Identifier = new byte[] { IsoConstants.SelfDirectoryIdentifier }, Parent = null, Level = 1 };

        var all = new List<IsoDir>();
        var map = new Dictionary<BuildDirectory, IsoDir>();
        CreateDirs(root, rootDir, all, map);

        foreach (var dir in all)
        {
            PopulateEntries(dir, joliet, map);
        }

        var ordered = AssignPathTableNumbers(all);
        long pathTableSize = ComputeExtentsAndPathTable(ordered);

        return new IsoHierarchy
        {
            Joliet = joliet,
            Root = rootDir,
            DirsInPathTableOrder = ordered,
            PathTableSizeBytes = pathTableSize,
        };
    }

    private static void CreateDirs(BuildDirectory source, IsoDir dir, List<IsoDir> all, Dictionary<BuildDirectory, IsoDir> map)
    {
        all.Add(dir);
        map[source] = dir;

        foreach (var childSource in source.Directories)
        {
            var child = new IsoDir
            {
                Source = childSource,
                Identifier = Array.Empty<byte>(),
                Parent = dir,
                Level = dir.Level + 1,
            };

            CreateDirs(childSource, child, all, map);
        }
    }

    private static void PopulateEntries(IsoDir dir, bool joliet, Dictionary<BuildDirectory, IsoDir> map)
    {
        // "." and ".." always come first, referencing this dir and its parent.
        dir.Entries.Add(new IsoEntry
        {
            Identifier = new byte[] { IsoConstants.SelfDirectoryIdentifier },
            FileFlags = IsoConstants.FileFlagDirectory,
            Dir = dir,
        });

        dir.Entries.Add(new IsoEntry
        {
            Identifier = new byte[] { IsoConstants.ParentDirectoryIdentifier },
            FileFlags = IsoConstants.FileFlagDirectory,
            Dir = dir.Parent ?? dir,
        });

        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var children = new List<IsoEntry>();

        foreach (var childSource in dir.Source.Directories)
        {
            string id = MakeUniqueDirectoryName(childSource.Name, joliet, used);
            var childDir = map[childSource];
            childDir.Identifier = Encode(id, joliet);

            children.Add(new IsoEntry
            {
                Identifier = childDir.Identifier,
                FileFlags = IsoConstants.FileFlagDirectory,
                Dir = childDir,
            });
        }

        foreach (var file in dir.Source.Files)
        {
            string id = MakeUniqueFileName(file.Name, joliet, used);

            children.Add(new IsoEntry
            {
                Identifier = Encode(id, joliet),
                FileFlags = 0,
                File = file,
            });
        }

        children.Sort((a, b) => CompareIdentifiers(a.Identifier, b.Identifier, joliet));
        dir.Entries.AddRange(children);
    }

    // ── Path table numbering (ECMA-119 §6.9.1) ───────────────────

    private static List<IsoDir> AssignPathTableNumbers(List<IsoDir> all)
    {
        int maxLevel = 0;

        foreach (var dir in all)
        {
            if (dir.Level > maxLevel)
            {
                maxLevel = dir.Level;
            }
        }

        var ordered = new List<IsoDir>();
        int number = 0;

        for (int level = 1; level <= maxLevel; level++)
        {
            var atLevel = new List<IsoDir>();

            foreach (var dir in all)
            {
                if (dir.Level == level)
                {
                    atLevel.Add(dir);
                }
            }

            atLevel.Sort((a, b) =>
            {
                int byParent = a.ParentNumber.CompareTo(b.ParentNumber);
                return byParent != 0 ? byParent : CompareIdentifiers(a.Identifier, b.Identifier, joliet: false);
            });

            foreach (var dir in atLevel)
            {
                dir.Number = ++number;
                ordered.Add(dir);
            }
        }

        return ordered;
    }

    private static long ComputeExtentsAndPathTable(List<IsoDir> ordered)
    {
        long pathTableSize = 0;

        foreach (var dir in ordered)
        {
            dir.ExtentLength = ComputeExtentLength(dir.Entries);
            pathTableSize += PathTableRecordLength(dir.Identifier.Length);
        }

        return pathTableSize;
    }

    private static long ComputeExtentLength(List<IsoEntry> entries)
    {
        long offset = 0;

        foreach (var entry in entries)
        {
            int len = entry.RecordLength;
            int posInSector = (int)(offset % IsoConstants.LogicalSectorSize);

            if (posInSector + len > IsoConstants.LogicalSectorSize)
            {
                offset += IsoConstants.LogicalSectorSize - posInSector;
            }

            offset += len;
        }

        return IsoConstants.RoundUpToSector(offset);
    }

    /// <summary>Path table record length: 8 fixed bytes + identifier, padded to even (ECMA-119 §9.4).</summary>
    public static int PathTableRecordLength(int identifierLength)
    {
        int len = 8 + identifierLength;

        if ((len & 1) == 1)
        {
            len++;
        }

        return len;
    }

    // ── Identifier encoding and ordering ─────────────────────────

    private static byte[] Encode(string identifier, bool joliet)
    {
        if (joliet)
        {
            return Encoding.BigEndianUnicode.GetBytes(identifier);
        }

        var bytes = new byte[identifier.Length];

        for (int i = 0; i < identifier.Length; i++)
        {
            bytes[i] = (byte)identifier[i];
        }

        return bytes;
    }

    private static int CompareIdentifiers(byte[] a, byte[] b, bool joliet)
    {
        int n = Math.Max(a.Length, b.Length);

        for (int i = 0; i < n; i++)
        {
            byte ba = i < a.Length ? a[i] : PadByte(i, joliet);
            byte bb = i < b.Length ? b[i] : PadByte(i, joliet);

            if (ba != bb)
            {
                return ba - bb;
            }
        }

        return 0;
    }

    private static byte PadByte(int index, bool joliet)
    {
        // Pad with the space character: a lone 0x20 for primary d-characters,
        // and the 0x00 0x20 UCS-2 unit for Joliet.
        if (!joliet)
        {
            return 0x20;
        }

        return (index & 1) == 0 ? (byte)0x00 : (byte)0x20;
    }

    // ── Name mangling and de-duplication ─────────────────────────

    private static string MakeUniqueDirectoryName(string name, bool joliet, HashSet<string> used)
    {
        string candidate = joliet ? JolietName(name, isFile: false) : PrimaryDirectoryName(name);
        return EnsureUnique(candidate, joliet, isFile: false, used);
    }

    private static string MakeUniqueFileName(string name, bool joliet, HashSet<string> used)
    {
        string candidate = joliet ? JolietName(name, isFile: true) : PrimaryFileName(name);
        return EnsureUnique(candidate, joliet, isFile: true, used);
    }

    private static string EnsureUnique(string candidate, bool joliet, bool isFile, HashSet<string> used)
    {
        if (used.Add(candidate))
        {
            return candidate;
        }

        for (int k = 1; k < 1_000_000; k++)
        {
            string alt = AppendSuffix(candidate, k, joliet, isFile);

            if (used.Add(alt))
            {
                return alt;
            }
        }

        throw new InvalidOperationException($"Unable to produce a unique identifier for '{candidate}'.");
    }

    private static string AppendSuffix(string candidate, int k, bool joliet, bool isFile)
    {
        string suffix = "~" + k.ToString(System.Globalization.CultureInfo.InvariantCulture);
        int maxLen = joliet ? IsoConstants.JolietMaxNameChars : 30;

        // Split off the version (";1") for files so the suffix lands on the name.
        string version = "";
        string body = candidate;

        if (isFile && candidate.EndsWith(";1", StringComparison.Ordinal))
        {
            version = ";1";
            body = candidate[..^2];
        }

        int dot = isFile ? body.LastIndexOf('.') : -1;
        string stem = dot > 0 ? body[..dot] : body;
        string ext = dot > 0 ? body[dot..] : "";

        int budget = maxLen - version.Length - ext.Length - suffix.Length;

        if (budget < 1)
        {
            budget = 1;
        }

        if (stem.Length > budget)
        {
            stem = stem[..budget];
        }

        return stem + suffix + ext + version;
    }

    private static string PrimaryDirectoryName(string name)
    {
        string mangled = MangleDChars(name);
        return mangled.Length > 31 ? mangled[..31] : mangled;
    }

    private static string PrimaryFileName(string name)
    {
        int dot = name.LastIndexOf('.');
        string stem;
        string ext;

        if (dot > 0 && dot < name.Length - 1)
        {
            stem = MangleDChars(name[..dot]);
            ext = MangleDChars(name[(dot + 1)..]);
        }
        else
        {
            stem = MangleDChars(name);
            ext = "";
        }

        if (ext.Length > 0)
        {
            if (ext.Length > 12)
            {
                ext = ext[..12];
            }

            int maxStem = 30 - 1 - ext.Length;

            if (maxStem < 1)
            {
                maxStem = 1;
            }

            if (stem.Length > maxStem)
            {
                stem = stem[..maxStem];
            }

            return stem + "." + ext + ";1";
        }

        if (stem.Length > 30)
        {
            stem = stem[..30];
        }

        return stem + ";1";
    }

    private static string JolietName(string name, bool isFile)
    {
        if (!isFile)
        {
            return name.Length > IsoConstants.JolietMaxNameChars ? name[..IsoConstants.JolietMaxNameChars] : name;
        }

        // Reserve two characters for the ";1" version suffix.
        int max = IsoConstants.JolietMaxNameChars - 2;
        string body = name.Length > max ? name[..max] : name;
        return body + ";1";
    }

    private static string MangleDChars(string s)
    {
        var sb = new StringBuilder(s.Length);

        foreach (char c in s)
        {
            char u = char.ToUpperInvariant(c);

            if ((u is >= 'A' and <= 'Z') || (u is >= '0' and <= '9') || u == '_')
            {
                sb.Append(u);
            }
            else
            {
                sb.Append('_');
            }
        }

        if (sb.Length == 0)
        {
            sb.Append('_');
        }

        return sb.ToString();
    }
}
