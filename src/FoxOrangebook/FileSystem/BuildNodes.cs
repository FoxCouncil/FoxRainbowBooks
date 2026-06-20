namespace FoxOrangebook.FileSystem;

/// <summary>
/// A file in the build tree. Carries the caller-supplied display name (used
/// for Joliet/UDF) and a lazy content source. The on-disc file LBA and the
/// ISO-mangled identifier are assigned during layout.
/// </summary>
internal sealed class BuildFile
{
    public required string Name { get; init; }
    public required IContentSource Content { get; init; }

    /// <summary>Length of the file content in bytes.</summary>
    public long Length => Content.Length;

    /// <summary>First logical block of the file's data extent (assigned during layout).</summary>
    public long DataLba { get; set; }
}

/// <summary>
/// A directory in the build tree. Children are kept in insertion order and
/// sorted into the spec-mandated order only when a hierarchy is materialized.
/// </summary>
internal sealed class BuildDirectory
{
    public required string Name { get; init; }
    public BuildDirectory? Parent { get; init; }

    public List<BuildDirectory> Directories { get; } = new();
    public List<BuildFile> Files { get; } = new();

    public bool IsRoot => Parent is null;

    /// <summary>
    /// Finds an immediate child directory by name (case-insensitive), or null.
    /// </summary>
    public BuildDirectory? FindDirectory(string name)
    {
        foreach (var dir in Directories)
        {
            if (string.Equals(dir.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return dir;
            }
        }

        return null;
    }

    /// <summary>
    /// Returns the immediate child directory with the given name, creating it
    /// if necessary.
    /// </summary>
    public BuildDirectory GetOrAddDirectory(string name)
    {
        var existing = FindDirectory(name);

        if (existing is not null)
        {
            return existing;
        }

        var dir = new BuildDirectory { Name = name, Parent = this };
        Directories.Add(dir);
        return dir;
    }
}
