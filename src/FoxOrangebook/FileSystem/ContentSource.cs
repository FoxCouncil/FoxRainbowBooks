namespace FoxOrangebook.FileSystem;

/// <summary>
/// A lazily-openable source of file content. File data is never buffered in
/// full during layout — only the length is needed to assign extents — so disc
/// images far larger than memory (e.g. a 4.7 GB DVD) can be produced.
/// </summary>
internal interface IContentSource
{
    /// <summary>Length of the content in bytes.</summary>
    long Length { get; }

    /// <summary>Opens a fresh stream positioned at the start of the content.</summary>
    Stream Open();
}

/// <summary>Content backed by a file on disk.</summary>
internal sealed class FileContentSource : IContentSource
{
    private readonly string _path;

    public FileContentSource(string path)
    {
        _path = path;
        Length = new FileInfo(path).Length;
    }

    public long Length { get; }

    public Stream Open()
    {
        return new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.SequentialScan);
    }
}

/// <summary>Content backed by an in-memory byte array.</summary>
internal sealed class BytesContentSource : IContentSource
{
    private readonly byte[] _bytes;

    public BytesContentSource(byte[] bytes)
    {
        _bytes = bytes;
    }

    public long Length => _bytes.Length;

    public Stream Open()
    {
        return new MemoryStream(_bytes, writable: false);
    }
}
