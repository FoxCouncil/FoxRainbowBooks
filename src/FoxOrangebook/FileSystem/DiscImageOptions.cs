namespace FoxOrangebook.FileSystem;

/// <summary>
/// Options controlling how a <see cref="DiscImageBuilder"/> lays out the
/// volume: which filesystems to emit and the descriptive identifiers written
/// into the volume descriptors.
/// </summary>
public sealed record DiscImageOptions
{
    /// <summary>
    /// Volume identifier (the disc label). ISO 9660 d-characters; mapped to
    /// uppercase A–Z, 0–9 and '_' in the primary descriptor, and to the raw
    /// string in the Joliet descriptor. Limited to 32 bytes (16 UCS-2 chars
    /// for Joliet). Defaults to "FOXDISC".
    /// </summary>
    public string VolumeIdentifier { get; init; } = "FOXDISC";

    /// <summary>System identifier (the system that can act on sectors 0–15). Optional.</summary>
    public string? SystemIdentifier { get; init; }

    /// <summary>Volume set identifier. Optional.</summary>
    public string? VolumeSetIdentifier { get; init; }

    /// <summary>Publisher identifier. Optional.</summary>
    public string? PublisherIdentifier { get; init; }

    /// <summary>Data preparer identifier. Optional.</summary>
    public string? DataPreparerIdentifier { get; init; }

    /// <summary>Application identifier. Optional.</summary>
    public string? ApplicationIdentifier { get; init; }

    /// <summary>
    /// When true, emit a Joliet supplementary volume descriptor so the disc
    /// carries Unicode long filenames in addition to the primary ISO 9660
    /// hierarchy. Recommended for Windows compatibility. Default true.
    /// </summary>
    public bool EnableJoliet { get; init; } = true;

    /// <summary>
    /// When true, emit a UDF Bridge (ISO 9660 + UDF over the same file data),
    /// required for real DVD use, files larger than 4 GiB, and DVD-Video.
    /// Default true.
    /// </summary>
    public bool EnableUdf { get; init; } = true;

    /// <summary>
    /// Timestamp recorded as the volume creation/modification time and applied
    /// to every directory record. Defaults to the moment the image is built.
    /// </summary>
    public DateTimeOffset? Timestamp { get; init; }
}
