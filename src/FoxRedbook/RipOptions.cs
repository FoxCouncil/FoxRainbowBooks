namespace FoxRedbook;

/// <summary>
/// Options controlling the verification engine.
/// </summary>
public sealed record RipOptions
{
    /// <summary>
    /// Maximum number of re-reads per sector before the engine gives up and
    /// returns best-effort data. Default is 20.
    /// </summary>
    public int MaxReReads { get; init; } = 20;
}
