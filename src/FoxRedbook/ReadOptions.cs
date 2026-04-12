namespace FoxRedbook;

/// <summary>
/// Flags controlling what data is requested from the drive alongside audio sectors.
/// Maps to fields in the READ CD (0xBE) CDB.
/// </summary>
[Flags]
public enum ReadOptions
{
    /// <summary>
    /// Read audio data only (2,352 bytes per sector).
    /// </summary>
    None = 0,

    /// <summary>
    /// Request C2 error pointers (294 bytes per sector). Each bit indicates an
    /// uncorrectable byte-level error after the drive's internal ECC processing.
    /// </summary>
    C2ErrorPointers = 1 << 0,

    /// <summary>
    /// Request raw subchannel data (96 bytes per sector, subchannels P–W interleaved).
    /// </summary>
    SubchannelData = 1 << 1,
}
