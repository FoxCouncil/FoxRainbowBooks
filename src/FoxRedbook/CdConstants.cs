namespace FoxRedbook;

/// <summary>
/// Constants defined by the Red Book (IEC 60908) CD-DA specification.
/// </summary>
public static class CdConstants
{
    /// <summary>
    /// Size of a single CD-DA audio sector in bytes (588 stereo samples × 4 bytes per sample).
    /// </summary>
    public const int SectorSize = 2352;

    /// <summary>
    /// Size of C2 error pointer data per sector in bytes (one bit per audio byte = 2352 / 8 = 294).
    /// A set bit indicates an uncorrectable error at that byte position after ECC processing.
    /// </summary>
    public const int C2ErrorPointerSize = 294;

    /// <summary>
    /// Size of raw subchannel data per sector in bytes (8 subchannels × 12 bytes each).
    /// </summary>
    public const int SubchannelSize = 96;

    /// <summary>
    /// Number of sectors per second of audio (1/75 s per sector).
    /// </summary>
    public const int SectorsPerSecond = 75;

    /// <summary>
    /// CD-DA sample rate in Hz.
    /// </summary>
    public const int SampleRate = 44100;

    /// <summary>
    /// Bits per sample per channel.
    /// </summary>
    public const int BitsPerSample = 16;

    /// <summary>
    /// Number of audio channels (stereo).
    /// </summary>
    public const int Channels = 2;

    /// <summary>
    /// Bytes per sample frame (left + right = 4 bytes).
    /// </summary>
    public const int BytesPerSampleFrame = BitsPerSample / 8 * Channels;

    /// <summary>
    /// Number of stereo sample frames per sector (2352 / 4 = 588).
    /// </summary>
    public const int SampleFramesPerSector = SectorSize / BytesPerSampleFrame;

    /// <summary>
    /// First valid track number on a CD.
    /// </summary>
    public const int MinTrackNumber = 1;

    /// <summary>
    /// Maximum valid track number on a CD (Red Book limit).
    /// </summary>
    public const int MaxTrackNumber = 99;

    /// <summary>
    /// Track number used to represent the lead-out area in the TOC.
    /// </summary>
    public const int LeadOutTrackNumber = 0xAA;

    /// <summary>
    /// Sector offset between MSF and LBA addressing (150 sectors = 2 seconds of audio).
    /// </summary>
    /// <remarks>
    /// MSF (Minutes:Seconds:Frames) addressing counts from the start of the disc including
    /// the 2-second pregap before track 1. LBA addressing starts at sector 0 which corresponds
    /// to MSF 00:02:00. To convert: <c>LBA = (M × 60 + S) × 75 + F − 150</c>.
    /// To convert back: <c>MSF_total_frames = LBA + 150</c>, then decompose into M:S:F.
    /// </remarks>
    public const int MsfLbaOffset = 150;

    /// <summary>
    /// Computes the minimum buffer size in bytes required for a
    /// <see cref="IOpticalDrive.ReadSectorsAsync"/> call with the given flags and sector count.
    /// </summary>
    /// <param name="flags">The read options controlling what data is requested per sector.</param>
    /// <param name="sectorCount">The number of sectors to be read.</param>
    /// <returns>Required buffer size in bytes.</returns>
    public static int GetReadBufferSize(ReadOptions flags, int sectorCount)
    {
        int perSector = SectorSize;

        if ((flags & ReadOptions.C2ErrorPointers) != 0)
        {
            perSector += C2ErrorPointerSize;
        }

        if ((flags & ReadOptions.SubchannelData) != 0)
        {
            perSector += SubchannelSize;
        }

        return perSector * sectorCount;
    }
}
