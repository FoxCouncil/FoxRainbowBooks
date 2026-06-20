using System.Buffers.Binary;

namespace FoxOrangebook;

/// <summary>
/// Pure functions for building SCSI CDBs and parsing responses needed
/// for CD-R/CD-RW burning. All commands target Disc-At-Once audio burning
/// per the Orange Book / MMC-6 spec.
/// </summary>
internal static class BurnCommands
{
    // ── Opcodes ──────────────────────────────────────────────

    internal const byte OpGetConfiguration = 0x46;
    internal const byte OpReadDiscInformation = 0x51;
    internal const byte OpModeSense10 = 0x5A;
    internal const byte OpModeSelect10 = 0x55;
    internal const byte OpSendCueSheet = 0x5D;
    internal const byte OpWrite10 = 0x2A;
    internal const byte OpCloseTrackSession = 0x5B;
    internal const byte OpBlank = 0xA1;
    internal const byte OpSendOpc = 0x54;
    internal const byte OpTestUnitReady = 0x00;
    internal const byte OpSynchronizeCache = 0x35;
    internal const byte OpFormatUnit = 0x04;
    internal const byte OpReadFormatCapacities = 0x23;

    // ── Feature numbers ──────────────────────────────────────

    internal const ushort FeatureCdMastering = 0x002F;
    internal const ushort FeatureCdTrackAtOnce = 0x002D;

    // ── Current media profiles (GET CONFIGURATION header) ────────

    internal const ushort ProfileCdR = 0x0009;
    internal const ushort ProfileCdRw = 0x000A;
    internal const ushort ProfileDvdPlusRw = 0x001A;

    // ── GET CONFIGURATION (0x46) ─────────────────────────────

    internal static void BuildGetConfiguration(Span<byte> cdb, ushort featureNumber, int allocationLength)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("GET CONFIGURATION CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpGetConfiguration;
        cdb[1] = 0x02; // RT = 2 (one feature only)
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(2, 2), featureNumber);
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)allocationLength);
    }

    internal static bool ParseGetConfigurationHasFeature(ReadOnlySpan<byte> response, ushort featureNumber)
    {
        if (response.Length < 8)
        {
            return false;
        }

        int dataLength = (int)BinaryPrimitives.ReadUInt32BigEndian(response);

        if (dataLength < 4)
        {
            return false;
        }

        // Feature header starts at byte 8. Check if the feature code matches.
        if (response.Length < 12)
        {
            return false;
        }

        ushort code = BinaryPrimitives.ReadUInt16BigEndian(response.Slice(8, 2));
        return code == featureNumber;
    }

    /// <summary>
    /// Builds a GET CONFIGURATION (RT=1) request that returns the feature
    /// header, whose Current Profile field identifies the loaded media.
    /// </summary>
    internal static void BuildGetConfigurationHeader(Span<byte> cdb, int allocationLength)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("GET CONFIGURATION CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpGetConfiguration;
        cdb[1] = 0x01; // RT = 1 (current features)
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)allocationLength);
    }

    /// <summary>Reads the Current Profile field (bytes 6–7) from a GET CONFIGURATION response.</summary>
    internal static ushort ParseCurrentProfile(ReadOnlySpan<byte> response)
    {
        return response.Length < 8 ? (ushort)0 : BinaryPrimitives.ReadUInt16BigEndian(response.Slice(6, 2));
    }

    // ── READ DISC INFORMATION (0x51) ─────────────────────────

    internal const int ReadDiscInfoResponseLength = 34;

    internal static void BuildReadDiscInformation(Span<byte> cdb)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("READ DISC INFORMATION CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpReadDiscInformation;
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), ReadDiscInfoResponseLength);
    }

    internal static DiscInfo ParseReadDiscInformation(ReadOnlySpan<byte> response)
    {
        if (response.Length < 34)
        {
            throw new ArgumentException("READ DISC INFORMATION response too short.", nameof(response));
        }

        byte statusByte = response[2];

        return new DiscInfo
        {
            Status = (DiscStatus)(statusByte & 0x03),
            Erasable = (statusByte & 0x10) != 0,
            FirstTrack = response[3],
            LastTrack = response[6],
        };
    }

    // ── MODE SENSE / MODE SELECT — Write Parameters page 0x05 ─

    internal const byte WriteParametersPageCode = 0x05;
    internal const int WriteParametersPageLength = 0x32;

    internal static void BuildModeSense10(Span<byte> cdb, byte pageCode, int allocationLength)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("MODE SENSE CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpModeSense10;
        cdb[2] = pageCode;
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)allocationLength);
    }

    internal static void BuildModeSelect10(Span<byte> cdb, int parameterListLength)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("MODE SELECT CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpModeSelect10;
        cdb[1] = 0x10; // PF = 1 (page format)
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)parameterListLength);
    }

    /// <summary>
    /// Builds the MODE SELECT parameter list with Write Parameters page 0x05
    /// configured for DAO audio burning.
    /// </summary>
    /// <param name="buffer">Output buffer for the mode parameter header + page. Must be at least 60 bytes.</param>
    /// <param name="testWrite">If true, enables simulation mode (no actual burn).</param>
    /// <param name="bufferUnderrunProtection">If true, enables BUFE.</param>
    /// <returns>Number of bytes written to <paramref name="buffer"/>.</returns>
    internal static int BuildWriteParametersPage(Span<byte> buffer, bool testWrite, bool bufferUnderrunProtection)
    {
        // 8-byte mode parameter header + 2-byte page header + 50-byte page body = 60 bytes
        const int totalLength = 8 + 2 + WriteParametersPageLength;

        if (buffer.Length < totalLength)
        {
            throw new ArgumentException($"Buffer must be at least {totalLength} bytes.", nameof(buffer));
        }

        buffer.Slice(0, totalLength).Clear();

        // Mode parameter header (8 bytes for MODE SELECT 10)
        // Leave mostly zeroed; byte 1 is mode data length (not set for MODE SELECT).

        // Page header at offset 8
        int page = 8;
        buffer[page] = WriteParametersPageCode;
        buffer[page + 1] = WriteParametersPageLength;

        // Page body at offset 10
        byte writeType = 0x02; // SAO/DAO
        byte flags = writeType;

        if (testWrite)
        {
            flags |= 0x10;
        }

        if (bufferUnderrunProtection)
        {
            flags |= 0x40;
        }

        buffer[page + 2] = flags;
        buffer[page + 3] = 0x00; // Track mode: audio, 2-channel, no pre-emphasis
        buffer[page + 4] = 0x00; // Data block type: raw 2352
        buffer[page + 14] = 0x00; // Session format: CD-DA or CD-ROM

        return totalLength;
    }

    /// <summary>
    /// Builds the MODE SELECT parameter list with Write Parameters page 0x05
    /// configured for Track-At-Once Mode 1 (2048-byte) data burning.
    /// </summary>
    /// <param name="buffer">Output buffer for the mode parameter header + page. Must be at least 60 bytes.</param>
    /// <param name="testWrite">If true, enables simulation mode (no actual burn).</param>
    /// <param name="bufferUnderrunProtection">If true, enables BUFE.</param>
    /// <returns>Number of bytes written to <paramref name="buffer"/>.</returns>
    internal static int BuildDataWriteParametersPage(Span<byte> buffer, bool testWrite, bool bufferUnderrunProtection)
    {
        const int totalLength = 8 + 2 + WriteParametersPageLength;

        if (buffer.Length < totalLength)
        {
            throw new ArgumentException($"Buffer must be at least {totalLength} bytes.", nameof(buffer));
        }

        buffer.Slice(0, totalLength).Clear();

        int page = 8;
        buffer[page] = WriteParametersPageCode;
        buffer[page + 1] = WriteParametersPageLength;

        byte writeType = 0x01; // Track-At-Once
        byte flags = writeType;

        if (testWrite)
        {
            flags |= 0x10;
        }

        if (bufferUnderrunProtection)
        {
            flags |= 0x40;
        }

        buffer[page + 2] = flags;
        buffer[page + 3] = 0x04; // Track mode: data track (control bit 2 set)
        buffer[page + 4] = 0x08; // Data block type: Mode 1, 2048 user bytes
        buffer[page + 14] = 0x00; // Session format: CD-ROM data

        return totalLength;
    }

    // ── SEND CUE SHEET (0x5D) ────────────────────────────────

    internal const int CueSheetEntrySize = 8;

    internal static void BuildSendCueSheet(Span<byte> cdb, int cueSheetBytes)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("SEND CUE SHEET CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpSendCueSheet;
        // bytes 6-8: cue sheet size (24-bit big-endian)
        cdb[6] = (byte)((cueSheetBytes >> 16) & 0xFF);
        cdb[7] = (byte)((cueSheetBytes >> 8) & 0xFF);
        cdb[8] = (byte)(cueSheetBytes & 0xFF);
    }

    /// <summary>
    /// Serializes an array of cue sheet entries into a contiguous byte buffer.
    /// </summary>
    internal static byte[] SerializeCueSheet(IReadOnlyList<CueSheetEntry> entries)
    {
        byte[] data = new byte[entries.Count * CueSheetEntrySize];

        for (int i = 0; i < entries.Count; i++)
        {
            entries[i].WriteTo(data.AsSpan(i * CueSheetEntrySize, CueSheetEntrySize));
        }

        return data;
    }

    // ── WRITE (10) (0x2A) ────────────────────────────────────

    internal static void BuildWrite10(Span<byte> cdb, uint lba, ushort sectorCount)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("WRITE (10) CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpWrite10;
        BinaryPrimitives.WriteUInt32BigEndian(cdb.Slice(2, 4), lba);
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), sectorCount);
    }

    // ── CLOSE TRACK/SESSION (0x5B) ───────────────────────────

    internal static void BuildCloseSession(Span<byte> cdb, bool immediate)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("CLOSE TRACK/SESSION CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpCloseTrackSession;

        if (immediate)
        {
            cdb[1] = 0x01;
        }

        cdb[2] = 0x02; // Close function: close session
    }

    // ── BLANK (0xA1) ─────────────────────────────────────────

    internal static void BuildBlank(Span<byte> cdb, bool minimal, bool immediate)
    {
        if (cdb.Length < 12)
        {
            throw new ArgumentException("BLANK CDB must be at least 12 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpBlank;

        byte flags = 0;

        if (immediate)
        {
            flags |= 0x10;
        }

        if (minimal)
        {
            flags |= 0x01;
        }

        cdb[1] = flags;
    }

    // ── READ FORMAT CAPACITIES (0x23) ────────────────────────

    internal static void BuildReadFormatCapacities(Span<byte> cdb, int allocationLength)
    {
        if (cdb.Length < 12)
        {
            throw new ArgumentException("READ FORMAT CAPACITIES CDB must be at least 12 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpReadFormatCapacities;
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)allocationLength);
    }

    /// <summary>DVD+RW background-format descriptor type.</summary>
    internal const byte FormatTypeDvdPlusRwBackground = 0x26;

    /// <summary>
    /// Parses a READ FORMAT CAPACITIES response: whether the current media is
    /// already formatted, and the block count of the preferred (background)
    /// formattable descriptor for re-formatting.
    /// </summary>
    internal static (bool Formatted, uint FormatBlocks) ParseFormatCapacities(ReadOnlySpan<byte> response)
    {
        if (response.Length < 12)
        {
            return (false, 0);
        }

        int listLength = response[3];
        int currentType = response[8] & 0x03; // 2 = formatted, 1 = unformatted
        bool formatted = currentType == 2;
        uint blocks = BinaryPrimitives.ReadUInt32BigEndian(response.Slice(4, 4));

        for (int q = 12; q + 8 <= 4 + listLength && q + 8 <= response.Length; q += 8)
        {
            int formatType = response[q + 4] >> 2;

            if (formatType == FormatTypeDvdPlusRwBackground)
            {
                blocks = BinaryPrimitives.ReadUInt32BigEndian(response.Slice(q, 4));
                break;
            }
        }

        return (formatted, blocks);
    }

    // ── FORMAT UNIT (0x04) ───────────────────────────────────

    internal static void BuildFormatUnit(Span<byte> cdb)
    {
        if (cdb.Length < 6)
        {
            throw new ArgumentException("FORMAT UNIT CDB must be at least 6 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpFormatUnit;
        cdb[1] = 0x11; // FmtData = 1, defect list format = 001
    }

    /// <summary>
    /// Builds the FORMAT UNIT parameter list for a DVD+RW background format
    /// (format type 0x26) covering <paramref name="blocks"/> logical blocks.
    /// </summary>
    /// <returns>Number of bytes written (always 12).</returns>
    internal static int BuildDvdPlusRwFormatParameters(Span<byte> buffer, uint blocks, bool immediate)
    {
        if (buffer.Length < 12)
        {
            throw new ArgumentException("Format parameter list buffer must be at least 12 bytes.", nameof(buffer));
        }

        buffer.Slice(0, 12).Clear();

        if (immediate)
        {
            buffer[1] = 0x02; // IMMED — format in the background, return immediately
        }

        buffer[3] = 0x08; // format descriptor length
        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(4, 4), blocks);
        buffer[8] = FormatTypeDvdPlusRwBackground << 2;

        return 12;
    }

    // ── TEST UNIT READY (0x00) ───────────────────────────────

    internal static void BuildTestUnitReady(Span<byte> cdb)
    {
        if (cdb.Length < 6)
        {
            throw new ArgumentException("TEST UNIT READY CDB must be at least 6 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpTestUnitReady;
    }

    // ── SYNCHRONIZE CACHE (0x35) ─────────────────────────────

    internal static void BuildSynchronizeCache(Span<byte> cdb)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("SYNCHRONIZE CACHE CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpSynchronizeCache;
    }

    // ── SEND OPC INFORMATION (0x54) ──────────────────────────

    internal static void BuildSendOpc(Span<byte> cdb)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("SEND OPC CDB must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpSendOpc;
        cdb[1] = 0x01; // DoOPC = 1
    }

    // ── MSF helpers ──────────────────────────────────────────

    /// <summary>
    /// Converts an LBA to MSF (minute, second, frame) format.
    /// LBA 0 = MSF 00:02:00 (the 2-second offset per Red Book).
    /// </summary>
    internal static (byte Min, byte Sec, byte Frame) LbaToMsf(long lba)
    {
        long adjusted = lba + 150; // 2-second offset
        int frame = (int)(adjusted % 75);
        int sec = (int)((adjusted / 75) % 60);
        int min = (int)(adjusted / 75 / 60);
        return ((byte)min, (byte)sec, (byte)frame);
    }
}
