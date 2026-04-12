using System.Buffers.Binary;
using System.Collections.ObjectModel;
using System.Text;

namespace FoxRedbook.Platforms.Common;

/// <summary>
/// Pure functions for building SCSI Command Descriptor Blocks (CDBs),
/// parsing the corresponding response buffers, and mapping SCSI sense
/// data to FoxRedbook exception types. Shared across every platform
/// backend — Linux, Windows, and macOS all import this namespace to
/// reuse the same CDB builders and response parsers. Deliberately NOT
/// annotated with any <c>[SupportedOSPlatform(...)]</c> so the tests
/// can exercise them on any development host.
/// </summary>
internal static class ScsiCommands
{
    // ── SCSI opcodes ───────────────────────────────────────────────

    internal const byte OpInquiry = 0x12;
    internal const byte OpReadToc = 0x43;
    internal const byte OpReadCd = 0xBE;

    // ── INQUIRY ────────────────────────────────────────────────────

    /// <summary>
    /// Minimum length of the standard INQUIRY response we require.
    /// The vendor/product/revision strings live at bytes 8..35.
    /// </summary>
    internal const int InquiryResponseLength = 36;

    /// <summary>
    /// Builds a 6-byte INQUIRY CDB requesting the standard inquiry data
    /// (EVPD=0) with a 36-byte allocation length. The caller provides a
    /// pre-zeroed 6-byte buffer.
    /// </summary>
    internal static void BuildInquiry(Span<byte> cdb)
    {
        if (cdb.Length < 6)
        {
            throw new ArgumentException("INQUIRY CDB buffer must be at least 6 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpInquiry;
        // byte[1] = 0: EVPD=0 means standard inquiry, page code ignored
        // byte[2] = 0: page code (ignored when EVPD=0)
        // bytes[3..4] = allocation length (big-endian 16-bit)
        cdb[3] = 0x00;
        cdb[4] = InquiryResponseLength; // 0x24 = 36
        // byte[5] = 0: control
    }

    /// <summary>
    /// Parses a standard INQUIRY response into a <see cref="DriveInquiry"/>,
    /// trimming trailing whitespace from the vendor, product, and revision
    /// ASCII fields.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The response is shorter than <see cref="InquiryResponseLength"/> bytes.
    /// </exception>
    internal static DriveInquiry ParseInquiry(ReadOnlySpan<byte> response)
    {
        if (response.Length < InquiryResponseLength)
        {
            throw new ArgumentException(
                $"INQUIRY response is {response.Length} bytes; expected at least {InquiryResponseLength}.",
                nameof(response));
        }

        // Bytes 8..15: vendor identification (8 ASCII, space-padded)
        // Bytes 16..31: product identification (16 ASCII, space-padded)
        // Bytes 32..35: product revision level (4 ASCII, space-padded)
        string vendor = Encoding.ASCII.GetString(response.Slice(8, 8)).TrimEnd();
        string product = Encoding.ASCII.GetString(response.Slice(16, 16)).TrimEnd();
        string revision = Encoding.ASCII.GetString(response.Slice(32, 4)).TrimEnd();

        return new DriveInquiry
        {
            Vendor = vendor,
            Product = product,
            Revision = revision,
        };
    }

    // ── READ TOC ───────────────────────────────────────────────────

    /// <summary>
    /// Maximum allocation length we request for the READ TOC response:
    /// 4 bytes of header + 100 × 8 bytes of track descriptors (99 tracks
    /// plus lead-out) = 804 bytes.
    /// </summary>
    internal const int ReadTocMaxAllocationLength = 4 + 100 * 8;

    /// <summary>
    /// Builds a 10-byte READ TOC/PMA/ATIP CDB in format 0 (standard TOC)
    /// with LBA addressing (MSF=0).
    /// </summary>
    /// <remarks>
    /// We always request the full 804-byte maximum allocation length even
    /// though small discs will return fewer bytes. The alternative — a
    /// two-pass read that first queries the TOC data length from the
    /// 4-byte header and then issues a second READ TOC with a precise
    /// allocation — would save a few hundred bytes of wasted transfer on
    /// short discs but cost an extra ioctl round-trip. At sg_io round-trip
    /// latency (single-digit milliseconds) versus ~100 bytes/ms wasted
    /// bandwidth, the over-allocation wins by an order of magnitude and
    /// the drive truncates cleanly by setting the <c>TOC Data Length</c>
    /// field in the response header.
    /// </remarks>
    internal static void BuildReadToc(Span<byte> cdb)
    {
        if (cdb.Length < 10)
        {
            throw new ArgumentException("READ TOC CDB buffer must be at least 10 bytes.", nameof(cdb));
        }

        cdb.Clear();
        cdb[0] = OpReadToc;
        // byte[1] = 0: MSF bit (bit 1) clear → LBA mode
        // byte[2] = 0: format field (low 4 bits) = 0000 → standard TOC
        // bytes[3..5] = 0: reserved
        cdb[6] = 0x01; // Starting Track = 1 (return all audio tracks)
        BinaryPrimitives.WriteUInt16BigEndian(cdb.Slice(7, 2), (ushort)ReadTocMaxAllocationLength);
        // byte[9] = 0: control
    }

    /// <summary>
    /// Parses a standard READ TOC format-0 response into a
    /// <see cref="TableOfContents"/>. The response consists of a 4-byte
    /// header (data length + first/last track) followed by 8-byte track
    /// descriptors including the lead-out at track number 0xAA.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Response too short, or the data length header does not match the
    /// observed byte count, or the lead-out descriptor is missing.
    /// </exception>
    internal static TableOfContents ParseReadTocResponse(ReadOnlySpan<byte> response)
    {
        if (response.Length < 4)
        {
            throw new ArgumentException("READ TOC response is shorter than the 4-byte header.", nameof(response));
        }

        // Data Length field does NOT include its own 2 bytes.
        ushort dataLength = BinaryPrimitives.ReadUInt16BigEndian(response.Slice(0, 2));
        int totalBytes = dataLength + 2;

        if (response.Length < totalBytes)
        {
            throw new ArgumentException(
                $"READ TOC response claims {totalBytes} bytes but buffer only has {response.Length}.",
                nameof(response));
        }

        int firstTrackNumber = response[2];
        int lastTrackNumber = response[3];

        // The descriptor area starts at byte 4 and consists of 8-byte
        // entries, one per track plus one for the lead-out (track 0xAA).
        int descriptorAreaBytes = totalBytes - 4;

        if (descriptorAreaBytes % 8 != 0)
        {
            throw new ArgumentException(
                $"READ TOC descriptor area is {descriptorAreaBytes} bytes, not a multiple of 8.",
                nameof(response));
        }

        int descriptorCount = descriptorAreaBytes / 8;
        var trackList = new List<TrackInfo>(descriptorCount);
        long leadOutLba = -1;

        // First pass: collect raw (number, LBA, control) triples for every
        // descriptor except the lead-out. We can't compute SectorCount
        // until we know each track's end, which requires seeing the next
        // track (or the lead-out).
        var rawEntries = new List<(int Number, long Lba, byte ControlNibble)>(descriptorCount);

        for (int i = 0; i < descriptorCount; i++)
        {
            ReadOnlySpan<byte> descriptor = response.Slice(4 + i * 8, 8);
            // byte 0: reserved
            byte adrControl = descriptor[1];
            byte trackNumber = descriptor[2];
            // byte 3: reserved
            long lba = BinaryPrimitives.ReadUInt32BigEndian(descriptor.Slice(4, 4));

            if (trackNumber == CdConstants.LeadOutTrackNumber)
            {
                leadOutLba = lba;
            }
            else
            {
                rawEntries.Add((trackNumber, lba, (byte)(adrControl & 0x0F)));
            }
        }

        if (leadOutLba < 0)
        {
            throw new ArgumentException("READ TOC response did not contain a lead-out descriptor.", nameof(response));
        }

        // Sort by track number just in case the drive reported them out of order.
        rawEntries.Sort((a, b) => a.Number.CompareTo(b.Number));

        // Second pass: compute SectorCount for each track using the next
        // track's LBA (or the lead-out for the final track).
        for (int i = 0; i < rawEntries.Count; i++)
        {
            long endLba = i + 1 < rawEntries.Count ? rawEntries[i + 1].Lba : leadOutLba;
            int sectorCount = (int)(endLba - rawEntries[i].Lba);

            (TrackControl control, TrackType type) = DecodeControl(rawEntries[i].ControlNibble);

            trackList.Add(new TrackInfo
            {
                Number = rawEntries[i].Number,
                StartLba = rawEntries[i].Lba,
                SectorCount = sectorCount,
                Type = type,
                Control = control,
            });
        }

        return new TableOfContents
        {
            FirstTrackNumber = firstTrackNumber,
            LastTrackNumber = lastTrackNumber,
            LeadOutLba = leadOutLba,
            Tracks = new ReadOnlyCollection<TrackInfo>(trackList),
        };
    }

    private static (TrackControl Control, TrackType Type) DecodeControl(byte controlNibble)
    {
        // Control nibble bits (SPC / MMC):
        //   bit 0: PreEmphasis (audio 50/15 μs)
        //   bit 1: CopyPermitted
        //   bit 2: DataTrack (0 = audio, 1 = data)
        //   bit 3: FourChannel (quadraphonic audio)
        var flags = TrackControl.None;

        if ((controlNibble & 0x01) != 0)
        {
            flags |= TrackControl.PreEmphasis;
        }

        if ((controlNibble & 0x02) != 0)
        {
            flags |= TrackControl.CopyPermitted;
        }

        if ((controlNibble & 0x04) != 0)
        {
            flags |= TrackControl.DataTrack;
        }

        if ((controlNibble & 0x08) != 0)
        {
            flags |= TrackControl.FourChannel;
        }

        TrackType type = (controlNibble & 0x04) != 0 ? TrackType.Data : TrackType.Audio;

        return (flags, type);
    }

    // ── READ CD ────────────────────────────────────────────────────

    /// <summary>
    /// Builds a 12-byte READ CD CDB configured for CD-DA audio extraction.
    /// Byte 1 bits 2-4 = 001b (expected sector type: CD-DA). Byte 9 is the
    /// main channel selection byte and byte 10 is the subchannel selection.
    /// </summary>
    internal static void BuildReadCd(Span<byte> cdb, long lba, int sectorCount, ReadOptions flags)
    {
        if (cdb.Length < 12)
        {
            throw new ArgumentException("READ CD CDB buffer must be at least 12 bytes.", nameof(cdb));
        }

        if (lba < 0 || lba > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(lba));
        }

        if (sectorCount < 0 || sectorCount > 0xFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(sectorCount));
        }

        cdb.Clear();
        cdb[0] = OpReadCd;

        // byte 1: expected sector type in bits 2-4.
        // Spec allows 001b (CD-DA filter, 0x04) but the community standard
        // is 000b (any type, 0x00) after years of USB-adapter compatibility
        // reports in EAC, cdparanoia, and whipper. Our Pioneer BDR-XS07U
        // handles either value; 0x00 is the safer default for drives we
        // haven't tested. Callers already constrain reads to audio-track
        // LBAs, so the filter adds no safety value.
        cdb[1] = 0x00;

        // bytes 2-5: starting LBA (big-endian 32-bit)
        BinaryPrimitives.WriteUInt32BigEndian(cdb.Slice(2, 4), (uint)lba);

        // bytes 6-8: transfer length in SECTORS (big-endian 24-bit)
        // C# has no native 24-bit write; decompose by hand.
        cdb[6] = (byte)((sectorCount >> 16) & 0xFF);
        cdb[7] = (byte)((sectorCount >> 8) & 0xFF);
        cdb[8] = (byte)(sectorCount & 0xFF);

        // byte 9: main channel selection byte (MCSB)
        //   bit 7: SYNC                  (0 for CD-DA)
        //   bits 6-5: header codes       (00 for CD-DA)
        //   bit 4: USER DATA             (1 — read 2352 audio bytes)
        //   bit 3: EDC/ECC               (0 for CD-DA)
        //   bits 2-1: C2 error info      (01 = C2 pointers, 00 = none)
        //   bit 0: reserved
        byte mcsb = 0x10; // user data

        if ((flags & ReadOptions.C2ErrorPointers) != 0)
        {
            // C2 error info = 01b in bits 2-1 → 0x02
            mcsb |= 0x02;
        }

        cdb[9] = mcsb;

        // byte 10: subchannel selection (low 3 bits)
        //   000 = none
        //   001 = raw P-W (96 bytes)
        //   010 = formatted Q
        byte subChannel = 0x00;

        if ((flags & ReadOptions.SubchannelData) != 0)
        {
            subChannel = 0x01; // raw P-W
        }

        cdb[10] = subChannel;
        // byte 11: control (0)
    }

    // ── Sense data → exception mapping ─────────────────────────────

    /// <summary>
    /// Parses a SCSI fixed-format sense buffer and returns an
    /// <see cref="OpticalDriveException"/> (or subclass) describing the
    /// error. Descriptor-format sense data (response code 0x72/0x73) is
    /// reported generically with the raw bytes in the message since
    /// optical drives rarely produce it.
    /// </summary>
    /// <param name="sense">
    /// The sense buffer. Expects at least 14 bytes (through ASCQ at byte 13)
    /// to produce a typed exception; shorter buffers are tolerated and
    /// fall through to a generic <see cref="OpticalDriveException"/> with
    /// the truncated bytes in the message. Longer buffers are tolerated
    /// and excess bytes are ignored — Linux sg_io typically returns 18
    /// bytes, Windows SCSI_PASS_THROUGH returns up to 32, and both work
    /// with this parser unchanged.
    /// </param>
    /// <returns>The appropriate exception instance, ready to throw.</returns>
    internal static OpticalDriveException MapSenseData(ReadOnlySpan<byte> sense)
    {
        if (sense.IsEmpty)
        {
            return new OpticalDriveException("SCSI command failed with no sense data.");
        }

        byte responseCode = (byte)(sense[0] & 0x7F);
        bool isFixedFormat = responseCode == 0x70 || responseCode == 0x71;

        if (!isFixedFormat)
        {
            return new OpticalDriveException(
                $"SCSI command failed with descriptor-format sense data (response code 0x{responseCode:X2}); raw: {FormatSenseBytes(sense)}.");
        }

        if (sense.Length < 14)
        {
            return new OpticalDriveException(
                $"SCSI fixed-format sense data truncated to {sense.Length} bytes; raw: {FormatSenseBytes(sense)}.");
        }

        byte senseKey = (byte)(sense[2] & 0x0F);
        byte asc = sense[12];
        byte ascq = sense[13];

        // SENSE KEY 0x02 = NOT READY. Distinguish "medium not present"
        // (ASC 0x3A) from other not-ready conditions (spinning up, loading, etc.)
        if (senseKey == 0x02)
        {
            if (asc == 0x3A)
            {
                return new MediaNotPresentException(
                    $"Medium not present (sense key 0x02, ASC 0x{asc:X2}, ASCQ 0x{ascq:X2}).");
            }

            return new DriveNotReadyException(
                $"Drive not ready (sense key 0x02, ASC 0x{asc:X2}, ASCQ 0x{ascq:X2}).");
        }

        return new OpticalDriveException(
            $"SCSI error: sense key 0x{senseKey:X2}, ASC 0x{asc:X2}, ASCQ 0x{ascq:X2}.");
    }

    private static string FormatSenseBytes(ReadOnlySpan<byte> sense)
    {
        var sb = new StringBuilder(sense.Length * 3);

        for (int i = 0; i < sense.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(' ');
            }

            sb.Append(sense[i].ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
        }

        return sb.ToString();
    }
}
