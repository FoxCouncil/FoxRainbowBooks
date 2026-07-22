using System.Buffers.Binary;
using FoxOrangebook;
using FoxRedbook;

namespace FoxOrangebook.Tests;

public sealed class BurnSessionTests
{
    // ── Cue sheet building ───────────────────────────────────

    [Fact]
    public void BuildCueSheet_SingleTrack_HasLeadInPregapStartLeadOut()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // Lead-in, pregap (150 forced), track 1 start, lead-out = 4 entries
        Assert.Equal(4, entries.Count);
        Assert.Equal(CueSheetEntry.LeadInTrack, entries[0].TrackNumber);
        Assert.Equal(1, entries[1].TrackNumber);
        Assert.Equal(0x00, entries[1].Index); // pregap
        Assert.Equal(1, entries[2].TrackNumber);
        Assert.Equal(0x01, entries[2].Index); // start
        Assert.Equal(CueSheetEntry.LeadOutTrack, entries[3].TrackNumber);
    }

    [Fact]
    public void BuildCueSheet_Track1Pregap_AtAbsoluteZero()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // Track 1 index 0 sits at absolute MSF 00:00:00 (LBA -150) per the
        // MMC DAO annex — the host streams the pregap itself.
        Assert.Equal(0, entries[1].Minute);
        Assert.Equal(0, entries[1].Second);
        Assert.Equal(0, entries[1].Frame);

        // Track 1 index 1 at MSF 00:02:00 (LBA 0).
        Assert.Equal(0, entries[2].Minute);
        Assert.Equal(2, entries[2].Second);
        Assert.Equal(0, entries[2].Frame);
    }

    [Fact]
    public void BuildCueSheet_TwoTracks_NoPregapOnSecond()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
            new() { Pcm = new MemoryStream(new byte[2352 * 500]) },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // Lead-in, T1 pregap, T1 start, T2 start (no pregap), lead-out = 5
        Assert.Equal(5, entries.Count);
        Assert.Equal(1, entries[1].TrackNumber); // T1 pregap
        Assert.Equal(1, entries[2].TrackNumber); // T1 start
        Assert.Equal(2, entries[3].TrackNumber); // T2 start (index 1)
        Assert.Equal(0x01, entries[3].Index);

        // T2 starts right after T1's audio: LBA 1000.
        var (min, sec, frame) = BurnCommands.LbaToMsf(1000);
        Assert.Equal(min, entries[3].Minute);
        Assert.Equal(sec, entries[3].Second);
        Assert.Equal(frame, entries[3].Frame);
    }

    [Fact]
    public void BuildCueSheet_SecondTrackWithPregap_HasPregapEntry()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
            new() { Pcm = new MemoryStream(new byte[2352 * 500]), PregapSectors = 150 },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // Lead-in, T1 pregap, T1 start, T2 pregap, T2 start, lead-out = 6
        Assert.Equal(6, entries.Count);
        Assert.Equal(2, entries[3].TrackNumber);
        Assert.Equal(0x00, entries[3].Index); // T2 pregap at LBA 1000
        Assert.Equal(2, entries[4].TrackNumber);
        Assert.Equal(0x01, entries[4].Index); // T2 start at LBA 1150

        var (pMin, pSec, pFrame) = BurnCommands.LbaToMsf(1000);
        Assert.Equal(pMin, entries[3].Minute);
        Assert.Equal(pSec, entries[3].Second);
        Assert.Equal(pFrame, entries[3].Frame);

        var (tMin, tSec, tFrame) = BurnCommands.LbaToMsf(1150);
        Assert.Equal(tMin, entries[4].Minute);
        Assert.Equal(tSec, entries[4].Second);
        Assert.Equal(tFrame, entries[4].Frame);
    }

    [Fact]
    public void BuildCueSheet_FirstTrackPregap_EnforcesMinimum150()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]), PregapSectors = 50 },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // The pregap entry is at LBA -150 (MSF 00:00:00); the track start
        // is forced to LBA 0 (MSF 00:02:00), not LBA -100.
        Assert.Equal(0, entries[2].Minute);
        Assert.Equal(2, entries[2].Second);
        Assert.Equal(0, entries[2].Frame);
    }

    [Fact]
    public void BuildCueSheet_FirstTrackLongPregap_ShiftsTrackStart()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]), PregapSectors = 300 },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // 300-sector pregap starting at LBA -150 puts index 1 at LBA 150
        // = MSF 00:04:00.
        Assert.Equal(0, entries[2].Minute);
        Assert.Equal(4, entries[2].Second);
        Assert.Equal(0, entries[2].Frame);
    }

    [Fact]
    public void BuildCueSheet_LeadOutMsf_MatchesExpected()
    {
        // 1000 sectors of audio; the forced pregap occupies LBA -150..-1,
        // so the lead-out starts at LBA 1000 = MSF 00:02:00 + program length.
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
        };

        var entries = BurnSession.BuildCueSheet(tracks);
        var leadOut = entries[^1];

        var (expectedMin, expectedSec, expectedFrame) = BurnCommands.LbaToMsf(1000);
        Assert.Equal(expectedMin, leadOut.Minute);
        Assert.Equal(expectedSec, leadOut.Second);
        Assert.Equal(expectedFrame, leadOut.Frame);
    }

    [Fact]
    public void BuildCueSheet_CdText_LeadInHasCdTextDataForm()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        var plain = BurnSession.BuildCueSheet(tracks);
        var withText = BurnSession.BuildCueSheet(tracks, cdTextInLeadIn: true);

        Assert.Equal(CueSheetEntry.DataFormAudio, plain[0].DataForm);
        Assert.Equal(CueSheetEntry.DataFormCdTextLeadIn, withText[0].DataForm);
    }

    // ── Validation ───────────────────────────────────────────

    [Fact]
    public async Task BurnAsync_EmptyTrackList_Throws()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        await Assert.ThrowsAsync<ArgumentException>(
            () => session.BurnAsync(Array.Empty<AudioTrackSource>()));
    }

    [Fact]
    public async Task BurnAsync_MoreThan99Tracks_Throws()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>();

        for (int i = 0; i < 100; i++)
        {
            tracks.Add(new AudioTrackSource { Pcm = new MemoryStream(new byte[2352 * 300]) });
        }

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => session.BurnAsync(tracks));
        Assert.Contains("99", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnAsync_TrackShorterThan300Sectors_Throws()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
            new() { Pcm = new MemoryStream(new byte[2352 * 299]) },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => session.BurnAsync(tracks));
        Assert.Contains("Track 2", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnAsync_Exactly300SectorTrack_Allowed()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 300]) },
        };

        await session.BurnAsync(tracks);

        Assert.True(transport.SessionClosed);
    }

    [Fact]
    public void Constructor_NullTransport_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new BurnSession(null!));
    }

    [Fact]
    public async Task BurnAsync_DriveDoesNotSupportDao_Throws()
    {
        var transport = new MockScsiTransport { DaoSupported = false };
        var session = new BurnSession(transport);
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.BurnAsync(tracks));
    }

    [Fact]
    public async Task BurnAsync_DiscNotBlank_Throws()
    {
        var transport = new MockScsiTransport { DiscIsBlank = false };
        var session = new BurnSession(transport);
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.BurnAsync(tracks));
    }

    // ── Program area streaming ───────────────────────────────

    [Fact]
    public async Task BurnAsync_SingleTrack_WritesPregapPlusAudio()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 10 });

        int sectorCount = 400;
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * sectorCount]) },
        };

        await session.BurnAsync(tracks);

        // 150 pregap silence sectors + 400 audio sectors.
        Assert.Equal(150 + sectorCount, transport.ProgramSectorsWritten);
        Assert.True(transport.SessionClosed);
        Assert.True(transport.OpcPerformed);
        Assert.True(transport.WriteParametersSet);
        Assert.Single(transport.CueSheets);
    }

    [Fact]
    public async Task BurnAsync_FirstWriteStartsAtLbaMinus150()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 32 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.Equal(-150, transport.FirstProgramWriteLba);
    }

    [Fact]
    public async Task BurnAsync_ProgramWritesAreContiguous()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 32 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
            new() { Pcm = new MemoryStream(new byte[2352 * 300]), PregapSectors = 150 },
        };

        await session.BurnAsync(tracks);

        // -150 pregap + 400 + 150 pregap + 300 = 1000 sectors ending at LBA 850.
        Assert.Equal(1000, transport.ProgramSectorsWritten);
        Assert.Equal(850, transport.NextExpectedProgramLba);
    }

    [Fact]
    public async Task BurnAsync_PregapSectorsAreSilence()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 50 });

        byte[] pcm = new byte[2352 * 300];
        Array.Fill(pcm, (byte)0xAB);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        // Everything before LBA 0 must be digital silence.
        Assert.All(transport.PregapBytes, b => Assert.Equal(0, b));
        Assert.Equal(150 * 2352, transport.PregapBytes.Count);
    }

    [Fact]
    public async Task BurnAsync_ReportsProgressIncludingPregap()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 50 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        var progress = new SyncProgress<BurnProgress>();

        await session.BurnAsync(tracks, progress);

        Assert.NotEmpty(progress.Reports);
        Assert.Equal(550, progress.Reports[^1].SectorsWritten);
        Assert.Equal(550, progress.Reports[^1].TotalSectorsWritten);
        Assert.Equal(550, progress.Reports[^1].TotalDiscSectors);
        Assert.Equal(550, progress.Reports[^1].TrackSectors);
    }

    [Fact]
    public async Task BurnAsync_Cancellation_Throws()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 1 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => session.BurnAsync(tracks, cancellationToken: cts.Token));
    }

    // ── CD-TEXT ──────────────────────────────────────────────

    [Fact]
    public async Task BurnAsync_NoMetadata_PlainCueSheetAndNoLeadInWrites()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.Single(transport.CueSheets);
        Assert.Equal(CueSheetEntry.DataFormAudio, transport.CueSheets[0][3]); // lead-in entry data form
        Assert.False(transport.NwaQueried);
        Assert.Equal(0, transport.LeadInBytesWritten);
        Assert.Empty(session.Warnings);
    }

    [Fact]
    public async Task BurnAsync_WithMetadata_SendsCdTextCueSheetAndFillsLeadIn()
    {
        var transport = new MockScsiTransport { CdTextLeadInSectors = 40 };
        var session = new BurnSession(transport, new BurnOptions { DiscTitle = "Album", DiscPerformer = "Artist" });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]), Title = "Song", Performer = "Artist" },
        };

        await session.BurnAsync(tracks);

        Assert.Single(transport.CueSheets);
        Assert.Equal(CueSheetEntry.DataFormCdTextLeadIn, transport.CueSheets[0][3]);
        Assert.True(transport.NwaQueried);

        // The whole simulated lead-in is filled: 40 sectors × 96 bytes.
        Assert.Equal(40 * 96, transport.LeadInBytesWritten);
        Assert.True(transport.LeadInWrittenBeforeProgram);
        Assert.Empty(session.Warnings);
    }

    [Fact]
    public async Task BurnAsync_DriveRejectsCdTextCueSheet_RetriesWithoutAndWarns()
    {
        var transport = new MockScsiTransport { RejectCdTextCueSheet = true };
        var session = new BurnSession(transport, new BurnOptions { DiscTitle = "Album" });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]), Title = "Song" },
        };

        await session.BurnAsync(tracks);

        // First cue sheet carried CD-TEXT and was rejected; the retry is plain.
        Assert.Equal(2, transport.CueSheets.Count);
        Assert.Equal(CueSheetEntry.DataFormCdTextLeadIn, transport.CueSheets[0][3]);
        Assert.Equal(CueSheetEntry.DataFormAudio, transport.CueSheets[1][3]);

        Assert.Equal(0, transport.LeadInBytesWritten);
        Assert.True(transport.SessionClosed);
        Assert.Equal(550, transport.ProgramSectorsWritten);
        Assert.Single(session.Warnings);
        Assert.Contains("CD-TEXT", session.Warnings[0], StringComparison.Ordinal);
    }

    [Fact]
    public async Task BurnAsync_LeadInPacksDecodeToOriginalMetadata()
    {
        var transport = new MockScsiTransport { CdTextLeadInSectors = 20 };
        var session = new BurnSession(transport, new BurnOptions { DiscTitle = "Joyful Nights", DiscPerformer = "United Cat Orchestra" });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]), Title = "Song of Joy", Performer = "Felix and The Purrs" },
        };

        await session.BurnAsync(tracks);

        byte[] packs = CdTextTestHelpers.CollapseFrom6Bit(transport.LeadInData.ToArray());
        var parsed = CdTextTestHelpers.ParseWithFoxRedbook(CdTextTestHelpers.TakeFirstCycle(packs));

        Assert.NotNull(parsed);
        Assert.Equal("Joyful Nights", parsed!.AlbumTitle);
        Assert.Equal("United Cat Orchestra", parsed.AlbumPerformer);
        Assert.Equal("Song of Joy", parsed.Tracks.Single(t => t.Number == 1).Title);
        Assert.Equal("Felix and The Purrs", parsed.Tracks.Single(t => t.Number == 1).Performer);
    }

    // ── Eject ────────────────────────────────────────────────

    [Fact]
    public void Eject_SendsStartStopUnitWithLoEj()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        session.Eject();

        Assert.NotNull(transport.LastStartStopCdb);
        Assert.Equal(0x1B, transport.LastStartStopCdb![0]);
        Assert.Equal(0x02, transport.LastStartStopCdb[4]); // LoEj=1, Start=0
    }

    // ── Write speed ──────────────────────────────────────────

    [Fact]
    public async Task BurnAsync_NoWriteSpeed_DoesNotSendSetCdSpeed()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport);

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.Null(transport.LastSetCdSpeedCdb);
    }

    [Fact]
    public async Task BurnAsync_WriteSpeed_SendsSetCdSpeedBeforeOpc()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { WriteSpeedKBps = 706 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.NotNull(transport.LastSetCdSpeedCdb);
        Assert.Equal(0xBB, transport.LastSetCdSpeedCdb![0]);
        Assert.Equal(706, BinaryPrimitives.ReadUInt16BigEndian(transport.LastSetCdSpeedCdb!.AsSpan(4, 2)));
        Assert.Equal(0xFFFF, BinaryPrimitives.ReadUInt16BigEndian(transport.LastSetCdSpeedCdb!.AsSpan(2, 2)));
        Assert.True(transport.SpeedSetBeforeOpc);
    }

    [Fact]
    public async Task BurnAsync_WriteSpeedBelow1x_ClampedTo176()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { WriteSpeedKBps = 10 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.NotNull(transport.LastSetCdSpeedCdb);
        Assert.Equal(176, BinaryPrimitives.ReadUInt16BigEndian(transport.LastSetCdSpeedCdb!.AsSpan(4, 2)));
    }

    [Fact]
    public async Task BurnAsync_WriteSpeedAboveMax_ClampedTo65535()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { WriteSpeedKBps = 1_000_000 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 400]) },
        };

        await session.BurnAsync(tracks);

        Assert.NotNull(transport.LastSetCdSpeedCdb);
        Assert.Equal(0xFFFF, BinaryPrimitives.ReadUInt16BigEndian(transport.LastSetCdSpeedCdb!.AsSpan(4, 2)));
    }

    // ── Test helpers ─────────────────────────────────────────

    private sealed class SyncProgress<T> : IProgress<T>
    {
        public List<T> Reports { get; } = new();

        public void Report(T value) => Reports.Add(value);
    }

    // ── Mock transport ───────────────────────────────────────

    private sealed class MockScsiTransport : IScsiTransport
    {
        public bool DaoSupported { get; set; } = true;
        public bool DiscIsBlank { get; set; } = true;
        public bool RejectCdTextCueSheet { get; set; }
        public int CdTextLeadInSectors { get; set; } = 75;

        public long ProgramSectorsWritten { get; private set; }
        public long? FirstProgramWriteLba { get; private set; }
        public long? NextExpectedProgramLba { get; private set; }
        public List<byte> PregapBytes { get; } = new();
        public long LeadInBytesWritten { get; private set; }
        public MemoryStream LeadInData { get; } = new();
        public bool LeadInWrittenBeforeProgram { get; private set; } = true;
        public bool NwaQueried { get; private set; }
        public List<byte[]> CueSheets { get; } = new();
        public bool SessionClosed { get; private set; }
        public bool OpcPerformed { get; private set; }
        public bool WriteParametersSet { get; private set; }
        public byte[]? LastStartStopCdb { get; private set; }
        public byte[]? LastSetCdSpeedCdb { get; private set; }
        public bool SpeedSetBeforeOpc { get; private set; }

        public DriveInquiry Inquiry => new()
        {
            Vendor = "MOCK",
            Product = "BURNER",
            Revision = "1.0",
        };

        public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
        {
            byte opcode = cdb[0];

            switch (opcode)
            {
                case BurnCommands.OpGetConfiguration:
                {
                    if (direction == ScsiDirection.In && buffer.Length >= 12 && DaoSupported)
                    {
                        BinaryPrimitives.WriteUInt32BigEndian(buffer, 8);
                        BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(8, 2), BurnCommands.FeatureCdMastering);
                    }

                    break;
                }

                case BurnCommands.OpReadDiscInformation:
                {
                    if (direction == ScsiDirection.In && buffer.Length >= 34)
                    {
                        buffer[2] = DiscIsBlank ? (byte)0x00 : (byte)0x02;
                        buffer[21] = 0xFF;
                        buffer[22] = 0xFF;
                        buffer[23] = 0xFF;
                    }

                    break;
                }

                case BurnCommands.OpReadTrackInformation:
                {
                    NwaQueried = true;

                    if (direction == ScsiDirection.In && buffer.Length >= 16)
                    {
                        buffer.Clear();
                        buffer[7] = 0x01; // NWA valid
                        int nwa = -150 - CdTextLeadInSectors;
                        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(12, 4), unchecked((uint)nwa));
                    }

                    break;
                }

                case BurnCommands.OpSendOpc:
                {
                    OpcPerformed = true;
                    SpeedSetBeforeOpc = LastSetCdSpeedCdb is not null;
                    break;
                }

                case BurnCommands.OpModeSelect10:
                {
                    WriteParametersSet = true;
                    break;
                }

                case BurnCommands.OpSendCueSheet:
                {
                    byte[] data = buffer.ToArray();

                    // Entry 0 is the lead-in; byte 3 is its data form.
                    if (RejectCdTextCueSheet && data.Length >= 8 && (data[3] & 0x40) != 0)
                    {
                        CueSheets.Add(data);
                        throw new OpticalDriveException("CHECK CONDITION: ILLEGAL REQUEST (mock rejects CD-TEXT cue sheets).");
                    }

                    CueSheets.Add(data);
                    break;
                }

                case BurnCommands.OpWrite10:
                {
                    int lba = unchecked((int)BinaryPrimitives.ReadUInt32BigEndian(cdb.Slice(2, 4)));
                    ushort count = BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(7, 2));

                    if (lba < -150)
                    {
                        LeadInBytesWritten += buffer.Length;
                        LeadInData.Write(buffer);
                    }
                    else
                    {
                        if (LeadInBytesWritten == 0 && NwaQueried && ProgramSectorsWritten == 0)
                        {
                            LeadInWrittenBeforeProgram = false;
                        }

                        FirstProgramWriteLba ??= lba;

                        if (NextExpectedProgramLba is long expected && expected != lba)
                        {
                            throw new InvalidOperationException($"Non-contiguous WRITE: expected LBA {expected}, got {lba}.");
                        }

                        // Capture bytes destined for LBAs below 0 (pregap region).
                        for (int i = 0; i < count; i++)
                        {
                            if (lba + i < 0)
                            {
                                for (int b = 0; b < 2352; b++)
                                {
                                    PregapBytes.Add(buffer[i * 2352 + b]);
                                }
                            }
                        }

                        ProgramSectorsWritten += count;
                        NextExpectedProgramLba = lba + count;
                    }

                    break;
                }

                case BurnCommands.OpCloseTrackSession:
                {
                    SessionClosed = true;
                    break;
                }

                case BurnCommands.OpStartStopUnit:
                {
                    LastStartStopCdb = cdb.ToArray();
                    break;
                }

                case BurnCommands.OpSetCdSpeed:
                {
                    LastSetCdSpeedCdb = cdb.ToArray();
                    break;
                }
            }
        }

        public void Dispose() => LeadInData.Dispose();

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
