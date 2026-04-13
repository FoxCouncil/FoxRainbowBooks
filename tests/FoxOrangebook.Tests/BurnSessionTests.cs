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
        Assert.Equal(0x00, entries[3].Index); // T2 pregap
        Assert.Equal(2, entries[4].TrackNumber);
        Assert.Equal(0x01, entries[4].Index); // T2 start
    }

    [Fact]
    public void BuildCueSheet_FirstTrackPregap_EnforcesMinimum150()
    {
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 100]), PregapSectors = 50 },
        };

        var entries = BurnSession.BuildCueSheet(tracks);

        // The pregap entry is at LBA 0, track start should be at LBA 150 (not 50).
        // MSF of LBA 150 = 00:04:00 (150 + 150 offset = 300 frames = 4 sec)
        Assert.Equal(0, entries[2].Minute);
        Assert.Equal(4, entries[2].Second);
        Assert.Equal(0, entries[2].Frame);
    }

    [Fact]
    public void BuildCueSheet_LeadOutMsf_MatchesExpected()
    {
        // 1000 sectors of audio + 150 pregap = 1150 total LBAs
        // MSF of LBA 1150 = (1150 + 150) / 75 = 1300/75 = 17 sec + 25 frames
        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 1000]) },
        };

        var entries = BurnSession.BuildCueSheet(tracks);
        var leadOut = entries[^1];

        var (expectedMin, expectedSec, expectedFrame) = BurnCommands.LbaToMsf(150 + 1000);
        Assert.Equal(expectedMin, leadOut.Minute);
        Assert.Equal(expectedSec, leadOut.Second);
        Assert.Equal(expectedFrame, leadOut.Frame);
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
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
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
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => session.BurnAsync(tracks));
    }

    [Fact]
    public async Task BurnAsync_SingleTrack_WritesAllSectors()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 10 });

        int sectorCount = 25;
        byte[] pcm = new byte[2352 * sectorCount];

        // Fill with a recognizable pattern
        for (int i = 0; i < pcm.Length; i++)
        {
            pcm[i] = (byte)(i & 0xFF);
        }

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(pcm) },
        };

        await session.BurnAsync(tracks);

        Assert.Equal(sectorCount, transport.TotalSectorsWritten);
        Assert.True(transport.SessionClosed);
        Assert.True(transport.OpcPerformed);
        Assert.True(transport.WriteParametersSet);
        Assert.True(transport.CueSheetSent);
    }

    [Fact]
    public async Task BurnAsync_ReportsProgress()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 50 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
        };

        var reports = new List<BurnProgress>();

        await session.BurnAsync(tracks, new Progress<BurnProgress>(p => reports.Add(p)));

        Assert.NotEmpty(reports);
        Assert.Equal(100, reports[^1].SectorsWritten);
        Assert.Equal(100, reports[^1].TotalSectorsWritten);
    }

    [Fact]
    public async Task BurnAsync_Cancellation_Throws()
    {
        var transport = new MockScsiTransport();
        var session = new BurnSession(transport, new BurnOptions { SectorsPerWrite = 1 });

        var tracks = new List<AudioTrackSource>
        {
            new() { Pcm = new MemoryStream(new byte[2352 * 100]) },
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => session.BurnAsync(tracks, cancellationToken: cts.Token));
    }

    // ── Mock transport ───────────────────────────────────────

    private sealed class MockScsiTransport : IScsiTransport
    {
        public bool DaoSupported { get; set; } = true;
        public bool DiscIsBlank { get; set; } = true;
        public int TotalSectorsWritten { get; private set; }
        public bool SessionClosed { get; private set; }
        public bool OpcPerformed { get; private set; }
        public bool WriteParametersSet { get; private set; }
        public bool CueSheetSent { get; private set; }

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
                        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(buffer, 8);
                        System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(8, 2), BurnCommands.FeatureCdMastering);
                    }

                    break;
                }

                case BurnCommands.OpReadDiscInformation:
                {
                    if (direction == ScsiDirection.In && buffer.Length >= 34)
                    {
                        buffer[2] = DiscIsBlank ? (byte)0x00 : (byte)0x02;
                    }

                    break;
                }

                case BurnCommands.OpSendOpc:
                {
                    OpcPerformed = true;
                    break;
                }

                case BurnCommands.OpModeSelect10:
                {
                    WriteParametersSet = true;
                    break;
                }

                case BurnCommands.OpSendCueSheet:
                {
                    CueSheetSent = true;
                    break;
                }

                case BurnCommands.OpWrite10:
                {
                    ushort count = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(7, 2));
                    TotalSectorsWritten += count;
                    break;
                }

                case BurnCommands.OpCloseTrackSession:
                {
                    SessionClosed = true;
                    break;
                }
            }
        }

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
