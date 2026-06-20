using System.Buffers.Binary;
using System.Text;
using FoxOrangebook.FileSystem;
using FoxOrangebook.FileSystem.Udf;
using FoxRedbook;

namespace FoxOrangebook.Tests;

public sealed class DataBurnSessionTests
{
    private const int Sector = 2048;

    // ── End-to-end burn to a file ────────────────────────────────

    [Fact]
    public async Task BurnAsync_ToFile_ProducesBytesIdenticalToImage()
    {
        DiscImage image = BuildImage(b =>
        {
            b.AddFile("readme.txt", Encoding.ASCII.GetBytes("end to end burn"));
            b.AddFile("docs/data.bin", Pattern(0x42, 6000));
        });

        byte[] expected = ImageBytes(image);
        string path = TempIsoPath();

        try
        {
            using (var transport = new FileBackedDataTransport(path))
            {
                var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 8 });
                await session.BurnAsync(image);
            }

            byte[] actual = await File.ReadAllBytesAsync(path);
            Assert.Equal(expected.Length, actual.Length);
            Assert.Equal(expected, actual);
        }
        finally
        {
            Delete(path);
        }
    }

    [Fact]
    public async Task BurnAsync_ToFile_OutputIsReadableBridgeDisc()
    {
        DiscImage image = BuildImage(b => b.AddFile("hello.txt", Encoding.ASCII.GetBytes("hi")));
        string path = TempIsoPath();

        try
        {
            using (var transport = new FileBackedDataTransport(path))
            {
                await new DataBurnSession(transport).BurnAsync(image);
            }

            byte[] iso = await File.ReadAllBytesAsync(path);

            // ISO 9660 primary volume descriptor at sector 16.
            Assert.Equal(0x01, iso[16 * Sector]);
            Assert.True(iso.AsSpan(16 * Sector + 1, 5).SequenceEqual("CD001"u8));

            // Valid UDF anchor at sector 256.
            int anchor = UdfConstants.AnchorSector * Sector;
            Assert.Equal(UdfConstants.TagAnchorVolumeDescriptorPointer, BinaryPrimitives.ReadUInt16LittleEndian(iso.AsSpan(anchor, 2)));
            Assert.Equal(iso[anchor + 4], UdfCrc.TagChecksum(iso.AsSpan(anchor, 16)));
        }
        finally
        {
            Delete(path);
        }
    }

    // ── Command sequence (mock transport) ────────────────────────

    [Fact]
    public async Task BurnAsync_RunsMode1DataWriteSequence()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", Pattern(0x01, 9000)));
        var transport = new MockDataTransport();
        var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 4 });

        await session.BurnAsync(image);

        Assert.True(transport.OpcPerformed);
        Assert.True(transport.WriteParametersSet);
        Assert.True(transport.CacheSynchronized); // flush before close (validated on a Pioneer BDR-XS07U)
        Assert.True(transport.SessionClosed);
        Assert.Equal(image.SectorCount, transport.TotalSectorsWritten);

        // Write Parameters page: track mode = data (0x04), data block type = Mode 1 (0x08).
        Assert.NotNull(transport.WriteParametersPayload);
        Assert.Equal(0x04, transport.WriteParametersPayload![11]);
        Assert.Equal(0x08, transport.WriteParametersPayload![12]);

        // Sectors written contiguously from LBA 0.
        Assert.Equal(0u, transport.FirstLba);
        Assert.Equal(image.SectorCount, transport.LastLbaPlusCount);
    }

    [Fact]
    public async Task BurnAsync_ReportsProgressToCompletion()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", Pattern(0x7F, 20000)));
        var transport = new MockDataTransport();
        var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 4 });

        var progress = new SyncProgress<BurnProgress>();
        await session.BurnAsync(image, progress);

        Assert.NotEmpty(progress.Reports);
        Assert.Equal(image.SectorCount, progress.Reports[^1].TotalSectorsWritten);
        Assert.Equal(image.SectorCount, progress.Reports[^1].TotalDiscSectors);
    }

    // ── DVD+RW ───────────────────────────────────────────────────

    [Fact]
    public async Task BurnAsync_DvdPlusRw_ToFile_ProducesBytesIdenticalToImage()
    {
        DiscImage image = BuildImage(b =>
        {
            b.AddFile("readme.txt", Encoding.ASCII.GetBytes("dvd+rw end to end"));
            b.AddFile("docs/data.bin", Pattern(0x42, 6000));
        });

        byte[] expected = ImageBytes(image);
        string path = TempIsoPath();

        try
        {
            using (var transport = new FileBackedDataTransport(path, FileBackedMedia.DvdPlusRewritable))
            {
                var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 8 });
                await session.BurnAsync(image);
            }

            byte[] actual = await File.ReadAllBytesAsync(path);
            Assert.Equal(expected, actual);
        }
        finally
        {
            Delete(path);
        }
    }

    [Fact]
    public async Task BurnAsync_DvdPlusRw_FormatsThenRetriesWriteWhileFormatInProgress()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", Pattern(0x01, 9000)));
        var transport = new MockDvdPlusRwTransport { RejectFirstWrites = 1 };
        var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 4 });

        await session.BurnAsync(image);

        Assert.True(transport.FormatUnitIssued);
        Assert.True(transport.WriteRejectionsObserved >= 1);
        Assert.Equal(image.SectorCount, transport.TotalSectorsWritten);
    }

    [Fact]
    public async Task BurnAsync_UnsupportedProfile_Throws()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", new byte[100]));
        var transport = new MockDataTransport { Profile = 0x0010 }; // DVD-ROM (read-only)
        var session = new DataBurnSession(transport);

        await Assert.ThrowsAsync<NotSupportedException>(() => session.BurnAsync(image));
    }

    // ── Validation / sad paths ───────────────────────────────────

    [Fact]
    public void Constructor_NullTransport_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new DataBurnSession(null!));
    }

    [Fact]
    public async Task BurnAsync_NullImage_Throws()
    {
        var session = new DataBurnSession(new MockDataTransport());
        await Assert.ThrowsAsync<ArgumentNullException>(() => session.BurnAsync(null!));
    }

    [Fact]
    public async Task BurnAsync_DriveLacksDataSupport_Throws()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", new byte[100]));
        var transport = new MockDataTransport { DataSupported = false };
        var session = new DataBurnSession(transport);

        await Assert.ThrowsAsync<InvalidOperationException>(() => session.BurnAsync(image));
    }

    [Fact]
    public async Task BurnAsync_DiscNotBlank_Throws()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", new byte[100]));
        var transport = new MockDataTransport { DiscIsBlank = false };
        var session = new DataBurnSession(transport);

        await Assert.ThrowsAsync<InvalidOperationException>(() => session.BurnAsync(image));
    }

    [Fact]
    public async Task BurnAsync_Cancellation_Throws()
    {
        DiscImage image = BuildImage(b => b.AddFile("a.bin", Pattern(0x10, 50000)));
        var transport = new MockDataTransport();
        var session = new DataBurnSession(transport, new DataBurnOptions { SectorsPerWrite = 1 });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => session.BurnAsync(image, cancellationToken: cts.Token));
    }

    // ── Helpers ──────────────────────────────────────────────────

    private static DiscImage BuildImage(Action<DiscImageBuilder> populate)
    {
        var builder = new DiscImageBuilder(new DiscImageOptions { VolumeIdentifier = "BURNTEST" });
        populate(builder);
        return builder.Build();
    }

    private static byte[] ImageBytes(DiscImage image)
    {
        using var ms = new MemoryStream();
        image.WriteTo(ms);
        return ms.ToArray();
    }

    private static byte[] Pattern(byte seed, int length)
    {
        var data = new byte[length];

        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)(seed + i);
        }

        return data;
    }

    private static string TempIsoPath() => Path.Combine(Path.GetTempPath(), $"foxburn_{Guid.NewGuid():N}.iso");

    private static void Delete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class SyncProgress<T> : IProgress<T>
    {
        public List<T> Reports { get; } = new();

        public void Report(T value) => Reports.Add(value);
    }

    private sealed class MockDataTransport : IScsiTransport
    {
        public bool DataSupported { get; set; } = true;
        public bool DiscIsBlank { get; set; } = true;
        public ushort Profile { get; set; } = BurnCommands.ProfileCdRw;
        public long TotalSectorsWritten { get; private set; }
        public bool OpcPerformed { get; private set; }
        public bool WriteParametersSet { get; private set; }
        public bool CacheSynchronized { get; private set; }
        public bool SessionClosed { get; private set; }
        public byte[]? WriteParametersPayload { get; private set; }
        public uint FirstLba { get; private set; }
        public long LastLbaPlusCount { get; private set; }

        private bool _firstWrite = true;

        public DriveInquiry Inquiry => new() { Vendor = "MOCK", Product = "DATA", Revision = "1.0" };

        public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
        {
            switch (cdb[0])
            {
                case BurnCommands.OpGetConfiguration:
                {
                    buffer.Clear();

                    if (buffer.Length >= 8)
                    {
                        BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(6, 2), Profile);
                    }

                    if (DataSupported && buffer.Length >= 12)
                    {
                        ushort requested = BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(2, 2));
                        BinaryPrimitives.WriteUInt32BigEndian(buffer, 8);
                        BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(8, 2), requested);
                    }

                    break;
                }

                case BurnCommands.OpReadDiscInformation:
                {
                    if (buffer.Length >= 34)
                    {
                        buffer.Clear();
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
                    WriteParametersPayload = buffer.ToArray();
                    break;
                }

                case BurnCommands.OpWrite10:
                {
                    uint lba = BinaryPrimitives.ReadUInt32BigEndian(cdb.Slice(2, 4));
                    ushort count = BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(7, 2));

                    if (_firstWrite)
                    {
                        FirstLba = lba;
                        _firstWrite = false;
                    }

                    TotalSectorsWritten += count;
                    LastLbaPlusCount = lba + count;
                    break;
                }

                case BurnCommands.OpSynchronizeCache:
                {
                    CacheSynchronized = true;
                    break;
                }

                case BurnCommands.OpTestUnitReady:
                {
                    // Ready immediately — no exception.
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

    private sealed class MockDvdPlusRwTransport : IScsiTransport
    {
        public int RejectFirstWrites { get; set; } = 1;
        public bool FormatUnitIssued { get; private set; }
        public int WriteRejectionsObserved { get; private set; }
        public long TotalSectorsWritten { get; private set; }

        private int _rejectionsRemaining = -1;

        public DriveInquiry Inquiry => new() { Vendor = "MOCK", Product = "DVDPRW", Revision = "1.0" };

        public void Execute(ReadOnlySpan<byte> cdb, Span<byte> buffer, ScsiDirection direction)
        {
            if (_rejectionsRemaining < 0)
            {
                _rejectionsRemaining = RejectFirstWrites;
            }

            switch (cdb[0])
            {
                case BurnCommands.OpGetConfiguration:
                {
                    buffer.Clear();

                    if (buffer.Length >= 8)
                    {
                        BinaryPrimitives.WriteUInt16BigEndian(buffer.Slice(6, 2), BurnCommands.ProfileDvdPlusRw);
                    }

                    break;
                }

                case BurnCommands.OpReadFormatCapacities:
                {
                    if (buffer.Length >= 20)
                    {
                        buffer.Clear();
                        buffer[3] = 16;
                        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(4, 4), 2295104);
                        buffer[8] = 0x01; // unformatted
                        buffer[10] = 0x08;
                        BinaryPrimitives.WriteUInt32BigEndian(buffer.Slice(12, 4), 2295104);
                        buffer[16] = BurnCommands.FormatTypeDvdPlusRwBackground << 2;
                    }

                    break;
                }

                case BurnCommands.OpFormatUnit:
                {
                    FormatUnitIssued = true;
                    break;
                }

                case BurnCommands.OpWrite10:
                {
                    if (_rejectionsRemaining > 0)
                    {
                        _rejectionsRemaining--;
                        WriteRejectionsObserved++;
                        throw new DriveNotReadyException("Drive not ready (sense key 0x02, ASC 0x04, ASCQ 0x04).");
                    }

                    TotalSectorsWritten += BinaryPrimitives.ReadUInt16BigEndian(cdb.Slice(7, 2));
                    break;
                }

                default:
                {
                    break;
                }
            }
        }

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
