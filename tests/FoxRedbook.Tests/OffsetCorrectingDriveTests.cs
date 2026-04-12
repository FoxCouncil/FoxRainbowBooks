using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

public sealed class OffsetCorrectingDriveTests : IDisposable
{
    private const int TestSectorCount = 20;

    private readonly string _tempDir;
    private readonly string _testFilePath;
    private readonly TableOfContents _toc;

    public OffsetCorrectingDriveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxredbook_offset_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _testFilePath = CreateTestFile(TestSectorCount);
        _toc = CreateToc(TestSectorCount);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }

    // ── Zero offset (passthrough) ───────────────────────────────

    [Fact]
    public async Task ZeroOffset_PassthroughUnchanged()
    {
        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples: 0);

        byte[] buffer = new byte[CdConstants.SectorSize];
        int read = await drive.ReadSectorsAsync(5, 1, buffer);

        Assert.Equal(1, read);

        // Should match the file data exactly
        for (int i = 0; i < CdConstants.SectorSize; i++)
        {
            Assert.Equal(ExpectedByte(lba: 5, offset: i), buffer[i]);
        }
    }

    // ── Positive offset (drive reads ahead) ─────────────────────

    [Fact]
    public async Task PositiveOffset_ShiftsDataBackward()
    {
        // Offset +6 sample frames = +24 bytes. Drive reads 24 bytes ahead,
        // so corrected data should come from 24 bytes earlier in the file.
        int offsetSamples = 6;
        int offsetBytes = offsetSamples * CdConstants.BytesPerSampleFrame;

        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples);

        byte[] buffer = new byte[CdConstants.SectorSize];
        await drive.ReadSectorsAsync(5, 1, buffer);

        // The corrected data for LBA 5 should be the file bytes starting at
        // (5 * 2352 - 24), which is 24 bytes before the nominal sector start.
        long expectedFileOffset = 5L * CdConstants.SectorSize - offsetBytes;

        for (int i = 0; i < CdConstants.SectorSize; i++)
        {
            long filePos = expectedFileOffset + i;
            long fileLba = filePos / CdConstants.SectorSize;
            int fileOff = (int)(filePos % CdConstants.SectorSize);
            Assert.Equal(ExpectedByte(fileLba, fileOff), buffer[i]);
        }
    }

    // ── Negative offset (drive reads behind) ────────────────────

    [Fact]
    public async Task NegativeOffset_ShiftsDataForward()
    {
        // Offset -6 sample frames = -24 bytes. Drive reads 24 bytes behind,
        // so corrected data should come from 24 bytes later in the file.
        int offsetSamples = -6;
        int offsetBytes = offsetSamples * CdConstants.BytesPerSampleFrame;

        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples);

        byte[] buffer = new byte[CdConstants.SectorSize];
        await drive.ReadSectorsAsync(5, 1, buffer);

        // Corrected data for LBA 5 comes from file offset (5 * 2352 + 24)
        long expectedFileOffset = 5L * CdConstants.SectorSize - offsetBytes;

        for (int i = 0; i < CdConstants.SectorSize; i++)
        {
            long filePos = expectedFileOffset + i;
            long fileLba = filePos / CdConstants.SectorSize;
            int fileOff = (int)(filePos % CdConstants.SectorSize);
            Assert.Equal(ExpectedByte(fileLba, fileOff), buffer[i]);
        }
    }

    // ── Edge: offset pushes read before LBA 0 ───────────────────

    [Fact]
    public async Task PositiveOffset_BeforeLba0_ZeroPads()
    {
        // Large positive offset reading from LBA 0 — needs data from before
        // the disc start, which should be zero-padded.
        int offsetSamples = CdConstants.SampleFramesPerSector; // one full sector
        int offsetBytes = offsetSamples * CdConstants.BytesPerSampleFrame;

        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples);

        byte[] buffer = new byte[CdConstants.SectorSize];
        await drive.ReadSectorsAsync(0, 1, buffer);

        // First offsetBytes should be zero (from before LBA 0)
        for (int i = 0; i < offsetBytes; i++)
        {
            Assert.Equal(0, buffer[i]);
        }

        // Remaining bytes should be from file offset 0
        for (int i = offsetBytes; i < CdConstants.SectorSize; i++)
        {
            long filePos = i - offsetBytes;
            Assert.Equal(ExpectedByte(0, (int)filePos), buffer[i]);
        }
    }

    // ── Edge: offset pushes read past lead-out ──────────────────

    [Fact]
    public async Task NegativeOffset_PastLeadOut_ZeroPads()
    {
        // Read last sector with negative offset — needs data past lead-out.
        int offsetSamples = -CdConstants.SampleFramesPerSector;
        int offsetBytes = Math.Abs(offsetSamples * CdConstants.BytesPerSampleFrame);

        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples);

        byte[] buffer = new byte[CdConstants.SectorSize];
        await drive.ReadSectorsAsync(TestSectorCount - 1, 1, buffer);

        // First (SectorSize - offsetBytes) bytes come from file
        int fromFile = CdConstants.SectorSize - offsetBytes;

        for (int i = 0; i < fromFile; i++)
        {
            long filePos = (TestSectorCount - 1L) * CdConstants.SectorSize + offsetBytes + i;
            long fileLba = filePos / CdConstants.SectorSize;
            int fileOff = (int)(filePos % CdConstants.SectorSize);
            Assert.Equal(ExpectedByte(fileLba, fileOff), buffer[i]);
        }

        // Remaining bytes should be zero-padded (past lead-out)
        for (int i = fromFile; i < CdConstants.SectorSize; i++)
        {
            Assert.Equal(0, buffer[i]);
        }
    }

    // ── Multi-sector read with offset ───────────────────────────

    [Fact]
    public async Task PositiveOffset_MultiSector_AllShifted()
    {
        int offsetSamples = 6;
        int offsetBytes = offsetSamples * CdConstants.BytesPerSampleFrame;

        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples);

        int count = 3;
        byte[] buffer = new byte[CdConstants.GetReadBufferSize(ReadOptions.None, count)];
        await drive.ReadSectorsAsync(5, count, buffer);

        for (int s = 0; s < count; s++)
        {
            int sectorStart = s * CdConstants.SectorSize;
            long expectedFileOffset = (5L + s) * CdConstants.SectorSize - offsetBytes;

            for (int i = 0; i < CdConstants.SectorSize; i++)
            {
                long filePos = expectedFileOffset + i;
                long fileLba = filePos / CdConstants.SectorSize;
                int fileOff = (int)(filePos % CdConstants.SectorSize);
                Assert.Equal(ExpectedByte(fileLba, fileOff), buffer[sectorStart + i]);
            }
        }
    }

    // ── Inquiry delegation ──────────────────────────────────────

    [Fact]
    public void Inquiry_DelegatesToInner()
    {
        var custom = new DriveInquiry { Vendor = "PLEXTOR", Product = "PX-716A", Revision = "1.04" };
        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc, inquiry: custom);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples: 100);

        Assert.Equal("PLEXTOR", drive.Inquiry.Vendor);
        Assert.Equal("PX-716A", drive.Inquiry.Product);
    }

    // ── Buffer validation ───────────────────────────────────────

    [Fact]
    public async Task BufferTooSmall_ThrowsArgumentException()
    {
        using var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var drive = new OffsetCorrectingDrive(inner, offsetSamples: 6);

        byte[] tooSmall = new byte[10];

        await Assert.ThrowsAsync<ArgumentException>(
            () => drive.ReadSectorsAsync(0, 1, tooSmall));
    }

    // ── Dispose ─────────────────────────────────────────────────

    [Fact]
    public async Task Dispose_DisposesInner()
    {
#pragma warning disable CA2000 // inner is intentionally owned by drive — testing that drive disposes it
        var inner = new FileBackedOpticalDrive(_testFilePath, _toc);
        var drive = new OffsetCorrectingDrive(inner, offsetSamples: 0);
#pragma warning restore CA2000

        drive.Dispose();

        // Inner should now be disposed — reading its TOC should throw
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => inner.ReadTocAsync());
    }

    // ── Helpers ─────────────────────────────────────────────────

    private static byte ExpectedByte(long lba, int offset) => (byte)((lba + offset) & 0xFF);

    private string CreateTestFile(int sectorCount)
    {
        string path = Path.Combine(_tempDir, "test.bin");
        byte[] data = new byte[sectorCount * CdConstants.SectorSize];

        for (int i = 0; i < sectorCount; i++)
        {
            int start = i * CdConstants.SectorSize;

            for (int j = 0; j < CdConstants.SectorSize; j++)
            {
                data[start + j] = ExpectedByte(i, j);
            }
        }

        File.WriteAllBytes(path, data);
        return path;
    }

    private static TableOfContents CreateToc(int sectorCount)
    {
        return new TableOfContents
        {
            FirstTrackNumber = 1,
            LastTrackNumber = 1,
            LeadOutLba = sectorCount,
            Tracks = new ReadOnlyCollection<TrackInfo>(
            [
                new TrackInfo
                {
                    Number = 1,
                    StartLba = 0,
                    SectorCount = sectorCount,
                    Type = TrackType.Audio,
                    Control = TrackControl.None,
                },
            ]),
        };
    }
}
