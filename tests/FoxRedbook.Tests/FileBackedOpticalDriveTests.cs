using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

public sealed class FileBackedOpticalDriveTests : IDisposable
{
    private const int TestSectorCount = 10;

    private readonly string _tempDir;
    private readonly string _testFilePath;
    private readonly TableOfContents _toc;

    public FileBackedOpticalDriveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxredbook_test_{Guid.NewGuid():N}");
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

    // ── Clean read ──────────────────────────────────────────────────

    [Fact]
    public async Task ReadSectorsAsync_CleanRead_ReturnsCorrectData()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        byte[] buffer = new byte[CdConstants.GetReadBufferSize(ReadOptions.None, 1)];
        int read = await drive.ReadSectorsAsync(0, 1, buffer);

        Assert.Equal(1, read);

        // Verify the sector matches what we wrote
        for (int j = 0; j < CdConstants.SectorSize; j++)
        {
            Assert.Equal(ExpectedByte(lba: 0, offset: j), buffer[j]);
        }
    }

    [Fact]
    public async Task ReadSectorsAsync_MultiSectorRead_ReturnsAllSectors()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        int count = 3;
        byte[] buffer = new byte[CdConstants.GetReadBufferSize(ReadOptions.None, count)];
        int read = await drive.ReadSectorsAsync(2, count, buffer);

        Assert.Equal(count, read);

        for (int i = 0; i < count; i++)
        {
            int sectorStart = i * CdConstants.SectorSize;
            long lba = 2 + i;

            for (int j = 0; j < CdConstants.SectorSize; j++)
            {
                Assert.Equal(ExpectedByte(lba, j), buffer[sectorStart + j]);
            }
        }
    }

    [Fact]
    public async Task ReadSectorsAsync_PastLeadOut_ClampsSectorCount()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        byte[] buffer = new byte[CdConstants.GetReadBufferSize(ReadOptions.None, 5)];
        int read = await drive.ReadSectorsAsync(TestSectorCount - 2, 5, buffer);

        Assert.Equal(2, read);
    }

    // ── TOC ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ReadTocAsync_ReturnsInjectedToc()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        var toc = await drive.ReadTocAsync();

        Assert.Same(_toc, toc);
    }

    // ── Inquiry ─────────────────────────────────────────────────────

    [Fact]
    public void Inquiry_DefaultValues()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        Assert.Equal("TEST", drive.Inquiry.Vendor);
        Assert.Equal("FileBackedDrive", drive.Inquiry.Product);
        Assert.Equal("1.0", drive.Inquiry.Revision);
    }

    [Fact]
    public void Inquiry_CustomValues()
    {
        var custom = new DriveInquiry { Vendor = "PLEXTOR", Product = "PX-716A", Revision = "1.04" };
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, inquiry: custom);

        Assert.Equal("PLEXTOR - PX-716A", drive.Inquiry.OffsetDatabaseKey);
    }

    // ── Buffer validation ───────────────────────────────────────────

    [Fact]
    public async Task ReadSectorsAsync_BufferTooSmall_ThrowsArgumentException()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        byte[] tooSmall = new byte[CdConstants.SectorSize - 1];

        await Assert.ThrowsAsync<ArgumentException>(
            () => drive.ReadSectorsAsync(0, 1, tooSmall));
    }

    [Fact]
    public async Task ReadSectorsAsync_BufferTooSmallForC2_ThrowsArgumentException()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        // Big enough for audio only, but not for audio + C2
        byte[] buffer = new byte[CdConstants.SectorSize];

        await Assert.ThrowsAsync<ArgumentException>(
            () => drive.ReadSectorsAsync(0, 1, buffer, ReadOptions.C2ErrorPointers));
    }

    // ── Dispose ─────────────────────────────────────────────────────

    [Fact]
    public async Task ReadSectorsAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        drive.Dispose();

        byte[] buffer = new byte[CdConstants.SectorSize];

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => drive.ReadSectorsAsync(0, 1, buffer));
    }

    [Fact]
    public async Task ReadTocAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        drive.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => drive.ReadTocAsync());
    }

    [Fact]
    public void Dispose_MultipleCalls_DoesNotThrow()
    {
        var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        drive.Dispose();
        drive.Dispose();
    }

    // ── BitFlipFault ────────────────────────────────────────────────

    [Fact]
    public async Task BitFlipFault_CorruptsFirstNReads_ThenReturnsCleanData()
    {
        int corruptReads = 2;
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 0,
                    EndLba = 1,
                    ByteOffset = 10,
                    XorMask = 0xFF,
                    CorruptReads = corruptReads,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];
        byte expectedClean = ExpectedByte(lba: 0, offset: 10);

        // First two reads should be corrupted (attempts 1 and 2)
        for (int attempt = 1; attempt <= corruptReads; attempt++)
        {
            await drive.ReadSectorsAsync(0, 1, buffer);
            Assert.Equal((byte)(expectedClean ^ 0xFF), buffer[10]);
        }

        // Third read should be clean (attempt 3, past CorruptReads threshold)
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedClean, buffer[10]);
    }

    [Fact]
    public async Task BitFlipFault_OnlyAffectsConfiguredLbaRange()
    {
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 5,
                    EndLba = 7,
                    ByteOffset = 0,
                    XorMask = 0xFF,
                    CorruptReads = 10,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // LBA 4 — outside range, should be clean
        await drive.ReadSectorsAsync(4, 1, buffer);
        Assert.Equal(ExpectedByte(lba: 4, offset: 0), buffer[0]);

        // LBA 5 — inside range, should be corrupted
        await drive.ReadSectorsAsync(5, 1, buffer);
        Assert.Equal((byte)(ExpectedByte(lba: 5, offset: 0) ^ 0xFF), buffer[0]);

        // LBA 7 — outside range (exclusive end), should be clean
        await drive.ReadSectorsAsync(7, 1, buffer);
        Assert.Equal(ExpectedByte(lba: 7, offset: 0), buffer[0]);
    }

    // ── C2ErrorFault ────────────────────────────────────────────────

    [Fact]
    public async Task C2ErrorFault_SetsCorrectBitsInC2Block()
    {
        var profile = new ReadErrorProfile
        {
            C2Errors =
            [
                new C2ErrorFault
                {
                    StartLba = 0,
                    EndLba = 1,
                    ByteOffset = 8,
                    ByteCount = 3,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.GetReadBufferSize(ReadOptions.C2ErrorPointers, 1)];

        await drive.ReadSectorsAsync(0, 1, buffer, ReadOptions.C2ErrorPointers);

        // C2 data starts after audio
        int c2Start = CdConstants.SectorSize;

        // Bytes 8, 9, 10 should be flagged. All three are in C2 byte index 1 (bits 0, 1, 2).
        Assert.Equal(0b00000111, buffer[c2Start + 1]);

        // C2 byte 0 should be clean (bytes 0–7 not flagged)
        Assert.Equal(0, buffer[c2Start + 0]);
    }

    [Fact]
    public async Task C2ErrorFault_NotReturnedWithoutC2Flag()
    {
        var profile = new ReadErrorProfile
        {
            C2Errors =
            [
                new C2ErrorFault { StartLba = 0, EndLba = 1, ByteOffset = 0, ByteCount = 10 },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // Read without C2 flag — buffer is exactly SectorSize, no C2 block
        int read = await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(1, read);
    }

    // ── JitterFault ─────────────────────────────────────────────────

    [Fact]
    public async Task JitterFault_ReturnsShiftedDataThenCorrectData()
    {
        int jitterReads = 2;
        int shiftFrames = 1; // 4 bytes forward

        var profile = new ReadErrorProfile
        {
            JitterFaults =
            [
                new JitterFault
                {
                    StartLba = 3,
                    EndLba = 4,
                    SampleFrameShift = shiftFrames,
                    JitterReads = jitterReads,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // First read should be jittered — data comes from 4 bytes later in the file
        await drive.ReadSectorsAsync(3, 1, buffer);
        int shiftBytes = shiftFrames * CdConstants.BytesPerSampleFrame;

        for (int j = 0; j < CdConstants.SectorSize - shiftBytes; j++)
        {
            // The data at buffer[j] should be file byte (3 * 2352 + shiftBytes + j)
            byte expected = (byte)((3 + ((shiftBytes + j) / CdConstants.SectorSize))
                + ((shiftBytes + j) % CdConstants.SectorSize)) ;
            // Actually, let's compute this properly using the file layout
            long fileByteIndex = 3L * CdConstants.SectorSize + shiftBytes + j;
            long fileLba = fileByteIndex / CdConstants.SectorSize;
            int fileOffset = (int)(fileByteIndex % CdConstants.SectorSize);
            byte expectedByte = ExpectedByte(fileLba, fileOffset);
            Assert.Equal(expectedByte, buffer[j]);
        }

        // Read again (attempt 2, still jittered)
        await drive.ReadSectorsAsync(3, 1, buffer);

        // Read again (attempt 3, should be clean now)
        drive.ResetAttemptCounts();

        // After reset, we need 3 reads to get past jitterReads=2
        await drive.ReadSectorsAsync(3, 1, buffer); // attempt 1 - jittered
        await drive.ReadSectorsAsync(3, 1, buffer); // attempt 2 - jittered
        await drive.ReadSectorsAsync(3, 1, buffer); // attempt 3 - clean

        for (int j = 0; j < CdConstants.SectorSize; j++)
        {
            Assert.Equal(ExpectedByte(lba: 3, offset: j), buffer[j]);
        }
    }

    // ── TransientFault ──────────────────────────────────────────────

    [Fact]
    public async Task TransientFault_ThrowsOnFirstNReads_ThenSucceeds()
    {
        int failureCount = 3;
        var profile = new ReadErrorProfile
        {
            TransientFailures =
            [
                new TransientFault
                {
                    StartLba = 0,
                    EndLba = 1,
                    FailureCount = failureCount,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // First 3 reads should throw
        for (int i = 0; i < failureCount; i++)
        {
            await Assert.ThrowsAsync<OpticalDriveException>(
                () => drive.ReadSectorsAsync(0, 1, buffer));
        }

        // Fourth read should succeed
        int read = await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(1, read);
        Assert.Equal(ExpectedByte(lba: 0, offset: 0), buffer[0]);
    }

    [Fact]
    public async Task TransientFault_DoesNotCorruptBuffer()
    {
        var profile = new ReadErrorProfile
        {
            TransientFailures =
            [
                new TransientFault { StartLba = 0, EndLba = 1, FailureCount = 1 },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);

        byte sentinel = 0xAB;
        byte[] buffer = new byte[CdConstants.SectorSize];
        Array.Fill(buffer, sentinel);

        // The throw should happen before any data is written
        await Assert.ThrowsAsync<OpticalDriveException>(
            () => drive.ReadSectorsAsync(0, 1, buffer));

        Assert.All(buffer, b => Assert.Equal(sentinel, b));
    }

    // ── ResetAttemptCounts ──────────────────────────────────────────

    [Fact]
    public async Task ResetAttemptCounts_ReArmsFaults()
    {
        var profile = new ReadErrorProfile
        {
            TransientFailures =
            [
                new TransientFault { StartLba = 0, EndLba = 1, FailureCount = 1 },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];

        // First read throws
        await Assert.ThrowsAsync<OpticalDriveException>(
            () => drive.ReadSectorsAsync(0, 1, buffer));

        // Second read succeeds
        await drive.ReadSectorsAsync(0, 1, buffer);

        // Reset and verify the fault fires again
        drive.ResetAttemptCounts();

        await Assert.ThrowsAsync<OpticalDriveException>(
            () => drive.ReadSectorsAsync(0, 1, buffer));
    }

    [Fact]
    public async Task ResetAttemptCounts_ReArmsBitFlipFault()
    {
        int corruptReads = 2;
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 0,
                    EndLba = 1,
                    ByteOffset = 0,
                    XorMask = 0xFF,
                    CorruptReads = corruptReads,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        byte[] buffer = new byte[CdConstants.SectorSize];
        byte expectedClean = ExpectedByte(lba: 0, offset: 0);
        byte expectedCorrupt = (byte)(expectedClean ^ 0xFF);

        // Exhaust the fault: reads 1 and 2 are corrupted, read 3 is clean
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedCorrupt, buffer[0]);
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedCorrupt, buffer[0]);
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedClean, buffer[0]);

        // Confirm it stays clean
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedClean, buffer[0]);

        // Reset — fault should fire again from attempt 1
        drive.ResetAttemptCounts();

        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedCorrupt, buffer[0]);
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedCorrupt, buffer[0]);
        await drive.ReadSectorsAsync(0, 1, buffer);
        Assert.Equal(expectedClean, buffer[0]);
    }

    // ── Helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Returns the expected byte value at a given LBA and offset within the sector,
    /// matching the pattern written by <see cref="CreateTestFile"/>.
    /// </summary>
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
