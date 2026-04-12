using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

public sealed class WiggleEngineTests : IDisposable
{
    private const int TestSectorCount = 20;

    private readonly string _tempDir;
    private readonly string _testFilePath;
    private readonly TableOfContents _toc;

    public WiggleEngineTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxredbook_engine_{Guid.NewGuid():N}");
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

    // ── Clean reads ─────────────────────────────────────────────

    [Fact]
    public void CleanRead_SingleSector_ReturnsCorrectData()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var engine = new WiggleEngine(drive, new RipOptions());

        byte[] buffer = new byte[CdConstants.SectorSize];
        engine.ReadVerifiedSector(buffer, 5, out var status, out int reReads, CancellationToken.None);

        // Verify the output matches the file data for LBA 5
        for (int i = 0; i < CdConstants.SectorSize; i++)
        {
            Assert.Equal(ExpectedByte(lba: 5, offset: i), buffer[i]);
        }
    }

    [Fact]
    public void CleanRead_SequentialSectors_AllCorrect()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var engine = new WiggleEngine(drive, new RipOptions());

        byte[] buffer = new byte[CdConstants.SectorSize];

        for (int lba = 0; lba < 5; lba++)
        {
            engine.ReadVerifiedSector(buffer, lba, out _, out _, CancellationToken.None);

            for (int i = 0; i < CdConstants.SectorSize; i++)
            {
                Assert.Equal(ExpectedByte(lba, i), buffer[i]);
            }
        }
    }

    [Fact]
    public void CleanRead_BufferTooSmall_ThrowsArgumentException()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var engine = new WiggleEngine(drive, new RipOptions());

        byte[] tooSmall = new byte[100];

        Assert.Throws<ArgumentException>(
            () => engine.ReadVerifiedSector(tooSmall, 0, out _, out _, CancellationToken.None));
    }

    // ── Bit flip recovery ───────────────────────────────────────

    [Fact]
    public void BitFlip_EngineConverges_ReturnsCorrectData()
    {
        // Corrupt only the FIRST read of LBAs 5-6. The second read returns
        // clean data. Cross-verification between the corrupted first read and
        // the clean second read will mismatch at the corrupted byte, forcing
        // re-reads. The third read is also clean, matches the second, and the
        // engine converges on correct data.
        //
        // CorruptReads=1 is essential: if both reads are identically corrupted,
        // cross-verification accepts the corruption as correct — that's a
        // fundamental property of the algorithm, not a bug.
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 5,
                    EndLba = 7,
                    ByteOffset = 100,
                    XorMask = 0xFF,
                    CorruptReads = 1,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        using var engine = new WiggleEngine(drive, new RipOptions { MaxReReads = 20 });

        byte[] buffer = new byte[CdConstants.SectorSize];
        engine.ReadVerifiedSector(buffer, 5, out var status, out int reReads, CancellationToken.None);

        // The engine should have recovered the correct data
        Assert.Equal(ExpectedByte(lba: 5, offset: 100), buffer[100]);

        // Should have needed at least one re-read
        Assert.True(reReads >= 1);
    }

    // ── Transient failure recovery ──────────────────────────────

    [Fact]
    public void TransientFailure_EngineRetries_EventuallySucceeds()
    {
        var profile = new ReadErrorProfile
        {
            TransientFailures =
            [
                new TransientFault
                {
                    StartLba = 3,
                    EndLba = 8,
                    FailureCount = 2,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        using var engine = new WiggleEngine(drive, new RipOptions { MaxReReads = 20 });

        byte[] buffer = new byte[CdConstants.SectorSize];
        engine.ReadVerifiedSector(buffer, 5, out var status, out int reReads, CancellationToken.None);

        // Should have gotten correct data after retries
        for (int i = 0; i < CdConstants.SectorSize; i++)
        {
            Assert.Equal(ExpectedByte(lba: 5, offset: i), buffer[i]);
        }

        // ReadError flag should have been set from the transient failures
        Assert.True((status & SectorStatus.ReadError) != 0 || reReads >= 1);
    }

    // ── Max retries → skip ──────────────────────────────────────

    [Fact]
    public void MaxRetries_Exhausted_SkipsFlagSet()
    {
        // Every read throws — engine can never get data, must skip.
        // Persistent transient failures are the correct way to force retry
        // exhaustion: the engine gets ReadError on every attempt, never
        // builds a root, and eventually gives up.
        var profile = new ReadErrorProfile
        {
            TransientFailures =
            [
                new TransientFault
                {
                    StartLba = 0,
                    EndLba = 20,
                    FailureCount = 1000,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        using var engine = new WiggleEngine(drive, new RipOptions { MaxReReads = 5 });

        byte[] buffer = new byte[CdConstants.SectorSize];
        engine.ReadVerifiedSector(buffer, 5, out var status, out _, CancellationToken.None);

        // Engine should have given up and set the Skipped flag
        Assert.True((status & SectorStatus.Skipped) != 0);
    }

    // ── Cancellation ────────────────────────────────────────────

    [Fact]
    public void Cancellation_ThrowsOperationCanceledException()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var engine = new WiggleEngine(drive, new RipOptions());

        byte[] buffer = new byte[CdConstants.SectorSize];
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(
            () => engine.ReadVerifiedSector(buffer, 0, out _, out _, cts.Token));
    }

    // ── Dispose ─────────────────────────────────────────────────

    [Fact]
    public void Dispose_ThenRead_ThrowsObjectDisposedException()
    {
        var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        var engine = new WiggleEngine(drive, new RipOptions());
        engine.Dispose();

        byte[] buffer = new byte[CdConstants.SectorSize];

        Assert.Throws<ObjectDisposedException>(
            () => engine.ReadVerifiedSector(buffer, 0, out _, out _, CancellationToken.None));

        drive.Dispose();
    }

    // ── RipSession integration ──────────────────────────────────

    [Fact]
    public async Task RipSession_CleanDisc_YieldsAllSectors()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0];
        int sectorIndex = 0;

        await foreach (var sector in session.RipTrackAsync(track))
        {
            long expectedLba = track.StartLba + sectorIndex;
            Assert.Equal(expectedLba, sector.Lba);
            Assert.Equal(CdConstants.SectorSize, sector.Pcm.Length);

            // Verify PCM data
            var pcm = sector.Pcm.Span;

            for (int i = 0; i < CdConstants.SectorSize; i++)
            {
                Assert.Equal(ExpectedByte(expectedLba, i), pcm[i]);
            }

            sectorIndex++;
        }

        Assert.Equal(track.SectorCount, sectorIndex);
    }

    [Fact]
    public async Task RipSession_ReportsProgress()
    {
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0];
        var reports = new List<RipProgress>();
        var progress = new Progress<RipProgress>(r => reports.Add(r));

        await foreach (var _ in session.RipTrackAsync(track, progress))
        {
            // consume all sectors
        }

        // Progress should have been reported for each sector.
        // Note: Progress<T> posts to SynchronizationContext which may not
        // deliver synchronously in all test environments.
        // Give it a moment for reports to arrive.
        await Task.Delay(50);

        Assert.True(reports.Count > 0);
        Assert.All(reports, r => Assert.Equal(track.SectorCount, r.TotalSectors));
    }

    [Fact]
    public async Task RipSession_BitFlipRecovery_YieldsCorrectData()
    {
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 3,
                    EndLba = 5,
                    ByteOffset = 50,
                    XorMask = 0xAA,
                    CorruptReads = 1,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0];
        int count = 0;

        await foreach (var sector in session.RipTrackAsync(track))
        {
            // All sectors — including corrupted ones — should have correct data
            var pcm = sector.Pcm.Span;

            for (int i = 0; i < CdConstants.SectorSize; i++)
            {
                if (pcm[i] != ExpectedByte(sector.Lba, i))
                {
                    Assert.Fail($"LBA {sector.Lba} byte {i}: expected {ExpectedByte(sector.Lba, i)}, got {pcm[i]}");
                }
            }

            count++;
        }

        Assert.Equal(track.SectorCount, count);
    }

    [Fact]
    public void Sector0_CleanDisc_IsVerifiedNotSkipped()
    {
        // Diagnostic: confirm that sector 0 gets verified data, not force-accepted
        // data from the retry-exhausted fallback. If the silence prefix approach
        // doesn't actually enable verification of the first samples, this test
        // catches it.
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);
        using var engine = new WiggleEngine(drive, new RipOptions());

        byte[] buffer = new byte[CdConstants.SectorSize];
        engine.ReadVerifiedSector(buffer, 0, out var status, out int reReads, CancellationToken.None);

        Assert.False((status & SectorStatus.Skipped) != 0,
            $"Sector 0 should not be Skipped on a clean disc. Status: {status}, ReReads: {reReads}");
    }

    // ── Regression tests ────────────────────────────────────────

    /// <summary>
    /// Regression test for the fragment merge mutation bug: after an Append
    /// extends _root.End, a naive Fill check against the mutated End would
    /// fire spuriously and overwrite rift-corrected data at the wrong offset.
    /// The fix snapshots root dimensions BEFORE any mutation. This test
    /// exercises the merge path by rip-streaming a small track with a bit-flip
    /// fault positioned to produce fragments that partially overlap and
    /// extend the root — the exact configuration that used to trigger the bug.
    /// </summary>
    [Fact]
    public async Task FragmentMergeMutation_RegressionTest()
    {
        var profile = new ReadErrorProfile
        {
            BitFlips =
            [
                new BitFlipFault
                {
                    StartLba = 2,
                    EndLba = 3,
                    ByteOffset = 100,
                    XorMask = 0x7F,
                    CorruptReads = 1,
                },
            ],
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, errorProfile: profile);
        using var session = new RipSession(drive);

        var track = _toc.Tracks[0];

        // Rip only the first 6 sectors — narrow enough that a merge mutation
        // error shows up immediately but broad enough to exercise multi-read
        // verification.
        int verified = 0;
        int sectorsToCheck = 6;

        await foreach (var sector in session.RipTrackAsync(track))
        {
            if (verified >= sectorsToCheck)
            {
                verified++;
                continue;
            }

            var pcm = sector.Pcm.Span;

            for (int i = 0; i < CdConstants.SectorSize; i++)
            {
                Assert.Equal(ExpectedByte(sector.Lba, i), pcm[i]);
            }

            verified++;
        }
    }

    /// <summary>
    /// Regression test for the adjacent-fragment bridge hard cap. The engine
    /// zero-fills gaps up to 2 × OverlapAdj (62 samples) between fragments
    /// from independent verification runs, because the OverlapAdj trim can
    /// create gaps that small without any actual missing disc data. A gap
    /// wider than 62 samples represents a real verification failure and
    /// must not be silently bridged — the engine must fall through to its
    /// normal retry / skip path.
    /// </summary>
    [Fact]
    public void AdjacentFragmentBridge_HardCap_ConstantIsCorrect()
    {
        // The cap lives in WiggleEngine.TryMergeFragment.
        // Verify the constant it's derived from is the expected value.
        const int ExpectedCap = 62;
        Assert.Equal(ExpectedCap, WiggleConstants.OverlapAdj * 2);
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Test data pattern with 16-bit sample uniqueness. The engine compares
    /// 16-bit samples (not bytes), so we need sample values that don't repeat
    /// within the dynamic overlap window. A naive byte pattern like
    /// <c>(lba + offset) &amp; 0xFF</c> produces sample values with period 128
    /// — only 2× MinWordsSearch, which causes false matches in FindOverlap.
    /// This pattern derives both bytes of each sample from the absolute sample
    /// index multiplied by a prime coprime to 65536, giving a full 65536 period.
    /// </summary>
    private static byte ExpectedByte(long lba, int offset)
    {
        long absoluteSample = lba * (CdConstants.SectorSize / sizeof(short)) + offset / sizeof(short);
        int sampleValue = (int)((absoluteSample * 12347 + 1) & 0xFFFF);

        return (offset % sizeof(short)) == 0
            ? (byte)(sampleValue & 0xFF)
            : (byte)((sampleValue >> 8) & 0xFF);
    }

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
