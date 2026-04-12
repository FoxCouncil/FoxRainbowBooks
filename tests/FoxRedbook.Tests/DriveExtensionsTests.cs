using System.Collections.ObjectModel;

namespace FoxRedbook.Tests;

/// <summary>
/// Tests for <see cref="DriveExtensions.ReadDiscInfoAsync"/> and the related
/// <see cref="DiscInfo.CdText"/> aggregate wiring. Verifies that the high-level
/// convenience path populates every field, tolerates drives that throw on
/// <see cref="IOpticalDrive.ReadCdTextAsync"/>, and that the pure-function
/// <see cref="DiscFingerprint.Compute"/> path leaves <c>CdText</c> null.
/// </summary>
public sealed class DriveExtensionsTests : IDisposable
{
    private const int TestSectorCount = 10;

    private readonly string _tempDir;
    private readonly string _testFilePath;
    private readonly TableOfContents _toc;

    public DriveExtensionsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"foxredbook_driveext_{Guid.NewGuid():N}");
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

    // ── Happy path ─────────────────────────────────────────────

    [Fact]
    public async Task ReadDiscInfoAsync_PopulatesAllFields_FromCleanDisc()
    {
        var cdText = new CdText
        {
            AlbumTitle = "Hello World",
            AlbumPerformer = "The Example Band",
        };

        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc, cdText: cdText);

        DiscInfo info = await drive.ReadDiscInfoAsync();

        Assert.Same(_toc, info.Toc);
        Assert.Equal(28, info.MusicBrainzDiscId.Length);
        Assert.NotEqual(0u, info.FreedbDiscId);
        Assert.NotEqual(0u, info.AccurateRipId1);
        Assert.NotEqual(0u, info.AccurateRipId2);
        Assert.NotNull(info.CdText);
        Assert.Equal("Hello World", info.CdText!.AlbumTitle);
        Assert.Equal("The Example Band", info.CdText.AlbumPerformer);
    }

    [Fact]
    public async Task ReadDiscInfoAsync_NullCdText_WhenDriveHasNone()
    {
        // FileBackedOpticalDrive defaults cdText to null — mimics a disc
        // with no CD-Text data (the common case).
        using var drive = new FileBackedOpticalDrive(_testFilePath, _toc);

        DiscInfo info = await drive.ReadDiscInfoAsync();

        Assert.NotNull(info.Toc);
        Assert.Null(info.CdText);
    }

    [Fact]
    public async Task ReadDiscInfoAsync_NullCdText_WhenDriveThrows()
    {
        // Drives that return an error (rather than null) when the disc has
        // no CD-Text must not fail the whole disc-info read.
        using var drive = new ThrowingCdTextDrive(_toc);

        DiscInfo info = await drive.ReadDiscInfoAsync();

        Assert.Null(info.CdText);
        Assert.Equal(28, info.MusicBrainzDiscId.Length);
    }

    [Fact]
    public async Task ReadDiscInfoAsync_NonOpticalException_Propagates()
    {
        // Only OpticalDriveException should be swallowed. Other exceptions
        // (cancellation, argument errors, bugs) must surface to the caller.
        using var drive = new GenericThrowingCdTextDrive(_toc);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => drive.ReadDiscInfoAsync());
    }

    [Fact]
    public async Task ReadDiscInfoAsync_NullDrive_ThrowsArgumentNullException()
    {
        IOpticalDrive drive = null!;
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => drive.ReadDiscInfoAsync());
    }

    // ── DiscFingerprint.Compute path ──────────────────────────

    [Fact]
    public void DiscFingerprint_Compute_LeavesCdTextNull()
    {
        // The pure-function path has no drive access and cannot populate
        // CD-Text. This is a contract the docs promise — lock it in a test.
        DiscInfo info = DiscFingerprint.Compute(_toc);

        Assert.Null(info.CdText);
    }

    // ── DiscInfo with-expression safety ───────────────────────

    [Fact]
    public void DiscInfo_WithCdText_PreservesAllOtherFields()
    {
        DiscInfo original = DiscFingerprint.Compute(_toc);
        var cdText = new CdText { AlbumTitle = "Test" };

        DiscInfo updated = original with { CdText = cdText };

        Assert.Same(original.Toc, updated.Toc);
        Assert.Equal(original.MusicBrainzDiscId, updated.MusicBrainzDiscId);
        Assert.Equal(original.FreedbDiscId, updated.FreedbDiscId);
        Assert.Equal(original.AccurateRipId1, updated.AccurateRipId1);
        Assert.Equal(original.AccurateRipId2, updated.AccurateRipId2);
        Assert.Same(cdText, updated.CdText);
    }

    // ── Helpers ──────────────────────────────────────────────

    private string CreateTestFile(int sectorCount)
    {
        string path = Path.Combine(_tempDir, "test.bin");
        byte[] data = new byte[sectorCount * CdConstants.SectorSize];
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

    /// <summary>
    /// Stub drive that serves a pre-built TOC but throws
    /// <see cref="OpticalDriveException"/> from <c>ReadCdTextAsync</c> —
    /// matches drives that signal "no CD-Text on this disc" via an error
    /// rather than an empty response.
    /// </summary>
    private sealed class ThrowingCdTextDrive : IOpticalDrive
    {
        private readonly TableOfContents _toc;

        public ThrowingCdTextDrive(TableOfContents toc)
        {
            _toc = toc;
        }

        public DriveInquiry Inquiry => new()
        {
            Vendor = "TEST",
            Product = "Throws",
            Revision = "1.0",
        };

        public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(_toc);

        public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
            => throw new OpticalDriveException("Simulated: drive has no CD-Text.");

        public Task<int> ReadSectorsAsync(long lba, int count, Memory<byte> buffer, ReadOptions flags = ReadOptions.None, CancellationToken cancellationToken = default)
            => Task.FromResult(count);

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// Variant that throws <see cref="InvalidOperationException"/> — used
    /// to verify that <c>ReadDiscInfoAsync</c>'s catch is scoped to
    /// <see cref="OpticalDriveException"/> and doesn't swallow other errors.
    /// </summary>
    private sealed class GenericThrowingCdTextDrive : IOpticalDrive
    {
        private readonly TableOfContents _toc;

        public GenericThrowingCdTextDrive(TableOfContents toc)
        {
            _toc = toc;
        }

        public DriveInquiry Inquiry => new()
        {
            Vendor = "TEST",
            Product = "ThrowsGeneric",
            Revision = "1.0",
        };

        public Task<TableOfContents> ReadTocAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(_toc);

        public Task<CdText?> ReadCdTextAsync(CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("Simulated bug.");

        public Task<int> ReadSectorsAsync(long lba, int count, Memory<byte> buffer, ReadOptions flags = ReadOptions.None, CancellationToken cancellationToken = default)
            => Task.FromResult(count);

        public void Dispose() { }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
