# FoxRainbowBooks

Cross-platform, AOT-compatible .NET 8 libraries for optical disc I/O — named for the [Rainbow Books](https://en.wikipedia.org/wiki/Rainbow_Books) that define the CD family of standards.

[![NuGet FoxRedbook](https://img.shields.io/nuget/v/FoxRedbook.svg?label=FoxRedbook)](https://www.nuget.org/packages/FoxRedbook)
[![NuGet FoxOrangebook](https://img.shields.io/nuget/v/FoxOrangebook.svg?label=FoxOrangebook)](https://www.nuget.org/packages/FoxOrangebook)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## SCSI Transport Layer

The shared foundation. All disc I/O flows through `IScsiTransport` — a platform-neutral interface over the OS-specific SCSI passthrough mechanism. Both FoxRedbook and FoxOrangebook build on it.

| Platform | Backend | Tested Hardware |
|----------|---------|-----------------|
| Windows | `DeviceIoControl` + `SCSI_PASS_THROUGH_DIRECT` | Pioneer BDR-XS07U |
| Linux | `ioctl(SG_IO)` | Pioneer BDR-XS07U |
| macOS | IOKit `MMCDeviceInterface` | Pioneer BDR-XS07U |

Device paths: `D:` or `\\.\CdRom0` on Windows, `/dev/sr0` on Linux, `disk1` on macOS.

- **AOT-compatible** — all P/Invoke uses `LibraryImport` source generation, no runtime marshalling
- **Zero external dependencies** at runtime

---

## <img src="assets/foxredbook.png" width="32" height="32" align="top"/> FoxRedbook — CD-DA Ripping

Bit-perfect audio CD ripping with AccurateRip verification. Named for the [Red Book](https://en.wikipedia.org/wiki/Compact_Disc_Digital_Audio) specification (IEC 60908).

[![NuGet](https://img.shields.io/nuget/v/FoxRedbook.svg)](https://www.nuget.org/packages/FoxRedbook)
[![NuGet Downloads](https://img.shields.io/nuget/dt/FoxRedbook.svg)](https://www.nuget.org/packages/FoxRedbook)

```
dotnet add package FoxRedbook
```

- **WiggleEngine** cross-read verification with automatic jitter correction, dropped/duplicated byte detection, and scratch repair via re-reads
- **AccurateRip v1/v2** checksum computation — verified against the community database
- **Embedded drive offset database** (4,800+ drives from AccurateRip) with automatic offset correction
- **Disc fingerprinting** — MusicBrainz, freedb/CDDB, and AccurateRip disc IDs from the TOC alone
- **CD-Text** parser (READ TOC format 5) with CRC-16 validation

### Quick Start — Ripping

```csharp
using FoxRedbook;

using var drive = OpticalDrive.Open("D:");

// One call gets TOC, disc IDs, and CD-Text
DiscInfo info = await drive.ReadDiscInfoAsync();

Console.WriteLine($"MusicBrainz ID: {info.MusicBrainzDiscId}");
Console.WriteLine($"Tracks: {info.Toc.TrackCount}");

if (info.CdText is { } cdText)
{
    Console.WriteLine($"Album: {cdText.AlbumTitle} by {cdText.AlbumPerformer}");
}

// Rip a track with automatic offset correction
using var session = RipSession.CreateAutoCorrected(drive);
var track = info.Toc.Tracks[0];

await foreach (var sector in session.RipTrackAsync(track))
{
    // sector.Pcm contains 2,352 bytes of verified 16-bit stereo 44.1kHz audio
}

// AccurateRip checksums available after the track is fully consumed
uint arV1 = session.GetAccurateRipV1Crc(track);
uint arV2 = session.GetAccurateRipV2Crc(track);
```

### Drive Offset Correction

AccurateRip checksums depend on applying the correct read offset for your drive model. FoxRedbook ships an embedded database of 4,800+ drive offsets. `RipSession.CreateAutoCorrected(drive)` handles the lookup and wrapping automatically.

For manual control:

```csharp
int? offset = KnownDriveOffsets.Lookup(drive.Inquiry);

if (offset is int samples)
{
    using var corrected = new OffsetCorrectingDrive(drive, -samples);
    using var session = new RipSession(corrected);
    // ...
}
```

---

## <img src="assets/foxorangebook.png" width="32" height="32" align="top"/> FoxOrangebook — CD-R/CD-RW Burning

Disc-At-Once audio CD burning. Named for the [Orange Book](https://en.wikipedia.org/wiki/Orange_Book_(CD_standard)) specification that defines CD-R and CD-RW.

[![NuGet](https://img.shields.io/nuget/v/FoxOrangebook.svg)](https://www.nuget.org/packages/FoxOrangebook)
[![NuGet Downloads](https://img.shields.io/nuget/dt/FoxOrangebook.svg)](https://www.nuget.org/packages/FoxOrangebook)

```
dotnet add package FoxOrangebook
```

- **Disc-At-Once** (DAO) burning — the whole disc in one pass, no inter-track gaps or run-out blocks
- **Cue sheet** pre-programming per MMC-6
- **Buffer underrun protection** (BURN-Free / SafeBurn)
- **CD-RW blanking** (full and minimal)
- **File-backed transport** — "burn" to a .bin/.cue file pair for testing or archival without touching hardware
- **Track metadata** — title and performer per track, written to cue sheet (CD-Text burn support planned)

### Quick Start — Burning

```csharp
using FoxOrangebook;
using FoxRedbook;

using var drive = OpticalDrive.Open("D:");
var transport = (IScsiTransport)drive;

var session = new BurnSession(transport);

var tracks = new List<AudioTrackSource>
{
    new() { Pcm = File.OpenRead("track01.raw"), Title = "Song One", Performer = "Artist" },
    new() { Pcm = File.OpenRead("track02.raw"), Title = "Song Two", Performer = "Artist" },
};

await session.BurnAsync(tracks);
```

### Burn to File

Test the full burn pipeline without a blank disc:

```csharp
using var transport = new FileBackedBurnTransport("output.bin")
{
    DiscTitle = "My Album",
    DiscPerformer = "Various Artists",
    TrackMetadata = tracks.Select(t => (t.Title, t.Performer)).ToList(),
};

var session = new BurnSession(transport);
await session.BurnAsync(tracks);
// Produces output.bin + output.cue — playable in foobar2000
```

---

## Building

```
dotnet build
dotnet test
```

The default test suite runs on any .NET 8 host with no hardware required. Hardware tests auto-detect optical drives and run when one is available with an audio CD inserted.

## License

MIT. See [LICENSE](LICENSE).

Test data files (`cdtext.cdt`, `cdtext.right`) are sourced from [libcdio](https://www.gnu.org/software/libcdio/) (GPL) and used unmodified for parser verification only. They are not included in the NuGet package. See [ATTRIBUTION.md](ATTRIBUTION.md).

Audio test assets in `assets/` are public domain recordings. See [assets/ATTRIBUTION.md](assets/ATTRIBUTION.md).
