# FoxRedbook

Cross-platform, AOT-compatible .NET 8 library for bit-perfect audio CD ripping with AccurateRip verification.

Named for the [Red Book](https://en.wikipedia.org/wiki/Compact_Disc_Digital_Audio) specification (IEC 60908) that defines CD-DA. Scope is deliberately tight: raw sectors in, verified PCM samples out. No encoding, no metadata lookup, no file management.

[![NuGet](https://img.shields.io/nuget/v/FoxRedbook.svg)](https://www.nuget.org/packages/FoxRedbook)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Features

- **SCSI passthrough** on Windows (`IOCTL_SCSI_PASS_THROUGH_DIRECT`), Linux (`SG_IO`), and macOS (IOKit `SCSITaskDeviceInterface`)
- **WiggleEngine** cross-read verification with automatic jitter correction, dropped/duplicated byte detection, and scratch repair via re-reads
- **AccurateRip v1/v2** checksum computation — verified against the community database
- **Embedded drive offset database** (4,800+ drives from AccurateRip) with automatic offset correction
- **Disc fingerprinting** — MusicBrainz, freedb/CDDB, and AccurateRip disc IDs from the TOC alone
- **CD-Text** parser (READ TOC format 5) with CRC-16 validation
- **AOT-compatible** — all P/Invoke uses `LibraryImport` source generation, no runtime marshalling
- **Zero external dependencies** at runtime

## Quick Start

```csharp
using FoxRedbook;

// Open the first available optical drive
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
    // Write to file, encode, stream — your choice
}

// AccurateRip checksums are available after the track is fully consumed
uint arV1 = session.GetAccurateRipV1Crc(track);
uint arV2 = session.GetAccurateRipV2Crc(track);
```

## Platform Notes

| Platform | Backend | Tested Hardware |
|----------|---------|-----------------|
| Windows | `DeviceIoControl` + `SCSI_PASS_THROUGH_DIRECT` | Pioneer BDR-XS07U |
| Linux | `ioctl(SG_IO)` | — |
| macOS | IOKit `MMCDeviceInterface` | Pioneer BDR-XS07U |

Device paths: `D:` or `\\.\CdRom0` on Windows, `/dev/sr0` on Linux, `disk1` on macOS.

## Drive Offset Correction

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

## Building

```
dotnet build
dotnet test
```

The default test suite (266 pure-function tests) runs on any .NET 8 host with no hardware required. Hardware tests auto-detect optical drives and run when one is available with an audio CD inserted.

## License

MIT. See [LICENSE](LICENSE).

Test data files (`cdtext.cdt`, `cdtext.right`) are sourced from [libcdio](https://www.gnu.org/software/libcdio/) (GPL) and used unmodified for parser verification only. They are not included in the NuGet package. See [ATTRIBUTION.md](ATTRIBUTION.md).
