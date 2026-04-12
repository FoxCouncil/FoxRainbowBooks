# FoxRedbook.Tests

Unit tests for the FoxRedbook library.

The default test suite is **pure-function CI-only** — every test runs identically on Windows, Linux, and macOS with no hardware, no network, and no platform-specific runtime calls. The suite covers: verification engine logic, AccurateRip v1/v2 checksums, disc ID fingerprinting, CDB byte layouts, SCSI response parsing, sense data mapping, struct-layout assertions against kernel ABIs, and CD-Text parsing against a libcdio reference fixture.

A separate **hardware category** talks to a real optical drive via the platform's SCSI passthrough backend. These tests are excluded from the default run and skip cleanly when no drive is available.

## Running the default suite

```
dotnet test
```

Pure-function tests pass on any supported .NET 8 host. Hardware tests
skip cleanly when no drive is available, so a default run on a drive-less
host reports the full suite as green with the hardware tiers in the
Skipped column.

## Hardware tests

A subset of tests runs against a real optical drive, tagged
`[Trait("Category", "Hardware")]`. To run only those:

```
# Uses the platform default: /dev/sr0 on Linux, D: on Windows, disk1 on macOS
dotnet test --filter "Category=Hardware"
```

Or override the device path via environment variable:

```
# Linux / macOS (bash)
FOXREDBOOK_TEST_DEVICE=/dev/sr1 dotnet test --filter "Category=Hardware"

# Windows (PowerShell)
$env:FOXREDBOOK_TEST_DEVICE="E:"; dotnet test --filter "Category=Hardware"
```

To force-exclude hardware tests from the default run (useful when a drive
is plugged in but you only want the pure-function suite):

```
dotnet test --filter "Category!=Hardware"
```

Insert an audio CD before running any tier that needs one. Tests that
require a drive or disc skip with a clear reason message rather than
failing. A hardware run on a drive-less host is a no-op — all tiers
skip cleanly.

### Tiers

Hardware tests are layered smallest-first so failures are easy to isolate:

- **Tier 1** — drive opens. Validates device-path normalization and handle acquisition.
- **Tier 2** — INQUIRY returns parseable vendor/product/revision strings. Validates the full SCSI passthrough stack with the smallest possible command.
- **Tier 3** — READ TOC returns a parseable audio TOC with valid track numbers and ascending LBAs.
- **Tier 4** — READ CD returns 2,352 bytes of plausible audio data from a single sector.
- **Tier 5** — Full track rip via `RipSession` with WAV file output. Writes to `%TEMP%/foxredbook_trackNN.wav`. Takes 30s–5min depending on track length and drive speed.
