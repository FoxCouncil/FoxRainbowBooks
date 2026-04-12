# Third-Party Attribution

FoxRedbook is an original MIT-licensed implementation with no third-party runtime code or data dependencies beyond the .NET Base Class Library. This file documents third-party sources used for **verification** (cross-checking algorithm output against independent references) and for **build-time test data**, neither of which ships in the published NuGet package.

## Test data

### `tests/FoxRedbook.Tests/TestData/cdtext.cdt` and `cdtext.right`

Raw CD-Text binary dump and the corresponding parsed text output, sourced from the libcdio project's test suite (`libcdio/test/data/cdtext.cdt` and `libcdio/test/cdtext.right`) at `github.com/libcdio/libcdio`. libcdio is GPL-licensed.

These files are used unmodified as reference test vectors: the raw binary is fed to our independent parser and the parser's output is compared against the expected strings in the `.right` file. The files exist only in the test project for build-time verification and are **not redistributed in the FoxRedbook NuGet package** — the NuGet package has no dependency on libcdio, no code was copied from libcdio, and the GPL does not propagate to the FoxRedbook library or to consumers of the NuGet package.

Using third-party reference vectors to verify an independent implementation is standard cross-implementation practice (the same pattern FoxRedbook uses to verify AccurateRip checksums against whipper's reference data and MusicBrainz disc IDs against python-discid's fixtures). No part of libcdio's source code is incorporated into FoxRedbook.

## Algorithm references (no code reuse)

The following projects were read during algorithm research but no code was copied:

- **libcdio / libcdio-paranoia** — reference for the verification engine's Stage 1/Stage 2 algorithms, sort-indexed sample matching, and CD-Text pack layout. FoxRedbook's `WiggleEngine` is an independent re-implementation of the same published algorithms.
- **whipper** — reference for AccurateRip v1/v2 checksum computation, freedb disc ID, and AccurateRip disc IDs ID1/ID2. FoxRedbook's `AccurateRipChecksum`, `FreedbDiscId`, and `AccurateRipDiscIds` were verified against whipper's test fixtures.
- **python-discid / libdiscid** — reference for MusicBrainz disc ID SHA-1 computation and the freedb disc ID low-byte quirk on non-standard first-track discs.
- **Apple xnu** — authoritative source for Linux SG_IO struct layout comparison and macOS IOReturn constants.
- **Microsoft Windows-driver-samples (spti)** — reference for the `SCSI_PASS_THROUGH_DIRECT_WITH_BUFFER` layout convention.

All specifications (MMC-6, SPC-4, Red Book IEC 60908, Sony CD-Text) were consulted as implementation references. FoxRedbook is a clean-room implementation written from spec.
