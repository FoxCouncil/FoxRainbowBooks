using System.Buffers;
using System.Runtime.InteropServices;

namespace FoxRedbook;

/// <summary>
/// Core verification engine. Manages the cache block list, fragment list,
/// root block, and the two-stage verification pipeline. One instance per session.
/// </summary>
internal sealed class WiggleEngine : IDisposable
{
    private readonly IOpticalDrive _drive;
    private readonly RipOptions _options;
    private readonly List<CacheBlock> _cache = new();
    private readonly List<VerifiedFragment> _fragments = new();
    private readonly RootBlock _root;
    private readonly JitterStatistics _stage1Stats = new();
    private readonly JitterStatistics _stage2Stats = new();

    private int _dynamicOverlap = WiggleConstants.MinSectorEpsilon;
    private long _dynamicDrift;
    private int _jiggleCounter;

    private byte[]? _readBuffer;
    private bool _disposed;

    internal WiggleEngine(IOpticalDrive drive, RipOptions options)
    {
        ArgumentNullException.ThrowIfNull(drive);
        _drive = drive;
        _options = options ?? new RipOptions();

        // Initial root capacity: 64 sectors worth of samples
        _root = new RootBlock(64 * WiggleConstants.WordsPerSector);
    }

    /// <summary>
    /// Reads and verifies one sector, writing <see cref="CdConstants.SectorSize"/>
    /// bytes of verified PCM into <paramref name="outputBuffer"/>.
    /// </summary>
    internal void ReadVerifiedSector(
        Span<byte> outputBuffer,
        long sectorLba,
        out SectorStatus status,
        out int reReadCount,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (outputBuffer.Length < CdConstants.SectorSize)
        {
            throw new ArgumentException(
                $"Buffer must be at least {CdConstants.SectorSize} bytes.",
                nameof(outputBuffer));
        }

        long beginWord = sectorLba * WiggleConstants.WordsPerSector;
        long endWord = beginWord + WiggleConstants.WordsPerSector;

        status = SectorStatus.None;
        reReadCount = 0;

        int retryCount = 0;
        long lastEnd = _root.IsEmpty ? -1 : _root.End;

        while (!_root.Covers(beginWord, endWord))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Trim old data behind what we've already returned
            TrimCache();

            // Try merging existing fragments into root (Stage 2)
            if (_fragments.Count > 0)
            {
                MergeFragments(beginWord, endWord, ref status);
            }

            if (_root.Covers(beginWord, endWord))
            {
                break;
            }

            // Issue a new drive read and run Stage 1 cross-verification
            ReadNewBlock(sectorLba, ref status, cancellationToken);
            reReadCount++;

            // Check if root grew
            long currentEnd = _root.IsEmpty ? -1 : _root.End;

            if (currentEnd > lastEnd + WiggleConstants.WordsPerSector / 2)
            {
                lastEnd = currentEnd;
                retryCount = 0;
            }
            else
            {
                retryCount++;

                if (retryCount % WiggleConstants.RetryBackoffInterval == 0)
                {
                    if (retryCount >= _options.MaxReReads)
                    {
                        // Skip: accept whatever we have and fill the gap
                        status |= SectorStatus.Skipped;
                        ForceAcceptSector(beginWord, endWord);
                        break;
                    }
                    else
                    {
                        // Widen the search window
                        _dynamicOverlap = Math.Min(
                            _dynamicOverlap * 3 / 2,
                            WiggleConstants.MaxSectorOverlap * WiggleConstants.WordsPerSector);
                        status |= SectorStatus.JitterCorrected;
                    }
                }
            }
        }

        // Copy the verified sector from root to output
        CopyFromRoot(beginWord, outputBuffer);

        // Advance the returned-data cursor
        if (_root.ReturnedLimit < endWord)
        {
            _root.ReturnedLimit = endWord;
        }
    }

    /// <summary>
    /// Repositions the engine's read cursor. Discards cached state.
    /// </summary>
    internal void Seek(long samplePosition)
    {
        DisposeCache();
        DisposeFragments();

        // If the seek target is outside the root's current extent, discard
        // the root entirely. Continuing with data from a distant prior read
        // would cause the force-accept path to stitch new data onto the old
        // root with misaligned absolute positions — the prepend operation
        // reduces _root.Begin by the count of prepended samples, but the
        // old root data still physically sits in the buffer beyond the
        // prepended region, and there is no mechanism to represent a gap
        // between them. The result is silently wrong absolute positions
        // for any subsequent CopyFromRoot call. Safer to start fresh.
        if (!_root.IsEmpty && (samplePosition < _root.Begin || samplePosition >= _root.End))
        {
            _root.Clear();
        }
        else
        {
            _root.TrimBefore(samplePosition);
        }

        _root.ReturnedLimit = samplePosition;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            DisposeCache();
            DisposeFragments();
            _root.Dispose();

            if (_readBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(_readBuffer);
                _readBuffer = null;
            }

            _disposed = true;
        }
    }

    // ── Drive reads ─────────────────────────────────────────────

    /// <summary>
    /// Reads a chunk of sectors from the drive, creates a CacheBlock, and
    /// runs Stage 1 cross-verification against all existing cached blocks.
    /// </summary>
    private void ReadNewBlock(long targetLba, ref SectorStatus status, CancellationToken ct)
    {
        // Calculate how many sectors to read: target + generous overlap on both sides.
        // Minimum 3 sectors of overlap ensures the OverlapAdj trim at fragment
        // boundaries doesn't eat into the target sector.
        int overlapSectors = Math.Max(3, _dynamicOverlap / WiggleConstants.WordsPerSector + 1);
        int readSectors = 1 + 2 * overlapSectors;

        // Simple 2-position jiggle: alternate the read start by 0 or 1 sector.
        // This prevents edge regions from aligning between consecutive reads,
        // which would create permanent blind spots at the block boundaries.
        // TODO: full jiggle mechanism cycling through more positions to defeat
        // drive read-ahead caching.
        int jiggle = _jiggleCounter % 2;
        _jiggleCounter++;

        long readLba = targetLba - overlapSectors + jiggle;

        if (readLba < 0)
        {
            readSectors += (int)readLba;
            readLba = 0;
        }

        if (readSectors <= 0)
        {
            readSectors = 1;
            readLba = targetLba;
        }

        // Ensure read buffer is large enough
        int requiredBytes = CdConstants.GetReadBufferSize(ReadOptions.None, readSectors);

        if (_readBuffer is null || _readBuffer.Length < requiredBytes)
        {
            if (_readBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(_readBuffer);
            }

            _readBuffer = ArrayPool<byte>.Shared.Rent(requiredBytes);
        }

        // Issue the drive read (sync-over-async: the interface is async but
        // the engine loop is sequential — each read must complete before we
        // can verify its contents)
        int sectorsRead;

        try
        {
            sectorsRead = _drive.ReadSectorsAsync(readLba, readSectors, _readBuffer, ReadOptions.None, ct)
                .GetAwaiter().GetResult();
        }
        catch (OpticalDriveException)
        {
            status |= SectorStatus.ReadError;
            return;
        }

        if (sectorsRead == 0)
        {
            return;
        }

        status |= SectorStatus.Read;

        // Convert byte buffer to a CacheBlock of 16-bit samples
        int totalSamples = sectorsRead * WiggleConstants.WordsPerSector;
        var block = new CacheBlock(totalSamples);
        block.Begin = readLba * WiggleConstants.WordsPerSector - _dynamicDrift;

        // Copy bytes into the block's sample buffer (byte[] → short[])
        var byteSpan = _readBuffer.AsSpan(0, sectorsRead * CdConstants.SectorSize);
        var sampleView = MemoryMarshal.Cast<byte, short>(byteSpan);
        sampleView.CopyTo(block.Samples);

        // Mark edge flags at block boundaries where this read meets adjacent
        // reads. The drive's laser positioning is least reliable at the start
        // of a seek and may drift at the end of a long read.
        //
        // ──────────────────────────────────────────────────────────────
        // LOAD-BEARING: DO NOT "CLEAN UP" WITHOUT READING THIS WHOLE COMMENT.
        // ──────────────────────────────────────────────────────────────
        //
        // The `if (block.Begin > 0)` guard below suppresses the leading edge
        // flag for blocks that begin at absolute sample position 0 (i.e. the
        // very first read of track 1 on a disc). This looks like a special
        // case that should be removed. It is not. Here is why it exists, why
        // the obvious alternative does not work, and what guarantees it is
        // built on.
        //
        // THE PROBLEM
        // -----------
        // EdgeHalfWidth (32 samples) marks the first 32 samples of every block
        // as physically unreliable due to seek jitter. Stage 1 refuses to
        // extend a match across any position where BOTH blocks have the Edge
        // flag set. OverlapAdj (31 samples) then trims each verified run by
        // 31 samples on its leading side during fragment extraction. For a
        // block that starts at absolute position 0, these two effects stack:
        // the first 32 samples cannot join a verified run, and then another
        // 31 samples get trimmed off the front of the run that does form.
        // The result is a permanent 63-sample dead zone at absolute [0, 63)
        // that the verification pipeline can never cover, no matter how many
        // re-reads the engine performs. The engine exhausts its retry budget
        // and falls through to ForceAcceptSector, which back-fills the gap
        // from raw cache data WITHOUT cross-verification and sets the
        // Skipped status flag on sector 0. That means track 1's first ~1.4 ms
        // of audio is force-accepted on every clean rip of every disc.
        //
        // THE REJECTED ALTERNATIVE
        // ------------------------
        // The obvious fix is to inject synthetic digital silence at virtual
        // negative sample positions as if the pregap were readable, and let
        // the normal verification pipeline match against it. A real CD's
        // pregap IS digital silence, so the data is "correct by definition"
        // and two independent reads would trivially match in the silence
        // region. We tried this. It does not solve the problem. The edge
        // flags represent the drive's physical positioning unreliability at
        // the transition from "seeking" to "reading real audio data" — that
        // boundary lives at absolute position 0 no matter what we put before
        // it. If we mark edges at the silence-prefix start, the prefix is
        // synthetic and always reliable, so marking it is a lie. If we mark
        // edges at the real drive-output boundary (absolute 0), we have the
        // original dead zone back: backward match extension from real data
        // still stops at the edge, forward extension from the silence prefix
        // also stops there, and the silence fragment is too short after
        // OverlapAdj trimming to survive extraction. The diagnostic test
        // Sector0_CleanDisc_IsVerifiedNotSkipped catches this by checking
        // for the Skipped status flag. Our silence-prefix attempt passed
        // data-content tests only because ForceAcceptSector was back-filling
        // from cache behind our backs.
        //
        // THE JUSTIFICATION
        // -----------------
        // Suppressing the leading edge flag for the LBA 0 block is the
        // correct minimal fix. The argument is physical: edge flags model
        // positioning unreliability at seek boundaries, which is where one
        // read request's data transitions to the next read request's data.
        // For a block that begins at absolute 0, there is no prior read
        // request. The drive did seek to find LBA 0, but the first-seek
        // unreliability manifests CONSISTENTLY across retries — the drive's
        // first-sample error, if any, is deterministic per disc per drive
        // session. Cross-verification catches deterministic errors the same
        // way it catches any other: two independent reads producing the
        // same wrong data is indistinguishable from two independent reads
        // producing the same right data, and that's the fundamental
        // limitation the engine accepts disc-wide. Suppressing the edge
        // flag here doesn't weaken verification relative to the rest of
        // the disc — it just removes an artificial dead zone that exists
        // only because our edge-flag model treats absolute 0 like any
        // other read boundary when it isn't.
        //
        // THE WEAKENED INVARIANT
        // ----------------------
        // The OverlapAdj gap-detection property — that fragments from
        // independent verification runs either overlap by ≥2 samples or
        // have a detectable gap of up to 62 samples between them — is also
        // relaxed for absolute positions < 63 (see the matching exception
        // in ExtractFragments). This is safe because there is no "previous
        // verification run" that could live at a negative absolute position
        // and confuse itself with a fragment at position 0. The gap
        // detector exists to distinguish "these two fragments came from
        // the same match, trust the adjacency" from "these two fragments
        // came from different matches, require a new verification to
        // bridge them." At the disc boundary there is nothing on the
        // other side of the bridge.
        //
        // THE GUARDRAIL
        // -------------
        // Sector 0 must come from genuine cross-verification, not
        // ForceAcceptSector backfill. The Sector0_CleanDisc_IsVerifiedNotSkipped
        // test asserts the SectorStatus for sector 0 does not contain
        // Skipped on a clean disc. If a future refactor reintroduces the
        // disc-start dead zone — by "cleaning up" this comment's
        // suppression, reverting the leadTrim relaxation in ExtractFragments,
        // or otherwise forcing the first 32 samples back into the edge
        // region — that test will fail and tell us exactly why. Do not
        // disable the test to make the refactor pass.
        {
            var flags = block.Flags;
            int edgeWidth = WiggleConstants.EdgeHalfWidth;

            if (block.Begin > 0)
            {
                int startEdge = Math.Min(edgeWidth, totalSamples);

                for (int j = 0; j < startEdge; j++)
                {
                    flags[j] |= SampleFlags.Edge;
                }
            }

            int endEdge = Math.Max(0, totalSamples - edgeWidth);

            for (int j = endEdge; j < totalSamples; j++)
            {
                flags[j] |= SampleFlags.Edge;
            }
        }

        // Handle short reads: mark missing samples as unread
        if (sectorsRead < readSectors)
        {
            // Samples beyond what was read are already zero from CacheBlock constructor
            // but we need to flag them
            var flags = block.Flags;

            for (int j = sectorsRead * WiggleConstants.WordsPerSector; j < totalSamples; j++)
            {
                flags[j] |= SampleFlags.Unread;
            }
        }

        // Run Stage 1: cross-verify this block against all cached blocks
        RunStage1(block, ref status);

        _cache.Add(block);

        // Try merging any new fragments
        if (_fragments.Count > 0)
        {
            long targetBegin = targetLba * WiggleConstants.WordsPerSector;
            long targetEnd = targetBegin + WiggleConstants.WordsPerSector;
            MergeFragments(targetBegin, targetEnd, ref status);
        }

        // Adjust dynamic overlap based on accumulated jitter measurements
        AdjustDynamicOverlap();
    }

    // ── Stage 1: Cross-verification ─────────────────────────────

    /// <summary>
    /// Compares a newly-read block against all cached blocks to find
    /// identical sample runs that prove correctness. Verified runs are
    /// extracted as fragments.
    /// </summary>
    private void RunStage1(CacheBlock newBlock, ref SectorStatus status)
    {
        // Compare against each existing cached block (newest first)
        for (int ci = _cache.Count - 1; ci >= 0; ci--)
        {
            var cached = _cache[ci];
            CompareBlocks(cached, newBlock, ref status);
        }

        // Extract contiguous verified regions as fragments
        ExtractFragments(newBlock);
    }

    /// <summary>
    /// Compares two cache blocks, finding identical sample runs and marking
    /// them as verified. Uses the sort-indexed search with the 23-sample stride.
    /// </summary>
    private void CompareBlocks(CacheBlock blockA, CacheBlock blockB, ref SectorStatus status)
    {
        // Build a sort index over block A for O(1) value lookups
        using var sortA = new SampleSortIndex(blockA.SamplesArray, 0, blockA.Size);

        var samplesB = blockB.Samples;
        var flagsB = blockB.Flags;

        // Determine the overlapping sample range
        long overlapBegin = Math.Max(blockA.Begin, blockB.Begin);
        long overlapEnd = Math.Min(blockA.End, blockB.End);

        if (overlapEnd - overlapBegin < WiggleConstants.MinWordsSearch)
        {
            return;
        }

        // Scan every 23rd sample in block B within the overlap range
        int bStart = (int)Math.Max(0, overlapBegin - blockB.Begin);
        int bEnd = (int)Math.Min(blockB.Size, overlapEnd - blockB.Begin);

        for (int posB = bStart; posB < bEnd; posB += WiggleConstants.SampleStride)
        {
            // Skip already-verified or unread samples
            if ((flagsB[posB] & (SampleFlags.Verified | SampleFlags.Unread)) != SampleFlags.None)
            {
                continue;
            }

            short value = samplesB[posB];

            // Fast path: try zero-jitter match first (same absolute position)
            long absPos = blockB.Begin + posB;
            int posA = (int)(absPos - blockA.Begin);

            if (posA >= 0 && posA < blockA.Size && blockA.Samples[posA] == value)
            {
                int matchLen = ExtendMatch(blockA, blockB, posA, posB, out int matchBeginA, out int matchBeginB);

                if (matchLen >= WiggleConstants.MinWordsSearch)
                {
                    int offset = (matchBeginA + (int)blockA.Begin) - (matchBeginB + (int)blockB.Begin);
                    MarkVerified(blockB, matchBeginB, matchLen, ref status);
                    _stage1Stats.AddMeasurement(offset);
                    continue;
                }
            }

            // Slow path: search the sort index within the dynamic overlap window
            int searchPosA = (int)(absPos - blockA.Begin);
            int idx = sortA.FindMatch(searchPosA, _dynamicOverlap, value);

            while (idx != SampleSortIndex.NoMatch)
            {
                int matchLen = ExtendMatch(blockA, blockB, idx, posB, out int matchBeginA2, out int matchBeginB2);

                if (matchLen >= WiggleConstants.MinWordsSearch)
                {
                    int offset = (matchBeginA2 + (int)blockA.Begin) - (matchBeginB2 + (int)blockB.Begin);
                    MarkVerified(blockB, matchBeginB2, matchLen, ref status);
                    _stage1Stats.AddMeasurement(offset);
                    break;
                }

                idx = sortA.FindNextMatch(idx);
            }
        }
    }

    /// <summary>
    /// Extends a match from a seed position in both directions. Stops at edge
    /// boundaries (where both blocks have Edge flags set simultaneously — matching
    /// across that point in both blocks would bridge two unreliable regions).
    /// </summary>
    private static int ExtendMatch(
        CacheBlock blockA, CacheBlock blockB,
        int seedA, int seedB,
        out int beginA, out int beginB)
    {
        var samplesA = blockA.Samples;
        var samplesB = blockB.Samples;
        var flagsA = blockA.Flags;
        var flagsB = blockB.Flags;

        int fwdA = seedA;
        int fwdB = seedB;

        // Extend forward
        while (fwdA < blockA.Size - 1 && fwdB < blockB.Size - 1)
        {
            // Stop if both blocks have edge flags at this position
            if ((flagsA[fwdA + 1] & SampleFlags.Edge) != 0 &&
                (flagsB[fwdB + 1] & SampleFlags.Edge) != 0)
            {
                break;
            }

            if (samplesA[fwdA + 1] != samplesB[fwdB + 1])
            {
                break;
            }

            fwdA++;
            fwdB++;
        }

        int bwdA = seedA;
        int bwdB = seedB;

        // Extend backward
        while (bwdA > 0 && bwdB > 0)
        {
            if ((flagsA[bwdA - 1] & SampleFlags.Edge) != 0 &&
                (flagsB[bwdB - 1] & SampleFlags.Edge) != 0)
            {
                break;
            }

            if (samplesA[bwdA - 1] != samplesB[bwdB - 1])
            {
                break;
            }

            bwdA--;
            bwdB--;
        }

        beginA = bwdA;
        beginB = bwdB;
        return fwdA - bwdA + 1;
    }

    /// <summary>
    /// Marks a range of samples in a block as verified.
    /// </summary>
    private static void MarkVerified(CacheBlock block, int start, int length, ref SectorStatus status)
    {
        var flags = block.Flags;

        for (int i = start; i < start + length; i++)
        {
            flags[i] |= SampleFlags.Verified;
        }

        status |= SectorStatus.Verified;
    }

    /// <summary>
    /// Scans a block for contiguous runs of verified samples and extracts them
    /// as fragments, trimming each end by OverlapAdj to ensure detectable gaps
    /// between independently-verified regions.
    /// </summary>
    private void ExtractFragments(CacheBlock block)
    {
        var flags = block.Flags;
        var samples = block.Samples;
        int i = 0;

        while (i < block.Size)
        {
            // Find start of a verified run
            while (i < block.Size && (flags[i] & SampleFlags.Verified) == 0)
            {
                i++;
            }

            if (i >= block.Size)
            {
                break;
            }

            int runStart = i;

            // Find end of the verified run
            while (i < block.Size && (flags[i] & SampleFlags.Verified) != 0)
            {
                i++;
            }

            int runEnd = i;
            int runLength = runEnd - runStart;

            // Apply overlap adjustment trim on each end. The trim guarantees
            // that fragments from independent verification runs have either
            // a detectable overlap (from the same run) or a detectable gap
            // (from different runs), which the merge logic uses to distinguish
            // contiguous runs from rift-analysis candidates.
            //
            // Exception: don't trim the leading edge near the start of the
            // audio stream. There's no prior data that could create a false
            // gap, so the trim serves no purpose and would leave the first
            // samples permanently unverifiable. The threshold accounts for
            // edge half-width because LBA 0 blocks with edge suppression
            // still have a natural start at position 0 and need their
            // leading samples preserved. This means the gap-detection
            // property is slightly weaker in the first 63 samples of the
            // disc, but those samples have no "previous run" to confuse
            // them with anyway.
            long absRunStart = block.Begin + runStart;
            int discStartThreshold = WiggleConstants.OverlapAdj + WiggleConstants.EdgeHalfWidth;
            int leadTrim = absRunStart < discStartThreshold ? 0 : WiggleConstants.OverlapAdj;

            int trimmedStart = runStart + leadTrim;
            int trimmedEnd = runEnd - WiggleConstants.OverlapAdj;
            int trimmedLength = trimmedEnd - trimmedStart;

            if (trimmedLength < WiggleConstants.MinWordsRift)
            {
                continue;
            }

            var fragment = new VerifiedFragment(trimmedLength);
            fragment.Begin = block.Begin + trimmedStart;
            samples.Slice(trimmedStart, trimmedLength).CopyTo(fragment.Samples);

            _fragments.Add(fragment);
        }
    }

    // ── Stage 2: Fragment merging ───────────────────────────────

    /// <summary>
    /// Attempts to merge all pending fragments into the root block.
    /// Loops until no more fragments can be merged.
    /// </summary>
    private void MergeFragments(long beginWord, long endWord, ref SectorStatus status)
    {
        bool merged = true;

        while (merged)
        {
            merged = false;

            // Sort fragments by position for deterministic merge order
            _fragments.Sort((a, b) => a.Begin.CompareTo(b.Begin));

            for (int i = _fragments.Count - 1; i >= 0; i--)
            {
                var frag = _fragments[i];

                if (_root.IsEmpty)
                {
                    // Initialize root from the first fragment
                    _root.InitializeFrom(frag);
                    frag.Dispose();
                    _fragments.RemoveAt(i);
                    merged = true;
                }
                else if (TryMergeFragment(frag, ref status))
                {
                    frag.Dispose();
                    _fragments.RemoveAt(i);
                    merged = true;
                }
            }
        }
    }

    /// <summary>
    /// Tries to merge a single fragment into the root by finding where it
    /// overlaps, then applying rift analysis to fix any boundary disagreements.
    /// </summary>
    private bool TryMergeFragment(VerifiedFragment fragment, ref SectorStatus status)
    {
        // Find where the fragment overlaps the root
        if (!FindOverlap(fragment, out int rootOffset, out int fragOffset, out int matchLength))
        {
            // No verified overlap found. Check if the fragment is adjacent to
            // the root — within the gap that OverlapAdj trimming can create.
            // Fragments from independent verification runs can end up separated
            // by up to 2 × OverlapAdj samples with no actual data gap on disc.
            int maxGap = WiggleConstants.OverlapAdj * 2;

            if (!_root.IsEmpty && fragment.Begin >= _root.End && fragment.Begin <= _root.End + maxGap)
            {
                // Fragment is just past root's end — direct append
                int gapSize = (int)(fragment.Begin - _root.End);

                if (gapSize > 0)
                {
                    // Fill the small gap with zeros (these samples couldn't be
                    // independently verified but the gap is tiny — ~62 samples
                    // = ~1.4 ms of audio at the boundary between two verified runs)
                    Span<short> zeros = stackalloc short[gapSize];
                    zeros.Clear();
                    _root.Append(zeros);
                }

                _root.Append(fragment.Samples);
                return true;
            }

            if (!_root.IsEmpty && fragment.End <= _root.Begin && fragment.End >= _root.Begin - maxGap)
            {
                // Fragment is just before root's start — direct prepend
                int gapSize = (int)(_root.Begin - fragment.End);
                int totalInsert = fragment.Size + gapSize;
                short[] temp = new short[totalInsert];
                fragment.Samples.CopyTo(temp);

                // Zero-fill the gap between fragment end and root start
                _root.Insert(0, temp.AsSpan(0, totalInsert));
                _root.Begin -= totalInsert;
                return true;
            }

            return false;
        }

        // The fragment's absolute begin + fragOffset should map to root's begin + rootOffset
        int jitterOffset = (int)((_root.Begin + rootOffset) - (fragment.Begin + fragOffset));

        _stage2Stats.AddMeasurement(jitterOffset);

        // Determine the region of the fragment to merge that extends the root
        long fragAbsBegin = fragment.Begin;
        long fragAbsEnd = fragment.End;

        // Analyze and fix rifts at the leading edge (backward from match start)
        int leadingBeginRoot = rootOffset;
        int leadingBeginFrag = fragOffset;
        AnalyzeLeadingRift(fragment, ref leadingBeginRoot, ref leadingBeginFrag, ref status);

        // Analyze and fix rifts at the trailing edge (forward from match end)
        int trailingEndRoot = rootOffset + matchLength;
        int trailingEndFrag = fragOffset + matchLength;
        AnalyzeTrailingRift(fragment, ref trailingEndRoot, ref trailingEndFrag, ref status);

        // Merge the fragment into the root. Snapshot the root's dimensions
        // BEFORE any mutations so each case tests against the same baseline.
        // Without this, an Append that grows rootEnd can cause the Fill check
        // to fire spuriously, overwriting rift-corrected data.
        long rootBeginBefore = _root.Begin;
        long rootEndBefore = _root.End;
        long fragBegin = fragment.Begin;
        long fragEnd = fragment.End;

        // Prepend: fragment overlaps root's start and extends before it
        if (fragBegin < rootBeginBefore && fragEnd > rootBeginBefore)
        {
            int prependCount = (int)Math.Min(rootBeginBefore - fragBegin, fragment.Size);

            if (prependCount > 0)
            {
                _root.Insert(0, fragment.Samples.Slice(0, prependCount));
                _root.Begin -= prependCount;
            }
        }

        // Append: fragment overlaps root's end and extends past it
        if (fragEnd > rootEndBefore && fragBegin < rootEndBefore)
        {
            int skipCount = (int)(rootEndBefore - fragBegin);
            skipCount = Math.Min(skipCount, fragment.Size);
            int appendCount = fragment.Size - skipCount;

            if (appendCount > 0)
            {
                _root.Append(fragment.Samples.Slice(skipCount, appendCount));
            }
        }

        // Fill: fragment is entirely within the root's ORIGINAL extent.
        // This handles the case where a gap (e.g., from a corrupted sample
        // that prevented verification) is later filled by a new fragment
        // from a clean re-read.
        if (fragBegin >= rootBeginBefore && fragEnd <= rootEndBefore)
        {
            int destOffset = (int)(fragBegin - rootBeginBefore);
            fragment.Samples.CopyTo(_root.Samples.Slice(destOffset));
        }

        return true;
    }

    /// <summary>
    /// Finds where a fragment overlaps with the root using sample comparison.
    /// </summary>
    private bool FindOverlap(
        VerifiedFragment fragment,
        out int rootOffset, out int fragOffset, out int matchLength)
    {
        rootOffset = 0;
        fragOffset = 0;
        matchLength = 0;

        if (_root.IsEmpty || fragment.Size < WiggleConstants.MinWordsSearch)
        {
            return false;
        }

        var rootSamples = _root.Samples;
        var fragSamples = fragment.Samples;

        // Try zero-offset first (most common case — fragment aligns with root)
        long expectedRootPos = fragment.Begin - _root.Begin;

        if (expectedRootPos >= 0 && expectedRootPos + fragment.Size <= _root.Size)
        {
            int rPos = (int)expectedRootPos;
            int len = CountMatchingFromSeed(rootSamples, fragSamples, rPos, 0);

            if (len >= WiggleConstants.MinWordsSearch)
            {
                rootOffset = rPos;
                fragOffset = 0;
                matchLength = len;
                return true;
            }
        }

        // Search: try matching the middle of the fragment against the root
        int fragMid = fragment.Size / 2;
        short searchValue = fragSamples[fragMid];

        // Linear scan over root for matching values near expected position
        int searchCenter = (int)Math.Clamp(expectedRootPos + fragMid, 0, _root.Size - 1);
        int searchWindow = _dynamicOverlap;

        int lo = Math.Max(0, searchCenter - searchWindow);
        int hi = Math.Min(_root.Size - 1, searchCenter + searchWindow);

        for (int r = lo; r <= hi; r++)
        {
            if (rootSamples[r] != searchValue)
            {
                continue;
            }

            // Extend match from this seed
            int matchFwd = 1;

            while (r + matchFwd < _root.Size && fragMid + matchFwd < fragment.Size &&
                   rootSamples[r + matchFwd] == fragSamples[fragMid + matchFwd])
            {
                matchFwd++;
            }

            int matchBwd = 0;

            while (r - matchBwd - 1 >= 0 && fragMid - matchBwd - 1 >= 0 &&
                   rootSamples[r - matchBwd - 1] == fragSamples[fragMid - matchBwd - 1])
            {
                matchBwd++;
            }

            int totalMatch = matchFwd + matchBwd;

            if (totalMatch >= WiggleConstants.MinWordsSearch && totalMatch > matchLength)
            {
                rootOffset = r - matchBwd;
                fragOffset = fragMid - matchBwd;
                matchLength = totalMatch;
            }
        }

        return matchLength >= WiggleConstants.MinWordsSearch;
    }

    /// <summary>
    /// Counts how many samples match starting from the given seed positions,
    /// extending forward.
    /// </summary>
    private static int CountMatchingFromSeed(
        ReadOnlySpan<short> a, ReadOnlySpan<short> b,
        int startA, int startB)
    {
        int count = 0;
        int ia = startA;
        int ib = startB;

        while (ia < a.Length && ib < b.Length && a[ia] == b[ib])
        {
            count++;
            ia++;
            ib++;
        }

        return count;
    }

    // ── Rift analysis ───────────────────────────────────────────

    /// <summary>
    /// Analyzes the leading (backward) edge of a match between fragment and root.
    /// If samples disagree before the match point, determines whether the disagreement
    /// is caused by dropped samples (insert) or duplicated samples (remove).
    /// </summary>
    private void AnalyzeLeadingRift(
        VerifiedFragment fragment,
        ref int rootPos, ref int fragPos,
        ref SectorStatus status)
    {
        var rootSamples = _root.Samples;
        var fragSamples = fragment.Samples;

        // Walk backward from the match boundary
        while (rootPos > 0 && fragPos > 0)
        {
            if (rootSamples[rootPos - 1] == fragSamples[fragPos - 1])
            {
                rootPos--;
                fragPos--;
                continue;
            }

            // Mismatch — try to classify the rift

            // Pattern 1: root has extra samples (stutter in root → remove from root)
            int rootSkip = FindResyncBackward(rootSamples, fragSamples, rootPos, fragPos, searchRoot: true);

            if (rootSkip > 0 && rootSkip <= WiggleConstants.MinWordsOverlap)
            {
                _root.Remove(rootPos - rootSkip, rootSkip);
                status |= SectorStatus.DuplicatedBytesFixed;
                continue;
            }

            // Pattern 2: fragment has extra samples (root is missing data → insert)
            int fragSkip = FindResyncBackward(rootSamples, fragSamples, rootPos, fragPos, searchRoot: false);

            if (fragSkip > 0 && fragSkip <= WiggleConstants.MinWordsOverlap)
            {
                _root.Insert(rootPos, fragSamples.Slice(fragPos - fragSkip, fragSkip));
                rootPos += fragSkip;
                status |= SectorStatus.DroppedBytesFixed;
                continue;
            }

            // Can't classify — stop
            break;
        }
    }

    /// <summary>
    /// Analyzes the trailing (forward) edge of a match between fragment and root.
    /// Same logic as leading rift but scanning forward.
    /// </summary>
    private void AnalyzeTrailingRift(
        VerifiedFragment fragment,
        ref int rootPos, ref int fragPos,
        ref SectorStatus status)
    {
        var rootSamples = _root.Samples;
        var fragSamples = fragment.Samples;

        while (rootPos < _root.Size && fragPos < fragment.Size)
        {
            if (rootSamples[rootPos] == fragSamples[fragPos])
            {
                rootPos++;
                fragPos++;
                continue;
            }

            // Pattern 1: root has extra samples (stutter → remove)
            int rootSkip = FindResyncForward(rootSamples, fragSamples, rootPos, fragPos, searchRoot: true);

            if (rootSkip > 0 && rootSkip <= WiggleConstants.MinWordsOverlap)
            {
                _root.Remove(rootPos, rootSkip);
                status |= SectorStatus.DuplicatedBytesFixed;
                continue;
            }

            // Pattern 2: fragment has extra (root missing → insert)
            int fragSkip = FindResyncForward(rootSamples, fragSamples, rootPos, fragPos, searchRoot: false);

            if (fragSkip > 0 && fragSkip <= WiggleConstants.MinWordsOverlap)
            {
                _root.Insert(rootPos, fragSamples.Slice(fragPos, fragSkip));
                rootPos += fragSkip;
                fragPos += fragSkip;
                status |= SectorStatus.DroppedBytesFixed;
                continue;
            }

            break;
        }
    }

    /// <summary>
    /// Searches backward for a resynchronization point. If searchRoot is true,
    /// tries skipping samples in the root to find where it re-aligns with the
    /// fragment. If false, tries skipping in the fragment.
    /// </summary>
    private static int FindResyncBackward(
        ReadOnlySpan<short> root, ReadOnlySpan<short> frag,
        int rootPos, int fragPos, bool searchRoot)
    {
        for (int skip = 1; skip <= WiggleConstants.MinWordsOverlap; skip++)
        {
            int rCheck, fCheck;

            if (searchRoot)
            {
                rCheck = rootPos - skip;
                fCheck = fragPos - 1;
            }
            else
            {
                rCheck = rootPos - 1;
                fCheck = fragPos - skip;
            }

            if (rCheck < 0 || fCheck < 0)
            {
                break;
            }

            if (root[rCheck] != frag[fCheck])
            {
                continue;
            }

            // Verify the resync with enough agreeing samples
            int agreeing = 1;
            int ra = rCheck - 1;
            int fa = fCheck - 1;

            while (ra >= 0 && fa >= 0 && root[ra] == frag[fa] && agreeing < WiggleConstants.MinWordsRift)
            {
                agreeing++;
                ra--;
                fa--;
            }

            if (agreeing >= WiggleConstants.MinWordsRift)
            {
                return skip;
            }
        }

        return 0;
    }

    /// <summary>
    /// Searches forward for a resynchronization point.
    /// </summary>
    private static int FindResyncForward(
        ReadOnlySpan<short> root, ReadOnlySpan<short> frag,
        int rootPos, int fragPos, bool searchRoot)
    {
        for (int skip = 1; skip <= WiggleConstants.MinWordsOverlap; skip++)
        {
            int rCheck, fCheck;

            if (searchRoot)
            {
                rCheck = rootPos + skip;
                fCheck = fragPos;
            }
            else
            {
                rCheck = rootPos;
                fCheck = fragPos + skip;
            }

            if (rCheck >= root.Length || fCheck >= frag.Length)
            {
                break;
            }

            if (root[rCheck] != frag[fCheck])
            {
                continue;
            }

            // Verify resync
            int agreeing = 1;
            int ra = rCheck + 1;
            int fa = fCheck + 1;

            while (ra < root.Length && fa < frag.Length && root[ra] == frag[fa] && agreeing < WiggleConstants.MinWordsRift)
            {
                agreeing++;
                ra++;
                fa++;
            }

            if (agreeing >= WiggleConstants.MinWordsRift)
            {
                return skip;
            }
        }

        return 0;
    }

    // ── Dynamic overlap adjustment ──────────────────────────────

    /// <summary>
    /// Recalculates the dynamic overlap window based on accumulated jitter
    /// measurements. Called after each new drive read.
    /// </summary>
    private void AdjustDynamicOverlap()
    {
        // Stage 2 drift compensation: correct systematic position creep
        if (_stage2Stats.OffsetPoints >= WiggleConstants.JitterMeasurementInterval)
        {
            long average = _stage2Stats.OffsetAccum / _stage2Stats.OffsetPoints;

            if (Math.Abs(average) > _dynamicOverlap / 4)
            {
                // Round to sector boundary
                long correction = (average / WiggleConstants.MinSectorEpsilon) * WiggleConstants.MinSectorEpsilon;
                _dynamicDrift += correction;

                // Adjust all cached block positions to compensate
                foreach (var block in _cache)
                {
                    block.Begin -= correction;
                }

                foreach (var frag in _fragments)
                {
                    frag.Begin -= correction;
                }
            }

            _stage2Stats.Reset();
        }

        // Stage 1 jitter: set overlap window based on observed variation
        if (_stage1Stats.OffsetPoints >= WiggleConstants.JitterMeasurementInterval)
        {
            long avgDiff = _stage1Stats.OffsetDiff / _stage1Stats.OffsetPoints;
            int newOverlap = (int)(avgDiff * 3);

            if (Math.Abs(_stage1Stats.OffsetMin) * 3 / 2 > newOverlap)
            {
                newOverlap = Math.Abs(_stage1Stats.OffsetMin) * 3 / 2;
            }

            if (_stage1Stats.OffsetMax * 3 / 2 > newOverlap)
            {
                newOverlap = _stage1Stats.OffsetMax * 3 / 2;
            }

            _dynamicOverlap = Math.Clamp(
                newOverlap,
                WiggleConstants.MinSectorEpsilon,
                WiggleConstants.MaxSectorOverlap * WiggleConstants.WordsPerSector);

            _stage1Stats.Reset();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Copies a verified sector from the root block into the output buffer.
    /// </summary>
    private void CopyFromRoot(long beginWord, Span<byte> outputBuffer)
    {
        if (_root.IsEmpty)
        {
            outputBuffer.Slice(0, CdConstants.SectorSize).Clear();
            return;
        }

        int rootIndex = (int)(beginWord - _root.Begin);

        // Safety: if the root doesn't fully cover the sector (can happen after
        // max-retry skip), zero-fill what we can't provide and copy what we can.
        if (rootIndex < 0 || rootIndex + WiggleConstants.WordsPerSector > _root.Size)
        {
            outputBuffer.Slice(0, CdConstants.SectorSize).Clear();

            int srcStart = Math.Max(0, rootIndex);
            int srcEnd = Math.Min(_root.Size, rootIndex + WiggleConstants.WordsPerSector);

            if (srcEnd > srcStart)
            {
                int dstByteOffset = (srcStart - rootIndex) * sizeof(short);
                var partial = MemoryMarshal.AsBytes(_root.Samples.Slice(srcStart, srcEnd - srcStart));
                partial.CopyTo(outputBuffer.Slice(dstByteOffset));
            }

            return;
        }

        var sourceSamples = _root.Samples.Slice(rootIndex, WiggleConstants.WordsPerSector);
        var sourceBytes = MemoryMarshal.AsBytes(sourceSamples);
        sourceBytes.CopyTo(outputBuffer);
    }

    /// <summary>
    /// When max retries are exhausted, force-accept whatever data we have
    /// for the target sector. If the root doesn't cover the target, read
    /// directly from the drive without verification.
    /// </summary>
    private void ForceAcceptSector(long beginWord, long endWord)
    {
        if (_root.Covers(beginWord, endWord))
        {
            return;
        }

        // Try merging any available fragments
        foreach (var frag in _fragments)
        {
            if (_root.IsEmpty)
            {
                _root.InitializeFrom(frag);
                continue;
            }

            SectorStatus dummy = SectorStatus.None;
            TryMergeFragment(frag, ref dummy);
        }

        if (_root.Covers(beginWord, endWord))
        {
            return;
        }

        // Try pulling data from cache blocks
        foreach (var block in _cache)
        {
            long overlapBegin = Math.Max(block.Begin, beginWord);
            long overlapEnd = Math.Min(block.End, endWord);

            if (overlapEnd <= overlapBegin)
            {
                continue;
            }

            if (_root.IsEmpty)
            {
                _root.Begin = overlapBegin;
                _root.Size = 0;
            }

            // Prepend if root starts after beginWord
            if (_root.Begin > beginWord && block.Begin <= beginWord)
            {
                int prependEnd = (int)Math.Min(_root.Begin, block.End) - (int)beginWord;
                int srcOffset = (int)(beginWord - block.Begin);
                int count = (int)Math.Min(prependEnd, _root.Begin - beginWord);

                if (count > 0)
                {
                    _root.Insert(0, block.Samples.Slice(srcOffset, count));
                    _root.Begin -= count;
                }
            }

            // Append if root ends before endWord
            if (_root.End < endWord && block.End > _root.End)
            {
                int srcOffset = (int)Math.Max(0, _root.End - block.Begin);
                int count = (int)Math.Min(block.End, endWord) - (int)Math.Max(_root.End, block.Begin);

                if (count > 0 && srcOffset + count <= block.Size)
                {
                    _root.Append(block.Samples.Slice(srcOffset, count));
                }
            }
        }

        // Last resort: zero-fill any remaining gaps
        if (!_root.Covers(beginWord, endWord))
        {
            if (_root.IsEmpty)
            {
                _root.Begin = beginWord;
                _root.Size = 0;
            }

            // Prepend zeros if needed
            if (_root.Begin > beginWord)
            {
                int count = (int)(_root.Begin - beginWord);
                Span<short> zeros = stackalloc short[Math.Min(count, 4096)];
                zeros.Clear();

                while (_root.Begin > beginWord)
                {
                    int chunk = (int)Math.Min(_root.Begin - beginWord, zeros.Length);
                    _root.Insert(0, zeros.Slice(0, chunk));
                    _root.Begin -= chunk;
                }
            }

            // Append zeros if needed
            if (_root.End < endWord)
            {
                Span<short> zeros = stackalloc short[Math.Min((int)(endWord - _root.End), 4096)];
                zeros.Clear();

                while (_root.End < endWord)
                {
                    int chunk = (int)Math.Min(endWord - _root.End, zeros.Length);
                    _root.Append(zeros.Slice(0, chunk));
                }
            }
        }
    }

    /// <summary>
    /// Discards cache blocks and fragments that are entirely behind the
    /// returned-data cursor.
    /// </summary>
    private void TrimCache()
    {
        long limit = _root.ReturnedLimit;

        if (limit < 0)
        {
            return;
        }

        for (int i = _cache.Count - 1; i >= 0; i--)
        {
            if (_cache[i].End <= limit)
            {
                _cache[i].Dispose();
                _cache.RemoveAt(i);
            }
        }

        for (int i = _fragments.Count - 1; i >= 0; i--)
        {
            if (_fragments[i].End <= limit)
            {
                _fragments[i].Dispose();
                _fragments.RemoveAt(i);
            }
        }

        _root.TrimBefore(limit);
    }

    private void DisposeCache()
    {
        foreach (var block in _cache)
        {
            block.Dispose();
        }

        _cache.Clear();
    }

    private void DisposeFragments()
    {
        foreach (var frag in _fragments)
        {
            frag.Dispose();
        }

        _fragments.Clear();
    }
}
