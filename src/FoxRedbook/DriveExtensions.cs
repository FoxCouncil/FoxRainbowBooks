namespace FoxRedbook;

/// <summary>
/// High-level convenience extensions over <see cref="IOpticalDrive"/> that
/// wrap the common "read everything about this disc" workflow into a single
/// call.
/// </summary>
public static class DriveExtensions
{
    /// <summary>
    /// Reads the table of contents, fingerprints the disc, and reads CD-Text
    /// in a single call. The returned <see cref="DiscInfo"/> contains
    /// everything the library can determine about the disc without ripping
    /// audio: TOC, MusicBrainz / freedb / AccurateRip disc IDs, and CD-Text
    /// if the disc has it.
    /// </summary>
    /// <param name="drive">The drive to query. Must already have a disc inserted.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>
    /// A fully-populated <see cref="DiscInfo"/>. <see cref="DiscInfo.CdText"/>
    /// is <see langword="null"/> when the disc has no CD-Text data — which is
    /// the common case (roughly 80–90% of commercial CDs).
    /// </returns>
    /// <remarks>
    /// <para>
    /// CD-Text reads are exception-tolerant. Some drives signal "no CD-Text
    /// on this disc" by returning an error instead of an empty response,
    /// and callers of this high-level convenience method shouldn't have a
    /// successful disc-info read fail because of that. If the disc has no
    /// CD-Text or the drive errors when reading it, <see cref="DiscInfo.CdText"/>
    /// will be <see langword="null"/> and this method completes normally.
    /// </para>
    /// <para>
    /// Lower-level consumers that want the raw exception can call
    /// <see cref="IOpticalDrive.ReadCdTextAsync"/> directly.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="drive"/> is null.</exception>
    public static async Task<DiscInfo> ReadDiscInfoAsync(
        this IOpticalDrive drive,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(drive);

        TableOfContents toc = await drive.ReadTocAsync(cancellationToken).ConfigureAwait(false);

        CdText? cdText;

        try
        {
            cdText = await drive.ReadCdTextAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OpticalDriveException)
        {
            // Common case: drive reports "format 5 not supported" or similar
            // when the disc has no CD-Text. Treat as "no CD-Text present".
            cdText = null;
        }

        DiscInfo info = DiscFingerprint.Compute(toc);
        return info with { CdText = cdText };
    }
}
