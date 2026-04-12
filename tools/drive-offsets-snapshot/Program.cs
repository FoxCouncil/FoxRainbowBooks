using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using FoxRedbook;

const string AccurateRipUrl = "http://www.accuraterip.com/driveoffsets.htm";

string repoRoot = FindRepoRoot();
string outputPath = Path.Combine(repoRoot, "src", "FoxRedbook", "Resources", "drive-offsets.bin");

Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

using var http = new HttpClient();
http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("Mozilla", "5.0"));
http.Timeout = TimeSpan.FromSeconds(30);

Console.Write("Fetching AccurateRip drive offsets... ");

List<(string Vendor, string Product, short Offset, uint Submissions)> entries;

try
{
    string html = await http.GetStringAsync(new Uri(AccurateRipUrl)).ConfigureAwait(false);
    entries = ParseAccurateRipHtml(html);
    Console.WriteLine($"OK, {entries.Count} raw entries.");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"FAILED: {ex.Message}");
    Console.Error.WriteLine("No source available. Aborting.");
    return 1;
}

if (entries.Count == 0)
{
    Console.Error.WriteLine("Parsed zero entries. Aborting — something is wrong with the source data.");
    return 1;
}

// Normalize all entries.
var normalized = new List<(string Vendor, string Product, short Offset, uint Submissions)>();
var seen = new HashSet<string>(StringComparer.Ordinal);

foreach (var (vendor, product, offset, submissions) in entries)
{
    string nVendor = DriveNameNormalizer.Normalize(vendor);
    string nProduct = DriveNameNormalizer.Normalize(product);

    if (nVendor.Length == 0 && nProduct.Length == 0)
    {
        continue;
    }

    string key = DriveNameNormalizer.BuildKey(nVendor, nProduct);

    if (seen.Add(key))
    {
        normalized.Add((nVendor, nProduct, offset, submissions));
    }
}

Console.WriteLine($"After normalization and dedup: {normalized.Count} entries.");

// Serialize.
DateTime snapshotDate = DateTime.UtcNow;
byte[] binary = KnownDriveOffsets.SerializeBinary(normalized, snapshotDate);

File.WriteAllBytes(outputPath, binary);

Console.WriteLine($"Snapshot written: {outputPath}");
Console.WriteLine($"  Entries: {normalized.Count}");
Console.WriteLine($"  Date:    {snapshotDate:yyyy-MM-dd}");
Console.WriteLine($"  Size:    {binary.Length:N0} bytes");

return 0;

// ── Parsers ────────────────────────────────────────────────

static List<(string Vendor, string Product, short Offset, uint Submissions)> ParseAccurateRipHtml(string html)
{
    // AccurateRip's page is FrontPage-era HTML. Data rows have 4 <td> cells
    // with <font> tags wrapping the text: drive name, offset, submissions,
    // percentage. First 2 <tr> rows are headers (bgcolor="#000000").
    var results = new List<(string, string, short, uint)>();

    // Match each <tr> block, then extract <td> contents.
    var trRegex = new Regex(@"<tr[^>]*>(.*?)</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    var tdRegex = new Regex(@"<td[^>]*>(.*?)</td>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
    var stripTagsRegex = new Regex(@"<[^>]+>");

    int rowIndex = 0;

    foreach (Match trMatch in trRegex.Matches(html))
    {
        rowIndex++;

        // Skip header rows.
        if (rowIndex <= 2)
        {
            continue;
        }

        var cells = tdRegex.Matches(trMatch.Groups[1].Value);

        if (cells.Count < 3)
        {
            continue;
        }

        string driveName = stripTagsRegex.Replace(cells[0].Groups[1].Value, "").Trim();
        string offsetStr = stripTagsRegex.Replace(cells[1].Groups[1].Value, "").Trim();
        string subsStr = stripTagsRegex.Replace(cells[2].Groups[1].Value, "").Trim();

        // Skip purged entries.
        if (offsetStr.Contains("[Purged]", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!short.TryParse(offsetStr, out short offset))
        {
            continue;
        }

        uint.TryParse(subsStr, out uint submissions);

        int sepIdx = driveName.IndexOf(" - ", StringComparison.Ordinal);

        if (sepIdx < 0)
        {
            results.Add((string.Empty, driveName, offset, submissions));
        }
        else
        {
            string vendor = driveName[..sepIdx];
            string product = driveName[(sepIdx + 3)..];
            results.Add((vendor, product, offset, submissions));
        }
    }

    return results;
}

static string FindRepoRoot()
{
    string dir = AppContext.BaseDirectory;

    while (dir is not null)
    {
        if (Directory.Exists(Path.Combine(dir, ".git")))
        {
            return dir;
        }

        dir = Path.GetDirectoryName(dir)!;
    }

    // Fallback: assume cwd.
    return Directory.GetCurrentDirectory();
}
