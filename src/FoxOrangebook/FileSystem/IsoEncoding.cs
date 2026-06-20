using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace FoxOrangebook.FileSystem;

/// <summary>
/// Numeric, string, and date encoders for the ISO 9660 / ECMA-119 on-disc
/// formats. The cryptic method names (7.1.1, 7.3.3, …) match the section
/// numbers in ECMA-119 where each representation is defined, so the layout
/// code reads against the spec.
/// </summary>
internal static class IsoEncoding
{
    // ── Integer representations (ECMA-119 §7) ────────────────────

    /// <summary>7.1.1 — 8-bit unsigned.</summary>
    public static void Write711(Span<byte> dst, byte value)
    {
        dst[0] = value;
    }

    /// <summary>7.2.3 — 16-bit, both-byte-orders (LSB then MSB), 4 bytes total.</summary>
    public static void Write723(Span<byte> dst, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(dst.Slice(0, 2), value);
        BinaryPrimitives.WriteUInt16BigEndian(dst.Slice(2, 2), value);
    }

    /// <summary>7.3.1 — 32-bit, little-endian.</summary>
    public static void Write731(Span<byte> dst, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(dst.Slice(0, 4), value);
    }

    /// <summary>7.3.2 — 32-bit, big-endian.</summary>
    public static void Write732(Span<byte> dst, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(0, 4), value);
    }

    /// <summary>7.3.3 — 32-bit, both-byte-orders (LSB then MSB), 8 bytes total.</summary>
    public static void Write733(Span<byte> dst, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(dst.Slice(0, 4), value);
        BinaryPrimitives.WriteUInt32BigEndian(dst.Slice(4, 4), value);
    }

    // ── String representations ───────────────────────────────────

    /// <summary>
    /// Writes an a-characters / d-characters string padded with spaces to the
    /// full field width. Used for PVD identifier fields (system, volume, etc.).
    /// </summary>
    public static void WriteAString(Span<byte> dst, string? value)
    {
        dst.Fill((byte)' ');

        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        int n = Math.Min(dst.Length, value.Length);

        for (int i = 0; i < n; i++)
        {
            char c = value[i];
            dst[i] = c is >= (char)0x20 and < (char)0x7F ? (byte)c : (byte)'_';
        }
    }

    /// <summary>
    /// Writes a UCS-2 big-endian string (Joliet) padded with U+0020 spaces to
    /// the full field width. The width is in bytes and must be even.
    /// </summary>
    public static void WriteJolietString(Span<byte> dst, string? value)
    {
        for (int i = 0; i + 1 < dst.Length; i += 2)
        {
            dst[i] = 0x00;
            dst[i + 1] = (byte)' ';
        }

        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        int maxChars = dst.Length / 2;
        int n = Math.Min(maxChars, value.Length);
        Encoding.BigEndianUnicode.GetBytes(value.AsSpan(0, n), dst.Slice(0, n * 2));
    }

    // ── Date / time representations (ECMA-119 §8.4.26, §9.1.5) ────

    /// <summary>
    /// 8.4.26 — 17-byte "digits" date for volume descriptors:
    /// YYYYMMDDHHMMSScc + 1-byte GMT offset in 15-minute intervals.
    /// </summary>
    public static void WriteVolumeDateTime(Span<byte> dst, DateTimeOffset when)
    {
        Span<char> buf = stackalloc char[16];
        var inv = CultureInfo.InvariantCulture;
        when.Year.TryFormat(buf.Slice(0, 4), out _, "D4", inv);
        when.Month.TryFormat(buf.Slice(4, 2), out _, "D2", inv);
        when.Day.TryFormat(buf.Slice(6, 2), out _, "D2", inv);
        when.Hour.TryFormat(buf.Slice(8, 2), out _, "D2", inv);
        when.Minute.TryFormat(buf.Slice(10, 2), out _, "D2", inv);
        when.Second.TryFormat(buf.Slice(12, 2), out _, "D2", inv);
        (when.Millisecond / 10).TryFormat(buf.Slice(14, 2), out _, "D2", inv);

        for (int i = 0; i < 16; i++)
        {
            dst[i] = (byte)buf[i];
        }

        dst[16] = GmtOffsetIntervals(when);
    }

    /// <summary>
    /// Writes a 17-byte "unset" volume date (all ASCII '0', zero offset),
    /// the spec's representation of "no date specified".
    /// </summary>
    public static void WriteUnsetVolumeDateTime(Span<byte> dst)
    {
        dst.Slice(0, 16).Fill((byte)'0');
        dst[16] = 0;
    }

    /// <summary>
    /// 9.1.5 — 7-byte directory-record date: years-since-1900, month, day,
    /// hour, minute, second, GMT offset in 15-minute intervals.
    /// </summary>
    public static void WriteDirectoryDateTime(Span<byte> dst, DateTimeOffset when)
    {
        dst[0] = (byte)(when.Year - 1900);
        dst[1] = (byte)when.Month;
        dst[2] = (byte)when.Day;
        dst[3] = (byte)when.Hour;
        dst[4] = (byte)when.Minute;
        dst[5] = (byte)when.Second;
        dst[6] = unchecked((byte)(sbyte)GmtOffsetSigned(when));
    }

    private static byte GmtOffsetIntervals(DateTimeOffset when)
    {
        return unchecked((byte)(sbyte)GmtOffsetSigned(when));
    }

    private static int GmtOffsetSigned(DateTimeOffset when)
    {
        int intervals = (int)(when.Offset.TotalMinutes / 15);
        return Math.Clamp(intervals, -48, 52);
    }
}
