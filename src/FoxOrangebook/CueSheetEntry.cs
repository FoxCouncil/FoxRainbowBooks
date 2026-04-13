namespace FoxOrangebook;

/// <summary>
/// One 8-byte entry in a DAO cue sheet, describing a track boundary,
/// index point, or lead-in/lead-out marker. The drive uses the complete
/// cue sheet to pre-program the disc layout before any writes begin.
/// </summary>
public readonly record struct CueSheetEntry
{
    /// <summary>Control/ADR byte. Upper nibble = control (0x0 audio, 0x4 data), lower nibble = ADR (0x1 position).</summary>
    public required byte CtlAdr { get; init; }

    /// <summary>Track number. 0x00 = lead-in, 0x01–0x63 = tracks, 0xAA = lead-out.</summary>
    public required byte TrackNumber { get; init; }

    /// <summary>Index number. 0x00 = pre-gap, 0x01 = track start.</summary>
    public required byte Index { get; init; }

    /// <summary>Data form. 0x00 = CD-DA audio (2352 bytes).</summary>
    public required byte DataForm { get; init; }

    /// <summary>Serial Copy Management System. Typically 0x00.</summary>
    public required byte Scms { get; init; }

    /// <summary>Absolute minute in MSF addressing.</summary>
    public required byte Minute { get; init; }

    /// <summary>Absolute second in MSF addressing.</summary>
    public required byte Second { get; init; }

    /// <summary>Absolute frame in MSF addressing.</summary>
    public required byte Frame { get; init; }

    /// <summary>Lead-in track number constant.</summary>
    public const byte LeadInTrack = 0x00;

    /// <summary>Lead-out track number constant.</summary>
    public const byte LeadOutTrack = 0xAA;

    /// <summary>Data form for CD-DA audio.</summary>
    public const byte DataFormAudio = 0x00;

    /// <summary>CTL/ADR for audio with position.</summary>
    public const byte CtlAdrAudio = 0x01;

    /// <summary>
    /// Creates a lead-in entry at MSF 00:00:00.
    /// </summary>
    public static CueSheetEntry LeadIn() => new()
    {
        CtlAdr = CtlAdrAudio,
        TrackNumber = LeadInTrack,
        Index = 0x00,
        DataForm = DataFormAudio,
        Scms = 0x00,
        Minute = 0,
        Second = 0,
        Frame = 0,
    };

    /// <summary>
    /// Creates a lead-out entry at the given MSF position.
    /// </summary>
    public static CueSheetEntry LeadOut(byte min, byte sec, byte frame) => new()
    {
        CtlAdr = CtlAdrAudio,
        TrackNumber = LeadOutTrack,
        Index = 0x01,
        DataForm = DataFormAudio,
        Scms = 0x00,
        Minute = min,
        Second = sec,
        Frame = frame,
    };

    /// <summary>
    /// Creates an audio track pre-gap entry (index 0) at the given MSF.
    /// </summary>
    public static CueSheetEntry TrackPregap(byte trackNumber, byte min, byte sec, byte frame) => new()
    {
        CtlAdr = CtlAdrAudio,
        TrackNumber = trackNumber,
        Index = 0x00,
        DataForm = DataFormAudio,
        Scms = 0x00,
        Minute = min,
        Second = sec,
        Frame = frame,
    };

    /// <summary>
    /// Creates an audio track start entry (index 1) at the given MSF.
    /// </summary>
    public static CueSheetEntry TrackStart(byte trackNumber, byte min, byte sec, byte frame) => new()
    {
        CtlAdr = CtlAdrAudio,
        TrackNumber = trackNumber,
        Index = 0x01,
        DataForm = DataFormAudio,
        Scms = 0x00,
        Minute = min,
        Second = sec,
        Frame = frame,
    };

    /// <summary>
    /// Serializes this entry into an 8-byte span.
    /// </summary>
    public void WriteTo(Span<byte> destination)
    {
        destination[0] = CtlAdr;
        destination[1] = TrackNumber;
        destination[2] = Index;
        destination[3] = DataForm;
        destination[4] = Scms;
        destination[5] = Minute;
        destination[6] = Second;
        destination[7] = Frame;
    }
}
