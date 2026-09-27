using System.Globalization;
using System.Text;

namespace Thoth.Connectors.Csv;

/// <summary>
/// How the BYTES of a file are read. The operator STATES the encoding, the connector never guesses
/// it: a Windows-1252 export read as UTF-8 would turn every accent into U+FFFD in silence, and a
/// UTF-8 file read as Windows-1252 would turn it into two garbled characters — both plausible,
/// both wrong, neither reported. Here a wrong setting ends in a NAMED refusal, in both directions,
/// before a single identity is emitted.
///
/// Pure logic, deliberately free of any dependency on the rest of the connector, so it can be
/// tested on its own.
///
/// The PowerShell sibling (`script.ps1`) implements the SAME rules with the same messages: two
/// reference connectors must read the same bytes the same way.
/// </summary>
internal static class Encodings
{
    /// <summary>
    /// MUST equal manifest.json → encoding.DefaultValue (both connectors), the `encoding` key of
    /// both config.example.json, and $script:EncodingDefault in script.ps1. The manifest is read by
    /// the FRONT only: an autonomous run never sees it and falls back on this constant.
    /// </summary>
    internal const string Default = "utf-8";

    /// <summary>Value of `encoding` that opens the free-text `encoding_custom` field.</summary>
    private const string CustomOption = "custom";

    /// <summary>Size of one block handed to the decoders by <see cref="FindByteMismatch"/>.</summary>
    private const int ChunkSize = 65536;

    /// <summary>The code page of UTF-8: the multi-byte encoding a line number is exact for.</summary>
    private const int Utf8CodePage = 65001;

    /// <summary>
    /// The code-page tables live in the shared framework but are not registered by default.
    /// Registering here — and only here — means every caller (connector and tests alike) gets them.
    /// RegisterProvider is idempotent; a second call is harmless.
    /// </summary>
    static Encodings() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    /// <summary>
    /// A strict UTF-8, used by the second guard to ask "are these bytes valid UTF-8?" of a file
    /// declared single-byte. Never handed to a reader.
    /// </summary>
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// The name retained for one source, or null when the operator picked "custom" and typed
    /// nothing — which ends in a refusal (<see cref="FindEncodingError"/>), never in a silent
    /// fallback on the default. A Trim is the only hygiene: this is a setting the operator typed,
    /// not a value of the file.
    /// </summary>
    internal static string? ResolveName(string? encoding, string? custom)
    {
        if (string.IsNullOrWhiteSpace(encoding)) return Default;

        if (!encoding.Trim().Equals(CustomOption, StringComparison.OrdinalIgnoreCase))
            return encoding.Trim();

        return string.IsNullOrWhiteSpace(custom) ? null : custom.Trim();
    }

    /// <summary>
    /// The operator-facing refusal when the SETTING itself is unusable (nothing entered, unknown
    /// name), null when it is sound. Shared by the inspection (immediate feedback in the panel) and
    /// by the structural pass (the autonomous path, which never inspects).
    ///
    /// The ONE place <see cref="Get"/> is wrapped in a try/catch: callers only call it after a null
    /// answer from here, so a defensive catch elsewhere would one day swallow a real error.
    /// </summary>
    internal static string? FindEncodingError(string? name)
    {
        if (name is null)
            return "No encoding was entered. Write the name of the file's encoding, e.g. 'windows-1252'.";

        try
        {
            Get(name);
            return null;
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException)
        {
            // ArgumentException: a name or number no table answers to. NotSupportedException: a
            // code page the framework knows of but ships no table for.
            return $"Unknown encoding '{name}'. Use an IANA name (e.g. 'windows-1252', 'iso-8859-15') or a code page number (e.g. 850).";
        }
    }

    /// <summary>
    /// A STRICT encoding for a name <see cref="FindEncodingError"/> accepted: invalid bytes throw,
    /// never replace. Accepts an IANA name or a code page number — `GetEncoding("1252")` throws,
    /// while the field's description promises the number.
    /// </summary>
    internal static Encoding Get(string name) =>
        int.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out int codePage)
            ? Encoding.GetEncoding(codePage, EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback)
            : Encoding.GetEncoding(name,     EncoderFallback.ExceptionFallback, DecoderFallback.ExceptionFallback);

    /// <summary>
    /// The byte guards, run over the whole stream in 64 KiB blocks. Returns the operator-facing
    /// message, or null when the bytes are consistent with the DECLARED encoding.
    ///
    /// The declared encoding is the only judge — the operator states, the connector never guesses,
    /// and a file is either read as stated or refused. Three refusals, in this order:
    /// <list type="bullet">
    /// <item><b>Guard 0</b> — the file carries a byte-order mark announcing ANOTHER encoding than the
    /// declared one (a UTF-8 mark under a windows-1252 declaration, a UTF-16 mark under utf-8): the
    /// file is not what it is said to be, the mark says so. A mark that AGREES with the declaration
    /// is just the first character of the file.</item>
    /// <item><b>Guard 1</b> — at least one byte sequence is invalid in the declared encoding: the
    /// file is not what it is said to be. Named with the byte and its place.</item>
    /// <item><b>Guard 2</b> — the declared encoding is single-byte and the WHOLE file is valid UTF-8
    /// with at least one byte ≥ 0x80: the file is UTF-8, not what it is said to be. A pure-ASCII
    /// file trips no guard — any setting reads it right.</item>
    /// </list>
    ///
    /// Every refusal is a VERDICT — which file, what it is not, on what evidence — and nothing more:
    /// no hint at which encoding to state instead. The option labels of the setting carry the only
    /// help there is.
    ///
    /// The stream is read from its current position (the caller opens it at 0) to its end; it is
    /// neither rewound nor closed. Nothing is decoded for keeping: the character buffers are the
    /// price of <c>Decoder.GetChars</c>, the only call that carries the decoder's state from one
    /// block to the next — its count-only sibling is stateless and would throw on a sequence cut
    /// by a block boundary.
    /// </summary>
    internal static string? FindByteMismatch(Stream bytes, Encoding declared, string sourceLabel)
    {
        byte[] buffer = new byte[ChunkSize];
        int read = ReadBlock(bytes, buffer);

        // Guard 0. The mark is RECOGNIZED on the first bytes; when it agrees with the declaration,
        // those bytes then go through the decoder like any other (a UTF-8 mark is valid UTF-8, a
        // UTF-16 mark is a valid U+FEFF — they pass, and the reader strips them).
        Encoding? marked = DetectByteOrderMark(buffer, read);
        if (marked is not null && marked.CodePage != declared.CodePage)
        {
            return $"'{sourceLabel}' is not {WebName(declared)}: it carries a {WebName(marked)} byte-order mark.";
        }

        Decoder strict = declared.GetDecoder();
        char[] chars = new char[declared.GetMaxCharCount(ChunkSize)];

        // The UTF-8 probe of guard 2 is only in play for a single-byte declared encoding, and
        // leaves play at the first invalid sequence: the file is then not UTF-8, nothing more to ask.
        Decoder? utf8Probe = declared.IsSingleByte ? StrictUtf8.GetDecoder() : null;
        char[]? utf8Chars = utf8Probe is null ? null : new char[StrictUtf8.GetMaxCharCount(ChunkSize)];

        long bytesBefore = 0;     // bytes handed to the decoder before the current block
        long newlinesBefore = 0;  // 0x0A seen before the current block
        bool sawHighByte = false;

        while (read > 0)
        {
            int newlinesInBlock = 0;
            for (int i = 0; i < read; i++)
            {
                byte b = buffer[i];
                if (b == 0x0A) newlinesInBlock++;
                else if (b >= 0x80) sawHighByte = true;
            }

            try
            {
                strict.GetChars(buffer, 0, read, chars, 0, flush: false);
            }
            catch (DecoderFallbackException ex)
            {
                return Guard1(sourceLabel, declared, ex, buffer, bytesBefore, newlinesBefore);
            }

            if (utf8Probe is not null)
            {
                try
                {
                    utf8Probe.GetChars(buffer, 0, read, utf8Chars!, 0, flush: false);
                }
                catch (DecoderFallbackException)
                {
                    utf8Probe = null;
                }
            }

            bytesBefore    += read;
            newlinesBefore += newlinesInBlock;
            read = ReadBlock(bytes, buffer);
        }

        // A sequence cut by the END of the file throws here and nowhere else.
        try
        {
            strict.GetChars(buffer, 0, 0, chars, 0, flush: true);
        }
        catch (DecoderFallbackException ex)
        {
            return Guard1(sourceLabel, declared, ex, buffer, bytesBefore, newlinesBefore);
        }

        if (utf8Probe is not null)
        {
            try
            {
                utf8Probe.GetChars(buffer, 0, 0, utf8Chars!, 0, flush: true);
            }
            catch (DecoderFallbackException)
            {
                utf8Probe = null;
            }
        }

        if (utf8Probe is not null && sawHighByte)
        {
            return $"'{sourceLabel}' is not {WebName(declared)}: its bytes are utf-8.";
        }

        return null;
    }

    /// <summary>
    /// The refusal of guard 1. The faulty byte is read in <c>ex.BytesUnknown</c>, never in
    /// <c>buffer[ex.Index]</c>: the index may be NEGATIVE — a sequence head held back from the
    /// previous block, or a sequence cut by the end of the file at flush — and then designates a
    /// byte of a block no longer there. The position formula still lands on the right byte; the
    /// line count taken BEFORE the block holds for any negative index, since the bytes a decoder
    /// holds back are sequence heads or continuations (≥ 0x80), never a 0x0A. Not clamped to 0:
    /// that would accuse the first byte of the block, which is innocent.
    /// </summary>
    private static string Guard1(
        string sourceLabel, Encoding declared, DecoderFallbackException ex,
        byte[] buffer, long bytesBefore, long newlinesBefore)
    {
        byte faulty = ex.BytesUnknown is { Length: > 0 } unknown ? unknown[0] : (byte)0;
        long position = bytesBefore + ex.Index;

        long line = newlinesBefore + 1;
        for (int i = 0; i < ex.Index; i++)
        {
            if (buffer[i] == 0x0A) line++;
        }

        // A line is counted on 0x0A: exact in UTF-8 and in a single-byte encoding, approximate in
        // UTF-16/32 (a 0A may be the low byte of another character) — there, the byte offset alone.
        string where = declared.CodePage == Utf8CodePage || declared.IsSingleByte
            ? $"line {line}"
            : $"offset {position}";

        // The verdict and nothing else: which byte, where. Which encoding to state instead is the
        // operator's call, helped by the option labels of the setting.
        return $"'{sourceLabel}' is not valid {WebName(declared)}: byte 0x{faulty:X2} at {where} cannot be read.";
    }

    /// <summary>
    /// The name an encoding is called by in a message. Lower-cased so the two flavors say the same
    /// word: .NET Framework answers `Windows-1252` where .NET answers `windows-1252`.
    /// </summary>
    private static string WebName(Encoding encoding) => encoding.WebName.ToLowerInvariant();

    /// <summary>
    /// The strict encoding a byte-order mark announces, null without one. UTF-32 LE is tested
    /// BEFORE UTF-16 LE — both start with FF FE — which is also the order StreamReader uses.
    /// </summary>
    private static Encoding? DetectByteOrderMark(byte[] buffer, int length)
    {
        if (length >= 4 && buffer[0] == 0xFF && buffer[1] == 0xFE && buffer[2] == 0x00 && buffer[3] == 0x00)
            return new UTF32Encoding(bigEndian: false, byteOrderMark: false, throwOnInvalidCharacters: true);

        if (length >= 4 && buffer[0] == 0x00 && buffer[1] == 0x00 && buffer[2] == 0xFE && buffer[3] == 0xFF)
            return new UTF32Encoding(bigEndian: true, byteOrderMark: false, throwOnInvalidCharacters: true);

        if (length >= 3 && buffer[0] == 0xEF && buffer[1] == 0xBB && buffer[2] == 0xBF)
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        if (length >= 2 && buffer[0] == 0xFF && buffer[1] == 0xFE)
            return new UnicodeEncoding(bigEndian: false, byteOrderMark: false, throwOnInvalidBytes: true);

        if (length >= 2 && buffer[0] == 0xFE && buffer[1] == 0xFF)
            return new UnicodeEncoding(bigEndian: true, byteOrderMark: false, throwOnInvalidBytes: true);

        return null;
    }

    /// <summary>
    /// Fills the buffer as far as the stream allows. A single <c>Read</c> may return fewer bytes
    /// than asked without being at the end; looping keeps the blocks — and the line arithmetic —
    /// aligned on 64 KiB.
    /// </summary>
    private static int ReadBlock(Stream bytes, byte[] buffer)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int n = bytes.Read(buffer, total, buffer.Length - total);
            if (n <= 0) break;
            total += n;
        }
        return total;
    }
}
