using System.Globalization;

namespace Thoth.Connectors.Csv;

/// <summary>
/// How a date cell is read. The operator STATES the format, the connector never guesses it:
/// `04/07/2011` is a valid date in `dd/MM` as in `MM/dd`, so a guess would produce a wrong and
/// plausible date instead of a diagnosable refusal.
///
/// Pure logic, deliberately free of any dependency on the rest of the connector, so it can be
/// tested on its own.
///
/// The PowerShell sibling (`script.ps1` / `thoth-protocol.ps1`) implements the SAME rules with the
/// same messages: two reference connectors must read the same bytes the same way.
/// </summary>
internal static class DateFormats
{
    /// <summary>
    /// MUST equal manifest.json → date_format.DefaultValue (and the `date_format` key of
    /// config.example.json, and the hard-coded default of script.ps1). The manifest is read by the
    /// FRONT only: an autonomous run never sees it and falls back on this constant, so two
    /// different values would read one file one way by hand and another way at night.
    ///
    /// ISO 8601 because it is the only format no other one can imitate: when the source is not in
    /// it, the parse FAILS and the guard names the mismatch, instead of silently swapping day and
    /// month.
    /// </summary>
    internal const string Default = "yyyy-MM-dd";

    /// <summary>Value of `date_format` that opens the free-text `date_format_custom` field.</summary>
    private const string CustomOption = "custom";

    /// <summary>Separator between two patterns of the free-text field.</summary>
    private const char CustomSeparator = '|';

    /// <summary>
    /// The patterns retained for one source, tried in the stated order. An EMPTY array means the
    /// operator picked "custom" without entering anything: that ends in a refusal
    /// (<see cref="FindFormatError"/>), never in a silent fallback on the default.
    /// </summary>
    internal static string[] Resolve(string? dateFormat, string? custom)
    {
        if (string.IsNullOrWhiteSpace(dateFormat)) return [Default];

        if (!dateFormat.Equals(CustomOption, StringComparison.OrdinalIgnoreCase))
            return [dateFormat];

        if (custom is null) return [];

        List<string> patterns = [];
        foreach (string piece in custom.Split(CustomSeparator))
        {
            string trimmed = piece.Trim();
            if (trimmed.Length > 0) patterns.Add(trimmed);
        }
        return [.. patterns];
    }

    /// <summary>
    /// The operator-facing refusal when the SETTING itself is unusable, null when it is sound.
    /// Kept apart from <see cref="Resolve"/> so each sentence lives in exactly one place, shared by
    /// the inspection (immediate feedback in the panel) and by the structural pass (the autonomous
    /// path, which never inspects).
    /// </summary>
    internal static string? FindFormatError(string[] formats)
    {
        if (formats.Length == 0)
            return "No date format was entered. Write the format the dates are in, e.g. 'dd/MM/yyyy'.";

        foreach (string format in formats)
        {
            // A ONE-character format string is a standard specifier, not a pattern: "d" does not
            // mean "the day", it means "the culture's short date" — i.e. MM/dd/yyyy under the
            // invariant culture. An operator typing `d` would get back exactly the defect this
            // whole setting exists to fix, in silence.
            if (format.Length == 1)
                return $"Date format '{format}' is a single-letter standard specifier, not a pattern. Write the full pattern, e.g. 'd/M/yyyy'.";
        }

        return null;
    }

    /// <summary>
    /// One cell → a date. Null when the cell is absent, empty, blank, or matches no stated pattern
    /// (garbage in, null out): an isolated dirty cell leaves that person without a date, it never
    /// fails the import. A `null` erases nothing engine-side: the engine treats a null scalar as
    /// "not said".
    ///
    /// AllowWhiteSpaces, not None: a hand-filled `; 15/03/2010;` must pass. This is not cleaning —
    /// a type conversion is TOTAL, nothing of the original string survives in a DateOnly, so
    /// tolerating padding changes no emitted value. A fully blank cell still fails.
    /// </summary>
    internal static DateOnly? Parse(string? cell, string[] formats)
    {
        if (string.IsNullOrEmpty(cell) || formats.Length == 0) return null;

        // No culture is ever NAMED: under InvariantGlobalization, PredefinedCulturesOnly is true
        // and CultureInfo.GetCultureInfo("fr-FR") throws. The pattern carries the format,
        // InvariantCulture is only the separator table. An absurd pattern returns false, it never
        // throws — no defensive try/catch here, it would swallow a real error one day.
        return DateOnly.TryParseExact(cell, formats, CultureInfo.InvariantCulture,
                                      DateTimeStyles.AllowWhiteSpaces, out DateOnly parsed)
            ? parsed
            : null;
    }

    /// <summary>
    /// True when the sample PROVES the stated format is the wrong one: at least one non-blank cell,
    /// and not a single one parses. An empty or all-blank sample is not an error — a mapped but
    /// empty column is a legitimate file.
    ///
    /// This separates two events one unreadable cell may signal: the operator picked the wrong
    /// format (the WHOLE column fails ⇒ named refusal) versus the data is dirty (one cell ⇒ null).
    /// </summary>
    internal static bool IsSystemicMismatch(IReadOnlyList<string?> sample, string[] formats)
    {
        bool sawValue = false;

        foreach (string? cell in sample)
        {
            if (string.IsNullOrWhiteSpace(cell)) continue;
            sawValue = true;
            if (Parse(cell, formats) is not null) return false;
        }

        return sawValue;
    }
}
