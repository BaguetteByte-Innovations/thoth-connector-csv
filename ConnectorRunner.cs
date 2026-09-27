using System.Text;
using System.Text.Json;
using Thoth.Integration.Abstractions;

namespace Thoth.Connectors.Csv;

/// <summary>
/// Native variant of the reference CSV connector.
///
/// Philosophy: a connector PARSES, it does not CLEAN.
/// We read the CSV, build <see cref="ThothRawIdentity"/> instances as raw
/// as possible, and emit them on stdout as NDJSON. All the housekeeping
/// (trim, empty → null, format validation) is done engine-side.
///
/// Two reading modes, driven by the <c>rows_per_person</c> parameter:
/// <list type="bullet">
/// <item><b>one</b> (default) — one row is one person, rows are streamed straight out.</item>
/// <item><b>many</b> — several rows may describe the SAME person (three files, one per school; or
/// a long-format export with one row per teaching assignment). Rows are cross-referenced on an
/// anchor cell and folded into a single identity. The manifest declares
/// <c>JoinSourcesWhen</c> on that mode, so the engine hands every source over in ONE call.</item>
/// </list>
/// Merging is NOT cleaning: the anchor is normalized to COMPARE only, lists are concatenated
/// without deduplication (the engine owns the <c>Distinct</c>), and values travel raw.
///
/// <c>target_role</c> is read PER SOURCE (falling back to the call for a single-source or
/// single-role batch): one call may carry students, teachers and staff at once, and
/// <see cref="IngestionResult"/> counts them separately. In <c>many</c> mode the fold key is
/// therefore the couple (role, anchor) — see <c>MergeBuffer</c>.
///
/// The file's ENCODING is declared per source too (<c>encoding</c>, UTF-8 by default), and three
/// byte guards refuse — before the first identity — a file whose bytes contradict the declaration:
/// a byte-order mark of another encoding, a byte sequence invalid in the declared encoding, or a
/// UTF-8 file declared single-byte. See <see cref="Encodings"/>.
/// </summary>
internal static class ConnectorRunner
{
    // Parameter ids, shared with manifest.json. Kept as constants because inspect and ingest read
    // the same keys: a literal repeated in two places is a literal that drifts.
    private const string ParamDelimiter         = "delimiter";
    private const string ParamListDelimiter     = "list_delimiter";
    private const string ParamScopeDelimiter    = "scope_delimiter";
    private const string ParamHasHeader         = "has_header";
    private const string ParamEncoding          = "encoding";
    private const string ParamEncodingCustom    = "encoding_custom";
    private const string ParamDateFormat        = "date_format";
    private const string ParamDateFormatCustom  = "date_format_custom";
    private const string ParamTargetRole        = "target_role";
    private const string ParamRowsPerPerson     = "rows_per_person";
    private const string ParamMergeAnchor       = "merge_anchor";
    private const string ParamResidenceDistrict = "residence_district";

    private const string RowsPerPersonMany = "many";

    public static async Task<int> RunAsync()
    {
        // No BOM in either direction: no risk of a stray prefix on the NDJSON streams.
        Console.OutputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        Console.InputEncoding  = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

        // Tracked so that a failure answers in the shape of the mode it was asked in: the engine reads an
        // InspectionResult after an inspect, an IngestionResult otherwise. Same rule as the PowerShell
        // flavor (Emit-CrashResult).
        string? mode = null;
        try
        {
            string? requestLine = await Console.In.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(requestLine))
            {
                EmitFailure(mode, "No request received on stdin.");
                return 1;
            }

            ConnectorRequest? req = JsonSerializer.Deserialize(requestLine, ConnectorJsonContext.Default.ConnectorRequest);
            if (req is null)
            {
                EmitFailure(mode, "Invalid JSON request.");
                return 1;
            }
            mode = req.Mode;

            return req.Mode switch
            {
                "inspect" => await RunInspect(req),
                "ingest"  => await RunIngest(req),
                _         => HandleUnknownMode(req.Mode)
            };
        }
        catch (Exception ex)
        {
            EmitFailure(mode, $"Connector crash: {ex.Message}");
            return 1;
        }
    }

    private static int HandleUnknownMode(string mode)
    {
        EmitFailure(mode, $"Unknown mode: '{mode}'.");
        return 1;
    }

    private static async Task<int> RunInspect(ConnectorRequest req)
    {
        ConnectorContext context = req.Context;
        if (context.Sources is null || context.Sources.Count == 0)
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], "No file was provided."));
            return 0;
        }

        // Inspection describes ONE shape, so it reads ONE source. The engine inspects each source
        // of a batch separately (a single source per call), which is precisely what lets a batch
        // mix shapes: each gets its own mapping screen, and its own mapping at ingest.
        ConnectorSource source = context.Sources[0];
        Dictionary<string, string> settings = SettingsOf(context, source);

        string filePath = source.PhysicalPath;
        if (!File.Exists(filePath))
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], "File not found."));
            return 0;
        }

        string delimiter = settings.GetValueOrDefault(ParamDelimiter, ";");
        bool hasHeader = IsTrue(settings.GetValueOrDefault(ParamHasHeader, "true"));

        // `target_role` is declared IsRequired in the manifest: a silent default would turn a
        // teacher file into a student import — and since reconciliation is global per role, the
        // real students missing from that batch would become deletion candidates. Fail fast.
        if (!settings.TryGetValue(ParamTargetRole, out string? targetRoleParam)
            || string.IsNullOrWhiteSpace(targetRoleParam))
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], $"Missing required parameter '{ParamTargetRole}'."));
            return 0;
        }

        if (!TryParseTargetRole(targetRoleParam, out UserRole fixedRole))
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], $"Invalid user type: '{targetRoleParam}'."));
            return 0;
        }

        // The unusable settings ("custom" with nothing entered, an unknown encoding, a one-letter
        // date pattern) are judged without reading any data: refuse them HERE, in the panel the
        // operator just typed in, rather than two screens later. The ingest keeps its own check —
        // an autonomous run never inspects. Encoding first: it says how the bytes are read, before
        // the date format says how a cell is read.
        string? encodingName = ResolveEncodingName(settings);
        string? encodingError = Encodings.FindEncodingError(encodingName);
        if (encodingError is not null)
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], encodingError));
            return 0;
        }
        Encoding encoding = Encodings.Get(encodingName!);

        string? dateFormatError = DateFormats.FindFormatError(ResolveDateFormats(settings));
        if (dateFormatError is not null)
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], dateFormatError));
            return 0;
        }

        List<string> columns = [];
        List<Dictionary<string, string>> sampleData = [];

        try
        {
            // The bytes are judged BEFORE a line is decoded: a preview with the accents right, or a
            // named refusal — never a sample of U+FFFD the operator would have to interpret.
            using (FileStream probe = new(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536))
            {
                string? byteMismatch = Encodings.FindByteMismatch(probe, encoding, SourceLabel(source));
                if (byteMismatch is not null)
                {
                    EmitInspectionResult(new InspectionResult(false, false, [], [], byteMismatch));
                    return 0;
                }
            }

            using FileStream fs = new(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
            using StreamReader reader = new(fs, encoding, detectEncodingFromByteOrderMarks: true);

            string? headerLine = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(headerLine))
            {
                EmitInspectionResult(new InspectionResult(false, false, [], [], "The file is empty."));
                return 0;
            }

            string[] headers = headerLine.Split(delimiter);

            if (hasHeader)
            {
                // Trim the headers only, since they act as mapping keys
                // and must match exactly between Inspect and Ingest.
                foreach (string h in headers) columns.Add(h.Trim());
            }
            else
            {
                for (int i = 0; i < headers.Length; i++) columns.Add(PositionalColumnName(i));
                sampleData.Add(BuildSampleRow(columns, headers));
            }

            int rowsRead = 0;
            string? dataLine;
            while (rowsRead < 5 && (dataLine = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(dataLine)) continue;
                sampleData.Add(BuildSampleRow(columns, dataLine.Split(delimiter)));
                rowsRead++;
            }

            EmitInspectionResult(new InspectionResult(
                Success:               true,
                RequiresManualMapping: true,
                SourceFields:          columns,
                SampleData:            sampleData,
                FixedRole:             fixedRole));
            return 0;
        }
        catch (Exception ex)
        {
            EmitInspectionResult(new InspectionResult(false, false, [], [], $"Analysis error: {ex.Message}"));
            return 0;
        }
    }

    private static async Task<int> RunIngest(ConnectorRequest req)
    {
        string syncId = req.SyncId ?? string.Empty;

        ConnectorContext context = req.Context;
        if (context.Sources is null || context.Sources.Count == 0)
        {
            EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0, "No file was provided."));
            return 0;
        }

        // The role is declared PER SOURCE: one call may carry the students, the teachers and the
        // staff at once, and IngestionResult has three counters precisely so it can say so. It is
        // resolved for EVERY source before the first identity is built — same reason as the
        // structural guard below: refusing halfway would leave a half batch behind.
        //
        // `target_role` is declared IsRequired in the manifest: a silent default would turn a
        // teacher file into a student import — and since reconciliation is global per role, the
        // real students missing from that batch would become deletion candidates. Fail fast,
        // on an absent value as well as on an unreadable one, and NAME the source at fault.
        UserRole[] rolesBySource = new UserRole[context.Sources.Count];
        for (int i = 0; i < context.Sources.Count; i++)
        {
            ConnectorSource source = context.Sources[i];
            Dictionary<string, string> sourceSettings = SettingsOf(context, source);

            if (!sourceSettings.TryGetValue(ParamTargetRole, out string? targetRole)
                || string.IsNullOrWhiteSpace(targetRole))
            {
                EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0,
                    $"Missing required parameter '{ParamTargetRole}' for '{SourceLabel(source)}'."));
                return 0;
            }

            if (!TryParseTargetRole(targetRole, out UserRole role))
            {
                EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0,
                    $"Invalid user type: '{targetRole}' for '{SourceLabel(source)}'."));
                return 0;
            }

            rolesBySource[i] = role;
        }

        // Cross-referencing stays a decision of the CALL: it folds the sources of one call
        // together, so it cannot differ from one source to the next.
        bool merging = IsMerging(context.Parameters);
        string mergeAnchor =
            Cleaned(context.Parameters.GetValueOrDefault(ParamMergeAnchor)) ?? nameof(ThothRawIdentity.SamAccountName);

        try
        {
            // Every source is checked against ITS mapping BEFORE the first identity is built: a
            // mapped column the file no longer carries is a refusal, never a shrug (see below).
            string? structureError = await FindSourceStructureErrorAsync(context);
            if (structureError is not null)
            {
                EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0, structureError));
                return 0;
            }

            RoleTally tally = new();
            MergeBuffer? buffer = merging ? new MergeBuffer() : null;

            // Process EVERY source, each with ITS OWN role, ITS OWN mapping and ITS OWN reading
            // settings. That is what lets a reference file and its complement, two schools exporting
            // different columns, or three files describing three different populations, fold into
            // one batch — and, in `many` mode, into one identity per
            // person.
            for (int i = 0; i < context.Sources.Count; i++)
            {
                ConnectorSource source = context.Sources[i];
                Dictionary<string, string> settings = SettingsOf(context, source);

                ReadOptions options = new(
                    Role:              rolesBySource[i],
                    ListDelimiter:     settings.GetValueOrDefault(ParamListDelimiter,  ","),
                    ScopeDelimiter:    settings.GetValueOrDefault(ParamScopeDelimiter, "#"),
                    HasHeader:         IsTrue(settings.GetValueOrDefault(ParamHasHeader, "true")),
                    // The `!` is legitimate: the structural pass above already refused, for EVERY
                    // source, a null name ("custom" with nothing entered) and an unknown one.
                    Encoding:          Encodings.Get(ResolveEncodingName(settings)!),
                    // Read PER SOURCE, like its neighbours: two files of one batch may state two
                    // different date formats. The fallback is DateFormats.Default, which MUST equal
                    // the manifest's DefaultValue — the manifest is read by the FRONT only, so an
                    // autonomous run never sees it and lands here.
                    DateFormats:       ResolveDateFormats(settings),
                    ResidenceDistrict: Cleaned(settings.GetValueOrDefault(ParamResidenceDistrict)),
                    MergeAnchor:       mergeAnchor);

                int processed = await ProcessFile(
                    source,
                    source.Mapping!,
                    options,
                    settings.GetValueOrDefault(ParamDelimiter, ";"),
                    buffer);

                // Count on the PARSED role, not on the raw string: `target_role` is matched
                // case-insensitively, so a "teacher" would otherwise land in the students bucket
                // while its identities carry Role = Teacher.
                tally.Add(options.Role, processed);
            }

            // Merged mode counts IDENTITIES, not rows: ProcessFile returns 0 while filling the
            // buffer, and the folded population is only known once the last source is read. The
            // buffer counts by role on its own — a folded identity belongs to the role of the
            // fragments that built it, which the (role, anchor) key keeps homogeneous.
            if (buffer is not null) tally = buffer.Flush();

            EmitIngestionResult(new IngestionResult(
                true, syncId, tally.Students, tally.Teachers, tally.Staff));
            return 0;
        }
        catch (ConnectorIngestException ex)
        {
            // Refusal we authored: the message is already the operator-facing one, no wrapping.
            EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0, ex.Message));
            return 0;
        }
        catch (Exception ex)
        {
            EmitIngestionResult(new IngestionResult(false, syncId, 0, 0, 0, $"Ingestion error: {ex.Message}"));
            return 0;
        }
    }

    private static async Task<int> ProcessFile(
        ConnectorSource source,
        MappingProfile mapping,
        ReadOptions options,
        string delimiter,
        MergeBuffer? buffer)
    {
        string sourceLabel = SourceLabel(source);

        using FileStream fs = new(source.PhysicalPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
        using StreamReader reader = new(fs, options.Encoding, detectEncodingFromByteOrderMarks: true);

        string? firstLine = await reader.ReadLineAsync();
        if (string.IsNullOrWhiteSpace(firstLine)) return 0;

        string[] headers = firstLine.Split(delimiter);
        ColumnIndexes idx = ColumnIndexes.From(mapping, headers, options.HasHeader);
        SourceLayout layout = new(idx, source.Scope);

        int anchorIndex = buffer is null ? -1 : ResolveAnchorColumn(options.MergeAnchor, idx);

        int usersProcessed = 0;
        int lineNumber = 1;

        if (!options.HasHeader)
        {
            usersProcessed += Handle(headers, lineNumber);
        }

        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line)) continue;
            usersProcessed += Handle(line.Split(delimiter), lineNumber);
        }

        return usersProcessed;

        int Handle(string[] values, int number)
        {
            ThothRawIdentity identity = BuildIdentity(values, options, layout);

            if (buffer is null)
            {
                EmitIdentity(identity);
                return 1;
            }

            string? anchor = anchorIndex < values.Length ? values[anchorIndex] : null;
            if (string.IsNullOrWhiteSpace(anchor))
            {
                // Emitting the unfolded row would reintroduce the very duplicate this mode removes,
                // and the batch would carry an identity nothing can reconcile. Fail loudly.
                throw new ConnectorIngestException(
                    $"Cross-referencing is enabled but line {number} of '{sourceLabel}' has no value "
                    + $"in the column mapped to '{options.MergeAnchor}'.");
            }

            buffer.Add(options.Role, anchor, identity);
            return 0;
        }
    }

    /// <summary>
    /// Column index of the merge anchor. Reuses the indexes already resolved for this file rather
    /// than walking <c>FieldMappings</c> again: a second resolution path would diverge on the first
    /// awkward case (two columns aiming at the same target).
    /// </summary>
    private static int ResolveAnchorColumn(string anchor, ColumnIndexes idx)
    {
        int index;
        if (anchor.Equals(nameof(ThothRawIdentity.SourceId), StringComparison.OrdinalIgnoreCase))
            index = idx.SourceId;
        else if (anchor.Equals(nameof(ThothRawIdentity.SamAccountName), StringComparison.OrdinalIgnoreCase))
            index = idx.SamAccountName;
        else if (anchor.Equals(nameof(ThothRawIdentity.UserPrincipalName), StringComparison.OrdinalIgnoreCase))
            index = idx.UserPrincipalName;
        else
            throw new ConnectorIngestException($"Unknown cross-referencing key: '{anchor}'.");

        if (index < 0)
            throw new ConnectorIngestException(
                $"Cross-referencing is enabled on '{anchor}' but no column is mapped to that target.");

        return index;
    }

    /// <summary>
    /// Effective reading settings of a source: its own when the operator set some for it, the
    /// call's otherwise. The engine has ALREADY merged them (batch ← source), so this picks one
    /// dictionary or the other and never merges anything.
    /// </summary>
    private static Dictionary<string, string> SettingsOf(ConnectorContext context, ConnectorSource source) =>
        source.Parameters ?? context.Parameters;

    /// <summary>Human-readable name of a source, for error messages.</summary>
    private static string SourceLabel(ConnectorSource source) =>
        string.IsNullOrWhiteSpace(source.OriginalName)
            ? Path.GetFileName(source.PhysicalPath)
            : source.OriginalName;

    private static bool IsMerging(Dictionary<string, string> parameters) =>
        parameters.GetValueOrDefault(ParamRowsPerPerson, "one")
            .Trim()
            .Equals(RowsPerPersonMany, StringComparison.OrdinalIgnoreCase);

    private static bool IsTrue(string value) => value.Equals("true", StringComparison.OrdinalIgnoreCase);

    private static string? Cleaned(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    /// <summary>
    /// Structural check of EVERY source, run as a preliminary pass — before the first identity is
    /// built. It must stay a pass over the whole call: in `rows_per_person = one` the connector
    /// emits as it reads, so a check done when opening each file would let the first two sources
    /// emit before the third is refused — exactly the half-batch this guard exists to forbid.
    ///
    /// Three things are checked, both per source and against ITS OWN mapping and settings:
    /// <list type="bullet">
    /// <item>a source with no mapping at all — a free-schema connector cannot read a column without
    /// being told which one;</item>
    /// <item>the encoding: a sound setting, then bytes that do not contradict it
    /// (<see cref="Encodings.FindByteMismatch"/>) — checked BEFORE the header line is decoded,
    /// because headers decoded askew (`Pr�nom`) would not find the mapped column `Prénom`, and the
    /// refusal would say "column missing" instead of "wrong encoding";</item>
    /// <item>a mapped column the source does not carry (see below).</item>
    /// </list>
    /// Returns the operator-facing message, or null when every source is readable.
    ///
    /// A mapped column the source does not carry is a REFUSAL, not a shrug. Its index would be -1,
    /// every cell would read as null, and for the engine `null` ≡ empty list ≡ WIPE of the
    /// dimension: the night a source renames `Groupes`, an autonomous run would strip every
    /// pedagogical group off every student without a word. The optional dimensions are precisely
    /// the ones that bleed — a missing FirstName fails validation loudly, a missing Groupes does
    /// not. Failing here demands the human decision (re-map) instead of taking it alone.
    ///
    /// Checked PER FILE — a sibling carrying the column proves nothing about this one — and BEFORE
    /// the first identity is built, so a refusal never leaves half a batch behind.
    ///
    /// Without a header the mapping is positional (`Column_N`) and the check reads as a column
    /// COUNT; the defect is worse there, since a shift produces silently WRONG identities rather
    /// than merely empty ones.
    ///
    /// A column PRESENT but empty on a row is out of scope: that is the source saying "no value for
    /// this person", and the resulting wipe is wanted. This guard is about the file's structure.
    /// Returns the operator-facing message, or null when every mapped column is present.
    /// </summary>
    private static async Task<string?> FindSourceStructureErrorAsync(ConnectorContext context)
    {
        foreach (ConnectorSource source in context.Sources)
        {
            if (source.Mapping is null || source.Mapping.FieldMappings is null)
                return "The mapping profile is missing.";

            Dictionary<string, string> settings = SettingsOf(context, source);
            string delimiter = settings.GetValueOrDefault(ParamDelimiter, ";");
            bool hasHeader = IsTrue(settings.GetValueOrDefault(ParamHasHeader, "true"));

            // Same checks as the inspection, because the autonomous path NEVER inspects. They cost
            // nothing and they are the only place an autonomous run can hear about a typo.
            string? encodingName = ResolveEncodingName(settings);
            string? encodingError = Encodings.FindEncodingError(encodingName);
            if (encodingError is not null) return encodingError;
            Encoding encoding = Encodings.Get(encodingName!);

            // The bytes are judged on a dedicated stream, thrown away afterwards: the guard reads
            // the whole file, and the reader below must start from the first byte.
            using (FileStream probe = new(source.PhysicalPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536))
            {
                string? byteMismatch = Encodings.FindByteMismatch(probe, encoding, SourceLabel(source));
                if (byteMismatch is not null) return byteMismatch;
            }

            string[] dateFormats = ResolveDateFormats(settings);
            string? dateFormatError = DateFormats.FindFormatError(dateFormats);
            if (dateFormatError is not null) return dateFormatError;

            using (FileStream fs = new(source.PhysicalPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true))
            using (StreamReader reader = new(fs, encoding, detectEncodingFromByteOrderMarks: true))
            {
                string? firstLine = await reader.ReadLineAsync();

                // An empty source carries no column, but it produces no identity either: ProcessFile
                // skips it. Refusing here would turn a harmless no-op into a failed run.
                if (string.IsNullOrWhiteSpace(firstLine)) continue;

                string[] headers = firstLine.Split(delimiter);

                foreach (KeyValuePair<string, string> kvp in source.Mapping.FieldMappings)
                {
                    if (string.IsNullOrWhiteSpace(kvp.Key)) continue;
                    if (FindColumn(kvp.Key, headers, hasHeader) >= 0) continue;

                    return $"Mapped column '{kvp.Key}' was not found in '{SourceLabel(source)}'.";
                }

                // A WHOLE column of dates the stated format cannot read is an operator mistake, and
                // it is said once, here, instead of one cell at a time over 2 500 lines. An isolated
                // unreadable cell stays a null: BirthDate is an OPTIONAL target, and in autonomous
                // mode the all-or-nothing rule would make one stray N/C cost the whole night.
                int dateIndex = GetColumnIndex(nameof(ThothRawIdentity.BirthDate), source.Mapping, headers, hasHeader);
                if (dateIndex < 0) continue;

                List<string?> sample = [];
                int nonBlank = 0;
                int rowsRead = 0;

                void Collect(string[] values)
                {
                    string? cell = dateIndex < values.Length ? values[dateIndex] : null;
                    sample.Add(cell);
                    if (!string.IsNullOrWhiteSpace(cell)) nonBlank++;
                    rowsRead++;
                }

                // Without a header the first line IS a data row — that is already how inspection
                // counts it.
                if (!hasHeader) Collect(headers);

                // Two bounds, each earning its keep: 20 non-blank cells are enough to decide, and
                // the 200-row ceiling stops a column that is sparse at the top of the file from
                // making us read all of it — without creating the blind spot of a 20-ROW sample
                // that happens to be empty on this column.
                string? dataLine;
                while (nonBlank < 20 && rowsRead < 200 && (dataLine = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(dataLine)) continue;
                    Collect(dataLine.Split(delimiter));
                }

                // Zero non-blank cell is NOT an error: a mapped but empty column is a legitimate file.
                if (!DateFormats.IsSystemicMismatch(sample, dateFormats)) continue;

                string columnName = MappedColumnName(nameof(ThothRawIdentity.BirthDate), source.Mapping) ?? string.Empty;
                string firstValue = string.Empty;
                foreach (string? cell in sample)
                {
                    if (string.IsNullOrWhiteSpace(cell)) continue;
                    firstValue = cell;
                    break;
                }

                return $"Column '{columnName}' in '{SourceLabel(source)}': none of the {nonBlank} dates read "
                     + $"matches the format '{string.Join('|', dateFormats)}'. First value read: '{firstValue}'.";
            }
        }

        return null;
    }

    // ==============================================================================
    // PROTOCOL IO (idy:/log:/res: prefixes)
    // ==============================================================================

    private static void EmitIdentity(ThothRawIdentity id)
    {
        string json = JsonSerializer.Serialize(id, ConnectorJsonContext.Default.ThothRawIdentity);
        Console.Out.Write(ConnectorLinePrefix.Identity);
        Console.Out.WriteLine(json);
    }

    private static void EmitLog(string level, string message)
    {
        string json = JsonSerializer.Serialize(new ConnectorLogPayload(level, message), ConnectorJsonContext.Default.ConnectorLogPayload);
        Console.Out.Write(ConnectorLinePrefix.Log);
        Console.Out.WriteLine(json);
    }

    private static void EmitInspectionResult(InspectionResult result)
    {
        string json = JsonSerializer.Serialize(result, ConnectorJsonContext.Default.InspectionResult);
        Console.Out.Write(ConnectorLinePrefix.Result);
        Console.Out.WriteLine(json);
    }

    private static void EmitIngestionResult(IngestionResult result)
    {
        string json = JsonSerializer.Serialize(result, ConnectorJsonContext.Default.IngestionResult);
        Console.Out.Write(ConnectorLinePrefix.Result);
        Console.Out.WriteLine(json);
    }

    /// <summary>
    /// A failure result shaped for the mode: an <see cref="InspectionResult"/> after an inspect, an
    /// <see cref="IngestionResult"/> otherwise (unknown mode, or a request that could not be read).
    /// </summary>
    private static void EmitFailure(string? mode, string message)
    {
        if (mode == "inspect")
            EmitInspectionResult(new InspectionResult(false, false, [], [], message));
        else
            EmitIngestionResult(new IngestionResult(false, string.Empty, 0, 0, 0, message));
    }

    // ==============================================================================
    // PARSING — source → ThothRawIdentity conversion only.
    // No sanitization (the engine takes care of it).
    // ==============================================================================

    private static Dictionary<string, string> BuildSampleRow(List<string> columns, string[] values)
    {
        Dictionary<string, string> row = [];
        for (int i = 0; i < columns.Count; i++)
            row[columns[i]] = i < values.Length ? values[i] : string.Empty;
        return row;
    }

    private static ThothRawIdentity BuildIdentity(string[] values, ReadOptions options, SourceLayout layout)
    {
        ColumnIndexes idx = layout.Indexes;

        string? Get(int i) => i >= 0 && i < values.Length ? values[i] : null;

        List<string>? GetList(int i)
        {
            string? raw = Get(i);
            return raw is null ? null : [.. raw.Split(options.ListDelimiter)];
        }

        // Assignment schools, as the row DECLARES them. When the source says nothing, the school the
        // operator attached to the file stands in — not a guessed default: it is an explicit
        // statement ("this file is school X"). A student has no assignment, so nothing is injected.
        List<string>? schools = GetList(idx.Schools);
        if (options.Role is UserRole.Teacher or UserRole.Staff
            && IsBlank(schools)
            && !string.IsNullOrWhiteSpace(layout.FileScope))
            schools = [layout.FileScope];

        // The school an UNPREFIXED assignment belongs to. A row naming exactly one school says it
        // without ambiguity; a row naming several does not, and falls back to the file's school.
        // Null lets the engine backfill its default assignment school.
        //
        // ORDER MATTERS: this reads `schools`, which the block above may have filled from the file.
        // Computing it first would leave every unprefixed assignment of a per-school export in the
        // engine's default school — silently, and for the whole batch.
        string? lineSchool = SingleSchoolOrNull(schools) ?? layout.FileScope;

        List<ScopedAssignment>? GetScopedList(int i)
        {
            string? raw = Get(i);
            if (raw is null) return null;

            List<ScopedAssignment> list = [];
            foreach (string item in raw.Split(options.ListDelimiter))
            {
                int sep = item.IndexOf(options.ScopeDelimiter, StringComparison.Ordinal);
                if (sep >= 0)
                    list.Add(new ScopedAssignment(item[..sep], item[(sep + options.ScopeDelimiter.Length)..]));
                else
                    // No explicit prefix: the assignment belongs to this row's school. Null
                    // here would let the engine backfill the GLOBAL default school, which pulls
                    // every file of a per-school export into a single school.
                    list.Add(new ScopedAssignment(lineSchool, item));
            }
            return list;
        }

        // Residence: the mapped column cell > the batch's transverse district > the school stated on
        // the file > null, which the engine backfills with the default scope.
        //
        // The file's school is handed over RAW, for every role alike: the ENGINE interprets it
        // (a student's school is their residence, a teacher's or a staff member's resolves to that
        // school's dedicated district). A connector transports a
        // residence, it does not know what one is.
        //
        // The transverse district is the ONE role-conditioned term, and it is so because the SETTING
        // is: its VisibleWhen restricts it to teachers and staff, while it is asked once for the
        // whole call. A mixed batch would otherwise leak it onto the students.
        //
        // IsNullOrWhiteSpace serves ONLY the choice between cell and fallback: the retained value
        // is passed RAW, and the engine sanitizes/uppercases.
        string? scopeCell = Get(idx.Scope);
        string? transverseDistrict = options.Role is UserRole.Teacher or UserRole.Staff
            ? options.ResidenceDistrict
            : null;
        string? scopeFallback = transverseDistrict ?? layout.FileScope;

        return new ThothRawIdentity(
            Role:              options.Role,
            SourceId:          Get(idx.SourceId),
            SamAccountName:    Get(idx.SamAccountName),
            UserPrincipalName: Get(idx.UserPrincipalName),
            FirstName:         Get(idx.FirstName),
            LastName:          Get(idx.LastName),
            BirthDate:         DateFormats.Parse(Get(idx.BirthDate), options.DateFormats),

            Scope:                    string.IsNullOrWhiteSpace(scopeCell) ? scopeFallback : scopeCell,
            Class:                    Get(idx.Class),
            StudentPedagogicalGroups: GetList(idx.StudentGroups),
            Schools:                  schools,

            TeacherClasses:           GetScopedList(idx.TeacherClasses),
            TeacherPedagogicalGroups: GetScopedList(idx.TeacherGroups),
            TeacherSubjects:          GetScopedList(idx.TeacherSubjects),

            Description:   Get(idx.Description)
        );
    }

    private static bool IsBlank(List<string>? values)
    {
        if (values is null || values.Count == 0) return true;
        foreach (string value in values)
        {
            if (!string.IsNullOrWhiteSpace(value)) return false;
        }
        return true;
    }

    /// <summary>
    /// The one school a row names, or null when it names none or several — in which case the row
    /// does not say which school an unprefixed assignment belongs to.
    /// Blank entries are skipped for the COUNT only: the value returned travels RAW, like everything
    /// a connector emits.
    /// </summary>
    private static string? SingleSchoolOrNull(List<string>? schools)
    {
        if (schools is null) return null;

        string? found = null;
        foreach (string school in schools)
        {
            if (string.IsNullOrWhiteSpace(school)) continue;
            if (found is not null) return null;
            found = school;
        }
        return found;
    }

    /// <summary>
    /// Reading options resolved for ONE source. <c>MergeAnchor</c> is the only run-level member:
    /// cross-referencing folds the sources of a call together, so it comes from the call.
    /// </summary>
    private readonly record struct ReadOptions(
        UserRole Role,
        string ListDelimiter,
        string ScopeDelimiter,
        bool HasHeader,
        Encoding Encoding,
        string[] DateFormats,
        string? ResidenceDistrict,
        string MergeAnchor);

    /// <summary>What a given source resolved to: its column indexes and the school it describes.</summary>
    private readonly record struct SourceLayout(ColumnIndexes Indexes, string? FileScope);

    /// <summary>An operator-facing refusal: the message is surfaced as-is in the ingestion result.</summary>
    private sealed class ConnectorIngestException(string message) : Exception(message);

    /// <summary>
    /// The three counters of an <see cref="IngestionResult"/>, filled as the sources are read. A
    /// call carrying three roles fills all three: the role is declared per source, which is why the
    /// result has three counters.
    /// </summary>
    private struct RoleTally
    {
        public int Students;
        public int Teachers;
        public int Staff;

        public void Add(UserRole role, int count)
        {
            switch (role)
            {
                case UserRole.Teacher: Teachers += count; break;
                case UserRole.Staff:   Staff    += count; break;
                default:               Students += count; break;
            }
        }
    }

    // ==============================================================================
    // CROSS-REFERENCING — fold the fragments of one person into a single identity.
    // ==============================================================================

    /// <summary>
    /// Accumulates the fragments of a run, keyed by the couple (role, normalized anchor), and folds
    /// them. O(population of the call) in memory — 20 000 identities is a non-issue, and it is the
    /// price of cross-referencing: nothing can be emitted before the last source has been read.
    /// </summary>
    /// <remarks>
    /// The ROLE is part of the key. Two sources of a same call may describe two different
    /// populations — students split over two files PLUS a teacher file is a legitimate batch — and
    /// folding on the anchor alone would merge a teacher into a student who happens to share it.
    /// Refusing the combination instead would forbid precisely the batch this connector exists to
    /// serve.
    /// </remarks>
    private sealed class MergeBuffer
    {
        // Trim + case-insensitive, for COMPARISON only: `SOULARD.N1` and `soulard.n1` are the same
        // person to the engine, so they must be the same person here. The emitted values stay raw.
        private readonly Dictionary<string, int> _byKey = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<ThothRawIdentity> _identities = [];
        private readonly List<string> _anchors = [];
        private int _residenceConflicts;

        public void Add(UserRole role, string anchor, ThothRawIdentity identity)
        {
            string trimmed = anchor.Trim();
            // NUL cannot appear in a line read by ReadLine, so no anchor can spoof another role's key.
            string key = $"{(int)role}\u0000{trimmed}";

            if (!_byKey.TryGetValue(key, out int position))
            {
                _byKey[key] = _identities.Count;
                _identities.Add(identity);
                _anchors.Add(trimmed);
                return;
            }

            _identities[position] = Fold(_identities[position], identity, _anchors[position]);
        }

        /// <summary>
        /// Emits the folded identities in order of first appearance and returns their count, split
        /// by role. Every fragment of a bucket shares its role by construction (it is half the
        /// key), so the kept identity's own role is the bucket's.
        /// </summary>
        public RoleTally Flush()
        {
            RoleTally tally = new();
            foreach (ThothRawIdentity identity in _identities)
            {
                EmitIdentity(identity);
                tally.Add(identity.Role, 1);
            }

            if (_residenceConflicts > 0)
                EmitLog("Warning",
                    $"{_residenceConflicts} people appear in several files stating different schools: "
                    + "each kept the residence of the first file read. Map a Residence column, or fill "
                    + "'residence_district', to state it explicitly.");

            return tally;
        }

        private ThothRawIdentity Fold(ThothRawIdentity kept, ThothRawIdentity incoming, string anchor) =>
            kept with
            {
                SourceId          = Scalar(kept.SourceId,          incoming.SourceId,          nameof(kept.SourceId),          anchor),
                SamAccountName    = Scalar(kept.SamAccountName,    incoming.SamAccountName,    nameof(kept.SamAccountName),    anchor),
                UserPrincipalName = Scalar(kept.UserPrincipalName, incoming.UserPrincipalName, nameof(kept.UserPrincipalName), anchor),
                FirstName         = Scalar(kept.FirstName,         incoming.FirstName,         nameof(kept.FirstName),         anchor),
                LastName          = Scalar(kept.LastName,          incoming.LastName,          nameof(kept.LastName),          anchor),
                BirthDate         = Date(kept.BirthDate,           incoming.BirthDate,         nameof(kept.BirthDate),         anchor),
                Scope             = Residence(kept.Scope,          incoming.Scope),
                Class             = Scalar(kept.Class,             incoming.Class,             nameof(kept.Class),             anchor),
                Description       = Scalar(kept.Description,       incoming.Description,       nameof(kept.Description),       anchor),

                // Concatenate, NEVER deduplicate: the engine deduplicates, and the contract forbids
                // a connector from cleaning what it reads.
                StudentPedagogicalGroups = Concat(kept.StudentPedagogicalGroups, incoming.StudentPedagogicalGroups),
                Schools                  = Concat(kept.Schools,                  incoming.Schools),
                TeacherClasses           = Concat(kept.TeacherClasses,           incoming.TeacherClasses),
                TeacherPedagogicalGroups = Concat(kept.TeacherPedagogicalGroups, incoming.TeacherPedagogicalGroups),
                TeacherSubjects          = Concat(kept.TeacherSubjects,          incoming.TeacherSubjects)
            };

        /// <summary>
        /// First non-empty wins. A later fragment disagreeing is reported as a warning, never as a
        /// failure: dropping a 3 000-teacher import over one divergent birth date is out of
        /// proportion with the defect.
        /// </summary>
        private string? Scalar(string? kept, string? incoming, string field, string anchor)
        {
            if (string.IsNullOrWhiteSpace(incoming)) return kept;
            if (string.IsNullOrWhiteSpace(kept)) return incoming;

            // Case and padding differences are not worth a warning: the kept value is emitted raw,
            // and the engine sanitizes it.
            if (!kept.Trim().Equals(incoming.Trim(), StringComparison.OrdinalIgnoreCase))
                WarnConflict(anchor, field, kept.Trim(), incoming.Trim());

            return kept;
        }

        private DateOnly? Date(DateOnly? kept, DateOnly? incoming, string field, string anchor)
        {
            if (incoming is null) return kept;
            if (kept is null) return incoming;

            if (kept != incoming)
                WarnConflict(anchor, field, kept.Value.ToString("O"), incoming.Value.ToString("O"));

            return kept;
        }

        /// <summary>
        /// Residence folds like any other scalar — first non-empty wins — but its conflicts are
        /// counted rather than reported one by one. A divergent first name is a defect OF THE
        /// PERSON, so naming them is the useful thing; a divergent residence is a defect OF THE
        /// BATCH (per-school exports that never state where people live), and repeating it per
        /// person buries the one line that matters.
        /// </summary>
        private string? Residence(string? kept, string? incoming)
        {
            if (string.IsNullOrWhiteSpace(incoming)) return kept;
            if (string.IsNullOrWhiteSpace(kept)) return incoming;

            if (!kept.Trim().Equals(incoming.Trim(), StringComparison.OrdinalIgnoreCase))
                _residenceConflicts++;

            return kept;
        }

        private static void WarnConflict(string anchor, string field, string kept, string ignored) =>
            EmitLog("Warning", $"'{anchor}': conflicting '{field}' across rows — kept '{kept}', ignored '{ignored}'.");

        private static List<T>? Concat<T>(List<T>? kept, List<T>? incoming)
        {
            if (incoming is null || incoming.Count == 0) return kept;
            if (kept is null) return incoming;

            kept.AddRange(incoming);
            return kept;
        }
    }

    private readonly record struct ColumnIndexes(
        int SourceId, int SamAccountName, int UserPrincipalName,
        int FirstName, int LastName, int BirthDate,
        int Scope, int Class,
        int StudentGroups, int Schools,
        int TeacherClasses, int TeacherGroups, int TeacherSubjects,
        int Description)
    {
        public static ColumnIndexes From(MappingProfile mapping, string[] headers, bool hasHeader)
        {
            int Lookup(string target) => GetColumnIndex(target, mapping, headers, hasHeader);
            return new ColumnIndexes(
                SourceId:           Lookup(nameof(ThothRawIdentity.SourceId)),
                SamAccountName:     Lookup(nameof(ThothRawIdentity.SamAccountName)),
                UserPrincipalName:  Lookup(nameof(ThothRawIdentity.UserPrincipalName)),
                FirstName:          Lookup(nameof(ThothRawIdentity.FirstName)),
                LastName:           Lookup(nameof(ThothRawIdentity.LastName)),
                BirthDate:          Lookup(nameof(ThothRawIdentity.BirthDate)),
                Scope:              Lookup(nameof(ThothRawIdentity.Scope)),
                Class:              Lookup(nameof(ThothRawIdentity.Class)),
                StudentGroups:      Lookup(nameof(ThothRawIdentity.StudentPedagogicalGroups)),
                Schools:            Lookup(nameof(ThothRawIdentity.Schools)),
                TeacherClasses:     Lookup(nameof(ThothRawIdentity.TeacherClasses)),
                TeacherGroups:      Lookup(nameof(ThothRawIdentity.TeacherPedagogicalGroups)),
                TeacherSubjects:    Lookup(nameof(ThothRawIdentity.TeacherSubjects)),
                Description:        Lookup(nameof(ThothRawIdentity.Description))
            );
        }
    }

    /// <summary>
    /// Index of the column mapped to <paramref name="target"/> — a ThothRawIdentity property name,
    /// traveling in <c>FieldMappings</c>, whose values the engine never interprets.
    /// </summary>
    private static int GetColumnIndex(string target, MappingProfile mapping, string[] headers, bool hasHeader)
    {
        string? columnName = MappedColumnName(target, mapping);
        if (columnName is null) return -1;

        return FindColumn(columnName, headers, hasHeader);
    }

    /// <summary>Name of the source column mapped to <paramref name="target"/>, null when unmapped.</summary>
    private static string? MappedColumnName(string target, MappingProfile mapping)
    {
        foreach (KeyValuePair<string, string> kvp in mapping.FieldMappings)
        {
            if (kvp.Value == target) return kvp.Key;
        }
        return null;
    }

    /// <summary>
    /// The date patterns stated for ONE source. The fallback MUST stay equal to the manifest's
    /// <c>date_format.DefaultValue</c>: the manifest is a front-only file, so an autonomous run
    /// lands on <see cref="DateFormats.Default"/> and a second value would read one file two ways.
    /// </summary>
    private static string[] ResolveDateFormats(Dictionary<string, string> settings) =>
        DateFormats.Resolve(
            settings.GetValueOrDefault(ParamDateFormat, DateFormats.Default),
            settings.GetValueOrDefault(ParamDateFormatCustom));

    /// <summary>
    /// The encoding name stated for ONE source. The fallback MUST stay equal to the manifest's
    /// <c>encoding.DefaultValue</c> — same reason as <see cref="ResolveDateFormats"/>.
    /// </summary>
    private static string? ResolveEncodingName(Dictionary<string, string> settings) =>
        Encodings.ResolveName(
            settings.GetValueOrDefault(ParamEncoding, Encodings.Default),
            settings.GetValueOrDefault(ParamEncodingCustom));

    /// <summary>
    /// The role a file is declared to carry. Only the three importable roles are accepted, compared
    /// without regard to case — the same allow-list as the PowerShell flavor. Enum.TryParse would also
    /// let through "ServiceAccount", which the engine refuses, and any number.
    /// </summary>
    private static bool TryParseTargetRole(string value, out UserRole role)
    {
        role = value.ToUpperInvariant() switch
        {
            "STUDENT" => UserRole.Student,
            "TEACHER" => UserRole.Teacher,
            "STAFF"   => UserRole.Staff,
            _         => UserRole.Unknown
        };
        return role != UserRole.Unknown;
    }

    private static int FindColumn(string columnName, string[] headers, bool hasHeader)
    {
        for (int i = 0; i < headers.Length; i++)
        {
            string currentHeader = hasHeader ? headers[i].Trim() : PositionalColumnName(i);
            if (currentHeader.Equals(columnName, StringComparison.OrdinalIgnoreCase)) return i;
        }
        return -1;
    }

    /// <summary>
    /// Name of a column in a file without a header row: <c>Column_1</c>, <c>Column_2</c>… in file
    /// order. The inspection announces these names and a mapping refers to them, so inspection and
    /// ingestion must build them the same way. The PowerShell flavor uses the same names.
    /// </summary>
    private static string PositionalColumnName(int index) => $"Column_{index + 1}";
}
