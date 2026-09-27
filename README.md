# Thoth.Connectors.Csv

The **reference connector** for Thoth: a CSV connector built as a **standalone native
AOT executable** that the engine launches as a child process and talks to over stdin/stdout. This is
the example to read to ship a third-party native connector that does **not** link against
the engine.

> **A connector parses; it does not clean.** It reads the CSV, builds `ThothRawIdentity` instances as
> raw as possible, and emits them as NDJSON on stdout. All housekeeping (trim, empty-to-null, format
> validation) is done engine-side.

## Flavor

This connector exists in two flavors. This is the **native** flavor. The PowerShell flavor
(`thoth-connector-csv-ps`) implements the **same** behavior: same settings, same cascade, same
refusals, same messages. Anything documented below holds for both.

## What it handles

| Source shape | Supported | How |
|---|---|---|
| One file, one population | yes | nothing to declare |
| One file carrying the school in a column | yes | map that column (`Scope` for students, *Assigned schools* — field `Schools` — for teachers and staff) |
| One file per school, school **not** in the data | yes | state the school on each file at upload |
| Teachers whose values carry their own school (`SCHOOL#SUBJECT`) | yes | nothing to declare — the prefix wins over everything |
| **One person spread over several rows** (long format: one row per class, per subject) | yes | turn cross-referencing on and name the anchor |
| **One person spread over several files** (one export per school) | yes | idem — the sources are then handed over in a single call |
| Files with **different columns**, cross-referenced (a reference file plus its complement) | yes | each source carries its own column mapping |
| Files with **different delimiters** in one batch | yes | reading settings are adjustable per detected shape |
| **Several roles** in one batch (students, teachers, staff) | yes | the role is declared file by file |
| A role varying **row by row** (a `Role` column) | **no** | deliberate: reconciliation is global per role present, and a mixed file makes a partial population too easy to submit |
| Values containing the delimiter, a quote or a line break (RFC 4180 quoting) | **no** | each line is split on every delimiter and quotes are kept as part of the value: pick a delimiter that never appears in the data |

⚠ Cross-referencing requires a **stable key** in the source. A source carrying neither its own
identifier, nor a sAM, nor a UPN cannot be folded.

Without a header row (`has_header` unchecked), the columns are named `Column_1`, `Column_2`… in
file order and the first line is data. The mapping then becomes positional: a column inserted by
the source shifts every value after it, and produces wrong identities rather than empty ones. A
header row is preferable whenever the source can produce one.

## Settings

Where a setting is asked is declared by the manifest, and the frontend renders it accordingly.

| Setting | Asked | Purpose |
|---|---|---|
| `csv_files` | at upload, with a **school box per file** | the sources; the box names the school a file describes |
| `delimiter`, `list_delimiter`, `scope_delimiter`, `has_header` | batch, **adjustable per shape** | how a source is read |
| `encoding` | batch, **adjustable per shape** | how the bytes of the file are written. `utf-8` by default; `windows-1252` for an Excel "CSV" on Windows or most older exports; Latin-9 and Latin-1 in the list. A file that is not in the stated encoding is **refused** (see below); a byte-order mark that agrees with the setting is simply stripped |
| `encoding_custom` | batch, **adjustable per shape**, when `encoding` is `custom` | any IANA name (`ibm850`, `macintosh`, `utf-16`) or code page number (`1252`, `850`) |
| `date_format` | batch, **adjustable per shape** | how dates are written in the file. `yyyy-MM-dd` by default — ISO 8601 is the only form no other one can imitate, so a wrong setting **fails** instead of silently swapping day and month |
| `date_format_custom` | batch, **adjustable per shape**, when `date_format` is `custom` | a free-text .NET pattern. Several patterns separated by a vertical bar are tried in the stated order, for a file glued together from two exports |
| `target_role` | **per file** | which population a file carries |
| `rows_per_person` | batch | whether several rows may describe one person |
| `merge_anchor` | batch, when cross-referencing | which mapped identifier folds the rows: source id, sAM or UPN |
| `residence_district` | batch, teachers and staff only | a **transverse** district of residence, for people who do not reside in the district of their file's school |

## Residence and assignments

Two independent cascades. Neither ever guesses: each link is explicit, and the leftmost wins.

```
residence   ←  column mapped to Scope  >  residence_district (teachers, staff)  >  the file's school  >  engine default
assignment  ←  "SCHOOL#value" prefix   >  the ONE school the row names in Schools  >  the file's school  >  engine default
```

The file's school states one thing — *this file describes school X* — and both axes follow from the
role. It is handed over **raw**: the engine derives the residence from it, a student's school being
their residence, a teacher's or a staff member's resolving to that school's dedicated district.
`residence_district` comes before it because a setting **states** a residence where the file's school
only **implies** one; it is the way out for a transverse district (education authority, local
authority), not the ordinary path.

A row naming exactly one school in *Assigned schools* says, without ambiguity, where its unprefixed
assignments belong; a row naming several does not, and falls back to the file's school. For a
teacher or a staff member, the school entered on the file also fills *Assigned schools* when no
column carries them — an explicit operator statement, not a guessed default.

Residing in `SCHOOL1_` and being assigned to `SCHOOL1` remain two distinct facts: the file's school
states both, neither is deduced from the other.

## Cross-referencing rows

With `rows_per_person = many`, rows sharing an anchor are folded into one identity — whether they
come from one file or several.

- The fold key is the pair **(role, anchor)**: two populations never merge, even on an identical sAM.
- **Lists are concatenated, never deduplicated** — the engine owns that, as it owns trimming and
  validation.
- **Scalars keep the first non-empty value**; a later fragment carrying a different one emits a
  `log:` warning naming the anchor and the field. It does not fail: one divergent birth date must not
  sink an import of several thousand people. Residence is the exception: its conflicts are counted
  and reported in a single warning, since a batch of per-school exports that never states where
  people live would otherwise repeat it for every person.
- Anchors are compared trimmed and case-insensitively; the value emitted stays raw.

⚠ Memory grows with the folded population, not with file size — the connector holds an index until
the last source is read. Measured at ~20 MB for 20 000 identities.

## When it refuses

A refusal produces **no identity at all**, not a partial batch. Every one of them is a source
contradiction the operator must fix rather than a case the connector should guess through.

| Refusal | Why |
|---|---|
| A mapped column is **absent from a file** | it would silently read as empty, and an empty dimension **erases** it directory-side |
| The anchor cell is **empty on a row** | emitting the row unfolded would reintroduce the duplicate cross-referencing removes |
| The anchor target is **not mapped** | there is nothing to fold on |
| `target_role` missing on a source | a silent default would import teachers as students, and reconciliation is global per role |
| The file carries a **byte-order mark of another encoding** than the stated one — `'agents.csv' is not windows-1252: it carries a utf-8 byte-order mark.` | the mark says what the file is; letting it silently override the setting would make the setting a suggestion |
| The file **is not in the stated encoding** — `'agents.csv' is not valid utf-8: byte 0xEB at line 3 cannot be read.` | the default decoder would replace every such byte by U+FFFD without a word, and `Jo�l` would be written in the directory as a person's name |
| The file is **declared single-byte but is UTF-8** — `'agents.csv' is not windows-1252: its bytes are utf-8.` | every byte is valid in Windows-1252, so nothing would fail: `é` would silently become `Ã©` in 3 000 display names. A pure-ASCII file trips no guard — any setting reads it right. No refusal hints at which encoding to state: the option labels of the setting are the only help, on purpose |
| `encoding` is `custom` with **nothing entered** — `No encoding was entered. Write the name of the file's encoding, e.g. 'windows-1252'.` — or names an **unknown encoding** — `Unknown encoding 'nope'. Use an IANA name (e.g. 'windows-1252', 'iso-8859-15') or a code page number (e.g. 850).` | falling back on the default would read the file under an encoding the operator explicitly declined |
| **No date read in a column matches the stated format** — `Column '…' in '…': none of the N dates read matches the format '…'. First value read: '…'.` | a whole column the format cannot read is a wrong setting, not dirty data; reading it as empty would import nobody's birth date, reading it the other way round would write plausible wrong ones |
| `date_format` is `custom` with **nothing entered** — `No date format was entered. Write the format the dates are in, e.g. 'dd/MM/yyyy'.` | falling back on the default would read the file under a format the operator explicitly declined |
| A custom pattern of a **single character** — `Date format 'd' is a single-letter standard specifier, not a pattern. Write the full pattern, e.g. 'd/M/yyyy'.` | a one-character format string is a standard specifier: `d` means "the culture's short date", i.e. `MM/dd/yyyy` — the very day/month inversion this setting exists to prevent |

An unreadable **cell**, on the other hand, is not a refusal: that person is emitted without a birth
date, and a `null` erases nothing engine-side. The column-wide check runs on a sample read **before
the first identity is emitted**.

## Files

| File                            | Role                                                                                   |
|---------------------------------|----------------------------------------------------------------------------------------|
| `Program.cs`                    | Entry point — delegates to `ConnectorRunner.RunAsync()`.                                |
| `ConnectorRunner.cs`            | The protocol loop: reads the request on stdin, dispatches inspect/ingest, emits NDJSON. |
| `Encodings.cs`                  | The `encoding` setting: name resolution, strict encodings, the three byte guards. Pure logic, no dependency on the rest of the connector. |
| `DateFormats.cs`                | The `date_format` setting: pattern resolution and the column-wide mismatch check. Pure logic, no dependency on the rest of the connector. |
| `ConnectorJsonContext.cs` | Source-generated JSON context (AOT-safe serialization, no reflection).                |
| `manifest.json`                 | The manifest read by the engine's scanner (standard manifest fields). |
| `Properties/PublishProfiles/FolderProfile.pubxml` | Publish profile (output to `publish/csv/`).            |
| `config.example.json` | Example `config.json`: predefined mappings per role, and the autonomous section. |

## Protocol (stdin/stdout NDJSON)

The engine sends a single JSON request line on stdin (`{ "Mode": "inspect" | "ingest", ... }`). The
connector answers with newline-delimited JSON, each line carrying a prefix:

| Prefix | Meaning                                                       |
|--------|---------------------------------------------------------------|
| `idy:` | One `ThothRawIdentity` (emitted per row during ingest).        |
| `log:` | A log line (`{ "Level", "Message" }`) surfaced by the engine. |
| `res:` | The terminal result (`InspectionResult` or `IngestionResult`).|

The process exits `0` once the request is processed, a refusal included, and `1` when the request
itself cannot be handled (empty or unreadable request, unknown mode, crash) — after writing a
failure result line in every case.

The protocol records (`ConnectorRequest`, `ConnectorLogPayload`, `ConnectorLinePrefix`) are provided by
`Thoth.Integration.Abstractions` — the connector's only dependency. No duplication, no engine
reference.

## Build / publish

Publishing produces a self-contained native AOT binary, with the files the engine expects next to
it:

```
dotnet publish -c Release -p:PublishProfile=FolderProfile
```

The output lands in `publish/csv/` at the repository root: `Thoth.Connectors.Csv.exe`,
`manifest.json` and `config.example.json`. That folder is what goes into the engine's
`connectors-ext/csv/` (see *Install this connector*).

## Predefined mapping & autonomous import

Predefined mappings and the autonomous section live in a `config.json` placed next to the binary.
See `config.example.json`.

A predefined mapping per role pre-fills the column mapping screen; it is matched against the file's
headers **case-insensitively**, so a source writing `prenom` where the config says `Prenom` still
matches.

On the autonomous path each source declares its own url, and may declare its own school, its own
column mapping and its own reading settings — which is what lets one scheduled ingestion cross-
reference exports of different shapes.

Reading settings, `encoding` and `date_format` included, are layered: the config's global `parameters`, then the
ingestion's own `parameters`, then a source's. The nearest one wins whole — a source that states
`parameters` states them all. A run stating none lands on `utf-8` and `yyyy-MM-dd`, which is where nightly runs
differ from the interactive path: **the manifest is read by the frontend only**, so an autonomous
ingestion never sees its `DefaultValue` and falls back on the connector's own constant. The two are
kept equal for that reason.

⚠ An autonomous mapping is **frozen in the file** and replayed nightly against sources nobody
controls. The day one of them renames a column, the ingestion is **refused** rather than importing
that dimension as empty — see *When it refuses*. A column simply **omitted** from the mapping is a
different matter: with no key to confront, it carries no value — a list dimension (groups,
schools, assignments) is then cleared in the directory, a single value (description, birth date)
is left as it is.

Sources can be local (`file://`, including UNC shares) or remote (`http(s)://`, with optional
authentication headers). Schemes, header formats and the handling of the secret they carry are
covered in the [Thoth documentation](https://thoth-iam.com/docs/en/connectors/#local-or-remote-sources).

## Install this connector

Each version is published as a [release](https://github.com/BaguetteByte-Innovations/thoth-connector-csv/releases/latest):
a zip of the connector folder, to extract into `connectors-ext/csv/`, with its `SHA256SUMS.txt`
and a provenance attestation (`gh attestation verify <zip> --repo BaguetteByte-Innovations/thoth-connector-csv`).

See the **[Thoth documentation](https://thoth-iam.com/docs/en/connectors/#installation)**.

## Reference

- **[Developer documentation](https://thoth-iam.com/docs/en/develop/)** — how a Thoth connector works: the exchange with the engine, `manifest.json`, `config.json`, the identities it emits.
- **[`Thoth.Integration.Abstractions`](https://www.nuget.org/packages/Thoth.Integration.Abstractions)** — the shared contract (NuGet package) this executable references.
