# Thoth.Connectors.Csv

The CSV connector of [Thoth](https://thoth-iam.com/), an identity management suite for schools: a
standalone native executable that the Thoth engine runs to import students, teachers and staff from
CSV files. It ships with Thoth; this repository holds its source.

## Documentation

- **[Using the connector](https://thoth-iam.com/docs/en/connectors-csv/)** — the expected file, the
  settings, residence and assignments, cross-referencing, refusals, `config.json`.
- **[Installing a connector](https://thoth-iam.com/docs/en/connectors/#installation)**.
- **[Writing a connector](https://thoth-iam.com/docs/en/develop/)** — this repository is also the
  reference for a native connector: its only dependency is the contract package
  [`Thoth.Integration.Abstractions`](https://www.nuget.org/packages/Thoth.Integration.Abstractions).

A PowerShell flavor with the same behavior is published separately:
[thoth-connector-csv-ps](https://github.com/BaguetteByte-Innovations/thoth-connector-csv-ps).

## Download

Each version is published as a [release](https://github.com/BaguetteByte-Innovations/thoth-connector-csv/releases/latest):
a zip to extract into `connectors-ext/csv/`, its `SHA256SUMS.txt` and a provenance attestation.

## Build

```
dotnet publish -c Release -p:PublishProfile=FolderProfile
```

The output lands in `publish/csv/`: `Thoth.Connectors.Csv.exe`, `manifest.json` and
`config.example.json`, the content of a `connectors-ext/csv/` folder.
