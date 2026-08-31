# pudl.extract.ferceqr

Extract FERC EQR data.

## Attributes

| [`logger`](#pudl.extract.ferceqr.logger)                       |                                                                                                                      |
|-------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| [`_PROGRESS_LOG_INTERVAL`](#pudl.extract.ferceqr._PROGRESS_LOG_INTERVAL)       | How often (in filings) to log extraction progress for a quarter.                                                     |
| [`FercEqrTableType`](#pudl.extract.ferceqr.FercEqrTableType)             | The four raw table types present in each FERC EQR filing.                                                            |
| [`_ALL_TABLE_TYPES`](#pudl.extract.ferceqr._ALL_TABLE_TYPES)             | Canonical list of all [`FercEqrTableType`](#pudl.extract.ferceqr.FercEqrTableType) values, in extraction order. |
| [`_UNSAFE_CSV_NAME_CHARS_REGEX`](#pudl.extract.ferceqr._UNSAFE_CSV_NAME_CHARS_REGEX) | Characters stripped from an extracted CSV's filename before duckdb reads it.                                         |

## Functions

| [`_get_csv`](#pudl.extract.ferceqr._get_csv)(→ collections.abc.Generator[zipfile.ZipFile])   | Download CSV to a tempmorary directory to avoid reading into memory.             |
|-----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`_clean_csv_name`](#pudl.extract.ferceqr._clean_csv_name)(→ pathlib.Path)                          | Strip characters from an extracted CSV's filename that would confuse duckdb.     |
| [`_get_table_name`](#pudl.extract.ferceqr._get_table_name)(→ str)                                   |                                                                                  |
| [`_clear_raw_table_partition`](#pudl.extract.ferceqr._clear_raw_table_partition)(→ None)                       | Delete any existing per-filing parquet output for one raw table+quarter.         |
| [`_extract_ident`](#pudl.extract.ferceqr._extract_ident)(→ str | None)                             | Extract data from ident csv, write to parquet, and return CID from table.        |
| [`_extract_other_table`](#pudl.extract.ferceqr._extract_other_table)(table_type, csv_path, ...)          | Extract data from a table other than ident and add year_quarter and CID columns. |
| [`_resolve_cid`](#pudl.extract.ferceqr._resolve_cid)(→ str | None)                               | Extract one filing's ident table and return its CID, or `None`.                  |
| [`_csvs_to_parquet`](#pudl.extract.ferceqr._csvs_to_parquet)(→ frozenset[FercEqrTableType])          | Mirror CSVs in filing to a parquet file.                                         |
| [`_get_rejected_record_counts`](#pudl.extract.ferceqr._get_rejected_record_counts)(→ dict[str, int])            | Count rejected CSV records by DuckDB's reason for rejecting them.                |
| [`_save_extract_errors`](#pudl.extract.ferceqr._save_extract_errors)(→ None)                             | Persist DuckDB's CSV parsing errors for the quarter to parquet.                  |
| [`extract_ferceqr`](#pudl.extract.ferceqr.extract_ferceqr)(context[, ferceqr_archive])              | Extract year quarter from CSVs and load to parquet files.                        |

## Module Contents

### pudl.extract.ferceqr.logger

### pudl.extract.ferceqr.\_PROGRESS_LOG_INTERVAL *= 500*

How often (in filings) to log extraction progress for a quarter.

Some quarters contain thousands of filings, each processed one at a time; without
this, a long-running extraction has no visible sign of progress in the logs.

### pudl.extract.ferceqr.FercEqrTableType

The four raw table types present in each FERC EQR filing.

### pudl.extract.ferceqr.\_ALL_TABLE_TYPES *: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[FercEqrTableType](#pudl.extract.ferceqr.FercEqrTableType), ...]*

Canonical list of all [`FercEqrTableType`](#pudl.extract.ferceqr.FercEqrTableType) values, in extraction order.

`ident` is extracted first so its CID can be attached to the other tables; see
[`_extract_ident()`](#pudl.extract.ferceqr._extract_ident).

### pudl.extract.ferceqr.\_get_csv(base_path: upath.UPath, year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [collections.abc.Generator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Generator)[[zipfile.ZipFile](https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile)]

Download CSV to a tempmorary directory to avoid reading into memory.

### pudl.extract.ferceqr.\_UNSAFE_CSV_NAME_CHARS_REGEX *: [re.Pattern](https://docs.python.org/3/library/re.html#re.Pattern)*

Characters stripped from an extracted CSV’s filename before duckdb reads it.

`*`, `?`, `[`, and `]` are glob metacharacters that duckdb’s CSV reader
interprets even when passed a single literal path (not a SQL string): a file literally
named `[ab].csv` gets silently read as `a.csv` instead, if that file happens to
exist alongside it – no error, just wrong data. Quote characters are stripped
defensively too, a long-standing precaution predating this docstring; the specific
failure it was guarding against isn’t reproducible against the current duckdb version,
but real filings do contain apostrophes (e.g. “Citizens’ Electric”) so there’s no reason
to stop.

As of 2026-08-03 none of the known to be unsafe characters appeared in the most recent
batch of FERC EQR filenames in any of the 52 quarters (2013q3-2026q2, ~146k
filings/~533k CSVs). Real filenames do contain parentheses, `#`, and occasional
encoding-mangled accented letters, none of which are unsafe here and are left alone.
Stripping this set costs nothing and closes a silent-wrong-data failure mode before it
has a chance to occur.

### pudl.extract.ferceqr.\_clean_csv_name(csv_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Strip characters from an extracted CSV’s filename that would confuse duckdb.

### pudl.extract.ferceqr.\_get_table_name(table_type: [FercEqrTableType](#pudl.extract.ferceqr.FercEqrTableType), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

### pudl.extract.ferceqr.\_clear_raw_table_partition(table_type: [FercEqrTableType](#pudl.extract.ferceqr.FercEqrTableType), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Delete any existing per-filing parquet output for one raw table+quarter.

Each filing’s output file is named after that filing’s own ID. Filing IDs change
with every revision or resubmission. This means if we don’t clear the extracted
parquet output for a quarter before re-extracting it, we can end up with multiple
duplicate filings for the same company and quarter. This is unlikely to be an
issue in the production builds, but is a problem for local development and
testing.

### pudl.extract.ferceqr.\_extract_ident(ident_csv: [str](https://docs.python.org/3/library/stdtypes.html#str), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Extract data from ident csv, write to parquet, and return CID from table.

This table is always extracted first so we can pull the CID from it and include
a CID column in all other tables.

* **Returns:**
  The company identifier (CID) read from the ident table’s first row, or
  `None` if the CSV parsed but contained no rows – in which case no
  `raw_ferceqr__ident` parquet is written for this filing. A CSV that fails
  to parse at all instead raises `duckdb.Error`, left to the caller.

### pudl.extract.ferceqr.\_extract_other_table(table_type: [FercEqrTableType](#pudl.extract.ferceqr.FercEqrTableType), csv_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), cid: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Extract data from a table other than ident and add year_quarter and CID columns.

`cid` is None when the filing has no usable identity CSV (missing entirely,
or present but unpareable) – the table is still worth extracting, just
without a company_identifier to attach, so a real SQL NULL is used rather
than the literal string `"None"`.

### pudl.extract.ferceqr.\_resolve_cid(ident_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Extract one filing’s ident table and return its CID, or `None`.

Warns (but does not raise) if the identity CSV fails to parse entirely or
parses with no rows – either way the rest of the filing is still worth
extracting, just with a null company_identifier.

### pudl.extract.ferceqr.\_csvs_to_parquet(csv_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [frozenset](https://docs.python.org/3/library/stdtypes.html#frozenset)[[FercEqrTableType](#pudl.extract.ferceqr.FercEqrTableType)]

Mirror CSVs in filing to a parquet file.

Each filing is expected to contain a CSV for each of 4 EQR tables, extracted
to a separate parquet file. Real filings are sometimes incomplete or
malformed in ways that have been observed in practice, and those cases are
not fatal to the rest of the filing. A missing contracts, transactions, or
indexPub CSV is routine – many thousands of filings in a given quarter
simply have no data of that type – so it’s counted in the return value
rather than logged; logging a warning per filing for something this common
would flood the logs without conveying anything useful. A missing, empty,
or unparsable identity CSV, by contrast, is rare and more consequential (it
means no company_identifier (CID) can be attached to the other tables), so
it’s still logged individually; the other tables are still extracted, with
a null CID rather than being dropped entirely. A zip archive that fails to
parse due to corruption and unrecognized CSVs are also logged with a
warning per filing since it is relatively rare.

More than one CSV matching the same table type has never been observed in
the wild and has no established handling strategy, so it raises rather
than silently guessing (e.g. by using the first match) – a warning here
would be easy to miss among the routine ones above.

Records rejected by DuckDB due to too many or too few columns, invalid
UTF-8 encoding, or other reasons are not fatal. The bad records are
loaded into their own parquet files for later inspection.

* **Returns:**
  The subset of `_ALL_TABLE_TYPES` whose CSV was present in this
  filing, regardless of whether it was successfully parsed. Used by the
  caller to tally how many filings in the quarter included each table.

### pudl.extract.ferceqr.\_get_rejected_record_counts(duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]

Count rejected CSV records by DuckDB’s reason for rejecting them.

`reject_errors.error_type` is a DuckDB-defined enum with one row per
rejected record; as of DuckDB 1.5 its possible values are `CAST`,
`MISSING COLUMNS`, `TOO MANY COLUMNS`, `UNQUOTED VALUE`, `LINE SIZE
OVER MAXIMUM`, `INVALID ENCODING`, and `INVALID STATE`. In FERC EQR
filings the two seen in practice are invalid UTF-8 encoding and a wrong
column count from unescaped quotes within a field.

* **Returns:**
  A dict mapping each `error_type` string observed among rejected records
  (e.g. `"INVALID ENCODING"`) to the count of records rejected for that
  reason, e.g. `{"INVALID ENCODING": 12, "MISSING COLUMNS": 4}`. Error
  types with no rejected records are simply absent from the dict.

### pudl.extract.ferceqr.\_save_extract_errors(year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [None](https://docs.python.org/3/library/constants.html#None)

Persist DuckDB’s CSV parsing errors for the quarter to parquet.

Joins DuckDB’s `reject_errors` table (one row per rejected CSV record) against
`reject_scans` (one row per CSV file scanned) to attach the source filename to
each rejected record, then writes the result to the
`raw_ferceqr__extract_errors` table. `extract_ferceqr` builds its own
[`ParquetData`](../../helpers/index.html.md#pudl.helpers.ParquetData) pointing at this same table/quarter after
calling this function, the same way it does for the other four raw tables.

### pudl.extract.ferceqr.extract_ferceqr(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), ferceqr_archive: [pudl.dagster.resources.FercEqrArchiveResource](../../dagster/resources/index.html.md#pudl.dagster.resources.FercEqrArchiveResource) = FercEqrArchiveResource())

Extract year quarter from CSVs and load to parquet files.

This method will loop through the nested EQR archive zipfiles and extract all tables
from them, and write to parquet. It opens a duckdb connection at the top level to
keep track of extraction errors, so we can write these to the `raw_ferceqr__extract_errors`
table. Summary statistics about the extraction (filing counts, corrupt archives,
and unextractable records by reason) are logged and attached as Dagster metadata
on the `raw_ferceqr__extract_errors` output.
