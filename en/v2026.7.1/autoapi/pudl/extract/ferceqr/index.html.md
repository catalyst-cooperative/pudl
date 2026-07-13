# pudl.extract.ferceqr

Extract FERC EQR data.

## Attributes

| [`logger`](#pudl.extract.ferceqr.logger)   |    |
|--------------------------------------------|----|

## Functions

| [`_get_csv`](#pudl.extract.ferceqr._get_csv)(→ zipfile.ZipFile)                                        | Download CSV to a tempmorary directory to avoid reading into memory.             |
|--------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`_clean_csv_name`](#pudl.extract.ferceqr._clean_csv_name)(→ pathlib.Path)                             | Standardize zip file names to avoid errors when opening.                         |
| [`_get_table_name`](#pudl.extract.ferceqr._get_table_name)(→ str)                                      |                                                                                  |
| [`_extract_ident`](#pudl.extract.ferceqr._extract_ident)(→ str)                                        | Extract data from ident csv, write to parquet, and return CID from table.        |
| [`_extract_other_table`](#pudl.extract.ferceqr._extract_other_table)(table_type, csv_path, ...)        | Extract data from a table other than ident and add year_quarter and CID columns. |
| [`_csvs_to_parquet`](#pudl.extract.ferceqr._csvs_to_parquet)(csv_path, year_quarter, filing_name, ...) | Mirror CSVs in filing to a parquet file.                                         |
| [`_save_extract_errors`](#pudl.extract.ferceqr._save_extract_errors)(year_quarter, duckdb_connection)  | Create parquet file with metadata on any CSV parsing errors.                     |
| [`extract_ferceqr`](#pudl.extract.ferceqr.extract_ferceqr)() → tuple[pudl.helpers.ParquetData, ...)    | Extract year quarter from CSVs and load to parquet files.                        |

## Module Contents

### pudl.extract.ferceqr.logger

### pudl.extract.ferceqr.\_get_csv(base_path: upath.UPath, year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [zipfile.ZipFile](https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile)

Download CSV to a tempmorary directory to avoid reading into memory.

### pudl.extract.ferceqr.\_clean_csv_name(csv_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Standardize zip file names to avoid errors when opening.

### pudl.extract.ferceqr.\_get_table_name(table_type: [str](https://docs.python.org/3/library/stdtypes.html#str), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

### pudl.extract.ferceqr.\_extract_ident(ident_csv: [str](https://docs.python.org/3/library/stdtypes.html#str), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Extract data from ident csv, write to parquet, and return CID from table.

This table is always extracted first so we can pull the CID from it and include
a CID column in all other tables.

### pudl.extract.ferceqr.\_extract_other_table(table_type: [str](https://docs.python.org/3/library/stdtypes.html#str), csv_path: [str](https://docs.python.org/3/library/stdtypes.html#str), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), cid: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Extract data from a table other than ident and add year_quarter and CID columns.

### pudl.extract.ferceqr.\_csvs_to_parquet(csv_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), filing_name: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Mirror CSVs in filing to a parquet file.

Each filing contains a CSV for 4 EQR tables. These will each be extracted
to a separate parquet file.

### pudl.extract.ferceqr.\_save_extract_errors(year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), duckdb_connection: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Create parquet file with metadata on any CSV parsing errors.

### pudl.extract.ferceqr.extract_ferceqr(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), ferceqr_archive: [pudl.dagster.resources.FercEqrArchiveResource](../../dagster/resources/index.md#pudl.dagster.resources.FercEqrArchiveResource) = FercEqrArchiveResource()) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)]

Extract year quarter from CSVs and load to parquet files.

This method will loop through the nested EQR archive zipfiles and extract all tables
from them, and write to parquet. It opens a duckdb connection at the top level to
keep track of extraction errors, so we can write these to the `raw_ferceqr__extract_errors`
table.
