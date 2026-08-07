# pudl.extract.eia930

Extract EIA Form 930 data from CSVs.

EIA Form 930 is reported in half-year increments. Each half-year has three
separate pages, which are stored as separate CSVs:
“balance”, “interchange”, and “subregion.” See
[https://docs.catalyst.coop/pudl/en/latest/data_sources/eia930.html](https://docs.catalyst.coop/pudl/en/latest/data_sources/eia930.html) for
more information.

We extract these CSVs into DuckDB, rename the columns as per the column map, and
dump out the concatenated pages to Parquet.

## Attributes

| [`logger`](#pudl.extract.eia930.logger)   |    |
|-------------------------------------------|----|

## Functions

| [`raw_eia930__balance`](#pudl.extract.eia930.raw_eia930__balance)(→ pudl.helpers.ParquetData)         | Raw balance page.                                                |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| [`raw_eia930__interchange`](#pudl.extract.eia930.raw_eia930__interchange)(→ pudl.helpers.ParquetData) | Raw interchange page.                                            |
| [`raw_eia930__subregion`](#pudl.extract.eia930.raw_eia930__subregion)(→ pudl.helpers.ParquetData)     | Raw subregion page - only exists after 2018h2.                   |
| [`extract_page`](#pudl.extract.eia930.extract_page)(→ pudl.helpers.ParquetData)                       | Pull data for a page across many half-years into a Parquet file. |
| [`extract_half_year_page`](#pudl.extract.eia930.extract_half_year_page)(→ str)                        | Extract data from a single CSV.                                  |

## Module Contents

### pudl.extract.eia930.logger

### pudl.extract.eia930.raw_eia930_\_balance(context) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Raw balance page.

### pudl.extract.eia930.raw_eia930_\_interchange(context) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Raw interchange page.

### pudl.extract.eia930.raw_eia930_\_subregion(context) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Raw subregion page - only exists after 2018h2.

### pudl.extract.eia930.extract_page(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), page: [str](https://docs.python.org/3/library/stdtypes.html#str), half_years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Pull data for a page across many half-years into a Parquet file.

This involves reading each half-year, of course, but also concatenating them
together and expanding the schema to fit all the columns we see.

* **Parameters:**
  * **datastore** – the Datastore we use to actually access the raw data.
  * **page** – the name of the page we’re extracting.
  * **half_years** – the set of half-year segments we’re extracting.
* **Returns:**
  ParquetData pointing to parquet file with raw table data.

### pudl.extract.eia930.extract_half_year_page(con: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection), datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), half_year: [str](https://docs.python.org/3/library/stdtypes.html#str), page: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Extract data from a single CSV.

Reads into DuckDB for speed and memory use. To avoid reading the whole CSV
into memory, we’re extracting directly to a temporary directory.

* **Parameters:**
  * **con** – DuckDB connection.
  * **datastore** – the Datastore we use to actually access the input data.
  * **half_year** – the half-year we’re reading in.
  * **page** – the name of the page we’re reading.
* **Returns:**
  name of DuckDB view that represents the read & renamed CSV.
* **Return type:**
  view_name
