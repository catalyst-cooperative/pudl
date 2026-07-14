# pudl.extract.vcerare

Extract VCE Resource Adequacy Renewable Energy (RARE) Power Dataset.

This dataset has 1,000s of columns, so we don’t want to manually specify a rename on
import because we’ll pivot these to a column in the transform step. We adapt the
standard extraction infrastructure to simply read in the data.

Each annual zip folder contains a folder with three files:
Wind_Power_140m_Offshore_county.csv
Wind_Power_100m_Onshore_county.csv
Fixed_SolarPV_Lat_UPV_county.csv

The drive also contains one more CSV file: vce_county_lat_long_fips_table.csv. This gets
read in when the fips partition is set to True.

## Attributes

| [`logger`](#pudl.extract.vcerare.logger)               |    |
|--------------------------------------------------------|----|
| [`VCERARE_PAGES`](#pudl.extract.vcerare.VCERARE_PAGES) |    |

## Functions

| [`_clean_column_names`](#pudl.extract.vcerare._clean_column_names)(→ duckdb.DuckDBPyRelation)      | Apply basic cleaning to column names.                           |
|----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`extract_vcerare`](#pudl.extract.vcerare.extract_vcerare)(→ tuple[dict[int, ...)                  | Extract data from all vcerare pages and write to parquet files. |
| [`raw_vcerare__lat_lon_fips`](#pudl.extract.vcerare.raw_vcerare__lat_lon_fips)(→ pandas.DataFrame) | Extract lat/lon to FIPS and county mapping CSV.                 |

## Module Contents

### pudl.extract.vcerare.logger

### pudl.extract.vcerare.VCERARE_PAGES

### pudl.extract.vcerare.\_clean_column_names(table_relation: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation)) → [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation)

Apply basic cleaning to column names.

### pudl.extract.vcerare.extract_vcerare(context) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)]]

Extract data from all vcerare pages and write to parquet files.

### pudl.extract.vcerare.raw_vcerare_\_lat_lon_fips(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Extract lat/lon to FIPS and county mapping CSV.

This dataframe is static, so it has a distinct partition from the other datasets and
its extraction is controlled by a boolean in the ETL run.
