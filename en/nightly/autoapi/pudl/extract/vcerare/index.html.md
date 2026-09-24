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

| [`logger`](#pudl.extract.vcerare.logger)                           |    |
|-----------------------------------------------------------------------------------|----|
| [`VCERARE_PAGES`](#pudl.extract.vcerare.VCERARE_PAGES)                    |    |
| [`DATETIME_HOUR_OF_YEAR_START_YEAR`](#pudl.extract.vcerare.DATETIME_HOUR_OF_YEAR_START_YEAR) |    |

## Functions

| [`_clean_column_name`](#pudl.extract.vcerare._clean_column_name)(→ str)                     | Match the raw VCE RARE column naming convention to PUDL's snake_case.   |
|------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`_vcerare_column_types`](#pudl.extract.vcerare._vcerare_column_types)(...)                    | Build a DuckDB `columns` schema from a raw VCE RARE CSV header row.     |
| [`extract_vcerare`](#pudl.extract.vcerare.extract_vcerare)(→ tuple[dict[int, ...)        | Extract data from all vcerare pages and write to parquet files.         |
| [`raw_vcerare__lat_lon_fips`](#pudl.extract.vcerare.raw_vcerare__lat_lon_fips)(→ pandas.DataFrame) | Extract lat/lon to FIPS and county mapping CSV.                         |

## Module Contents

### pudl.extract.vcerare.logger

### pudl.extract.vcerare.VCERARE_PAGES

### pudl.extract.vcerare.DATETIME_HOUR_OF_YEAR_START_YEAR *= 2024*

### pudl.extract.vcerare.\_clean_column_name(col: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Match the raw VCE RARE column naming convention to PUDL’s snake_case.

### pudl.extract.vcerare.\_vcerare_column_types(year: [int](https://docs.python.org/3/builtins/functions.html#int)) → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]], [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]]

Build a DuckDB `columns` schema from a raw VCE RARE CSV header row.

Every column is a per-county/subregion capacity factor (`DOUBLE`) except the
first, unnamed column, which is always `hour_of_year` and whose type depends
on the report year (see `DATETIME_HOUR_OF_YEAR_START_YEAR`). The set of
county/subregion columns is not stable across vintages, so it’s always derived
from the actual header row rather than hardcoded.

### pudl.extract.vcerare.extract_vcerare(context) → [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[int](https://docs.python.org/3/builtins/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[int](https://docs.python.org/3/builtins/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[int](https://docs.python.org/3/builtins/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)]]

Extract data from all vcerare pages and write to parquet files.

### pudl.extract.vcerare.raw_vcerare_\_lat_lon_fips(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Extract lat/lon to FIPS and county mapping CSV.

This dataframe is static, so it has a distinct partition from the other datasets and
its extraction is controlled by a boolean in the ETL run.
