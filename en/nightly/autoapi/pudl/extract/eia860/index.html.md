# pudl.extract.eia860

Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA’s published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.

## Attributes

| [`logger`](#pudl.extract.eia860.logger)                                 |    |
|-------------------------------------------------------------------------|----|
| [`RAW_EIA860_TABLE_NAMES`](#pudl.extract.eia860.RAW_EIA860_TABLE_NAMES) |    |
| [`raw_eia860__all_dfs`](#pudl.extract.eia860.raw_eia860__all_dfs)       |    |

## Classes

| [`Extractor`](#pudl.extract.eia860.Extractor)   | Extractor for the excel dataset EIA860.   |
|-------------------------------------------------|-------------------------------------------|

## Functions

| [`extract_eia860`](#pudl.extract.eia860.extract_eia860)(context, raw_eia860_\_all_dfs)   | Extract raw EIA data from excel sheets into dataframes.   |
|------------------------------------------------------------------------------------------|-----------------------------------------------------------|

## Module Contents

### pudl.extract.eia860.logger

### *class* pudl.extract.eia860.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for the excel dataset EIA860.

#### METADATA

Instance of metadata object to use with this extractor.

#### cols_added *= []*

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Apply necessary pre-processing to the dataframe.

* Rename columns based on our compiled spreadsheet metadata
* Add report_year if it is missing
* Add a flag indicating if record came from EIA 860, or EIA 860M
* Fix any generator_id values with leading zeroes.

#### *static* get_dtypes(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Returns dtypes for plant id columns.

### pudl.extract.eia860.RAW_EIA860_TABLE_NAMES

### pudl.extract.eia860.raw_eia860_\_all_dfs

### pudl.extract.eia860.extract_eia860(context, raw_eia860_\_all_dfs)

Extract raw EIA data from excel sheets into dataframes.

* **Parameters:**
  **context** – dagster keyword that provides access to resources and config.
* **Returns:**
  A tuple of extracted EIA dataframes.
