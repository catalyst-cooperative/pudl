# pudl.extract.eia861

Retrieve data from EIA Form 861 spreadsheets for analysis.

This modules pulls data from EIA’s published Excel spreadsheets.

This code is for use analyzing EIA Form 861 data.

## Attributes

| [`logger`](#pudl.extract.eia861.logger)                           |    |
|-------------------------------------------------------------------|----|
| [`raw_eia861__all_dfs`](#pudl.extract.eia861.raw_eia861__all_dfs) |    |

## Classes

| [`Extractor`](#pudl.extract.eia861.Extractor)   | Extractor for the excel dataset EIA861.   |
|-------------------------------------------------|-------------------------------------------|

## Functions

| [`extract_eia861`](#pudl.extract.eia861.extract_eia861)(context, raw_eia861_\_all_dfs)   | Extract raw EIA-861 data from Excel sheets into dataframes.   |
|------------------------------------------------------------------------------------------|---------------------------------------------------------------|

## Module Contents

### pudl.extract.eia861.logger

### *class* pudl.extract.eia861.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for the excel dataset EIA861.

#### METADATA

Instance of metadata object to use with this extractor.

#### cols_added *= []*

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Rename columns with location.

#### *static* process_renamed(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Adds report_year column if missing.

#### *static* get_dtypes(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Returns dtypes for plant id columns.

### pudl.extract.eia861.raw_eia861_\_all_dfs

### pudl.extract.eia861.extract_eia861(context, raw_eia861_\_all_dfs)

Extract raw EIA-861 data from Excel sheets into dataframes.
