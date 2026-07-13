# pudl.extract.eia860m

Retrieve data from EIA Form 860M spreadsheets for analysis.

This modules pulls data from EIA’s published Excel spreadsheets.

This code is for use analyzing EIA Form 860M data. EIA 860M is only used in
conjunction with EIA 860. This module both extracts EIA 860M and appends
the extracted EIA 860M dataframes to the extracted EIA 860 dataframes. Example
setup with pre-generated eia860_raw_dfs and datastore as ds:

eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
: Eia860DataConfig.eia860m_date)

eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
: eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

## Attributes

| [`logger`](#pudl.extract.eia860m.logger)   |    |
|--------------------------------------------|----|

## Classes

| [`Extractor`](#pudl.extract.eia860m.Extractor)   | Extractor for the excel dataset EIA860M.   |
|--------------------------------------------------|--------------------------------------------|

## Functions

| [`append_eia860m`](#pudl.extract.eia860m.append_eia860m)(→ dict[str, pandas.DataFrame])    | Append EIA 860M to EIA860 data.                                      |
|--------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| [`raw_eia860m__all_dfs`](#pudl.extract.eia860m.raw_eia860m__all_dfs)(context)              | Extract raw EIA 860M data from excel sheets into dict of dataframes. |
| [`extract_eia860m`](#pudl.extract.eia860m.extract_eia860m)(context, raw_eia860m_\_all_dfs) | Extract raw EIA data from excel sheets into dataframes.              |

## Module Contents

### pudl.extract.eia860m.logger

### *class* pudl.extract.eia860m.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for the excel dataset EIA860M.

#### METADATA

Instance of metadata object to use with this extractor.

#### cols_added *= []*

#### process_raw(df, page, \*\*partition)

Adds source column and report_year column if missing.

#### *static* get_dtypes(page, \*\*partition)

Returns dtypes for plant id columns.

### pudl.extract.eia860m.append_eia860m(eia860_raw_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], eia860m_raw_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Append EIA 860M to EIA860 data.

* **Parameters:**
  * **eia860_raw_dfs** – EIA 860 raw tables. Result of
    `pudl.extract.eia860.Extractor.extract()`
  * **eia860m_raw_dfs** – EIA 860M raw tables. Result of `Extractor.extract()`
* **Returns:**
  Augmented version of eia860_raw_dfs. Each raw page stored in eia860m_raw_dfs
  appended to its eia860_raw_dfs counterpart.

### pudl.extract.eia860m.raw_eia860m_\_all_dfs(context)

Extract raw EIA 860M data from excel sheets into dict of dataframes.

### pudl.extract.eia860m.extract_eia860m(context, raw_eia860m_\_all_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)])

Extract raw EIA data from excel sheets into dataframes.
