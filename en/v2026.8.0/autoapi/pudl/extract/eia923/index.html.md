# pudl.extract.eia923

Retrieves data from EIA Form 923 spreadsheets for analysis.

This module pulls data from archived copies of EIA’s published Excel spreadsheets.

## Attributes

| [`logger`](#pudl.extract.eia923.logger)                           |    |
|-------------------------------------------------------------------|----|
| [`raw_eia923__all_dfs`](#pudl.extract.eia923.raw_eia923__all_dfs) |    |

## Classes

| [`Extractor`](#pudl.extract.eia923.Extractor)   | Extractor for EIA form 923.   |
|-------------------------------------------------|-------------------------------|

## Functions

| [`extract_eia923`](#pudl.extract.eia923.extract_eia923)(context, raw_eia923_\_all_dfs)   | Extract raw EIA-923 data from excel sheets into dataframes.   |
|------------------------------------------------------------------------------------------|---------------------------------------------------------------|

## Module Contents

### pudl.extract.eia923.logger

### *class* pudl.extract.eia923.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for EIA form 923.

#### METADATA

Instance of metadata object to use with this extractor.

#### BLACKLISTED_PAGES *= ['plant_frame']*

List of supported pages that should not be extracted.

#### cols_added *= []*

#### process_raw(df, page, \*\*partition)

Prepare raw table for extraction.

Check extraction configuration is sensible, drop reserved columns, switch to
standardized column names, and perform other broadly-applicable cleanup of
data formats, types, and missingness.

#### *static* process_renamed(df, page, \*\*partition)

Cleans up unnamed_0 column in stocks page, drops invalid plan_id_eia rows.

#### process_final_page(df, page)

Removes reserved columns from the final dataframe.

#### *static* get_dtypes(page, \*\*partition)

Returns dtypes for plant id columns and county FIPS column.

### pudl.extract.eia923.raw_eia923_\_all_dfs

### pudl.extract.eia923.extract_eia923(context, raw_eia923_\_all_dfs)

Extract raw EIA-923 data from excel sheets into dataframes.
