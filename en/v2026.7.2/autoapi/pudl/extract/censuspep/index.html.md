# pudl.extract.censuspep

Retrieve data from Census PEP spreadsheets.

## Attributes

| [`logger`](#pudl.extract.censuspep.logger)                                 |    |
|----------------------------------------------------------------------------|----|
| [`raw_censuspep__all_dfs`](#pudl.extract.censuspep.raw_censuspep__all_dfs) |    |

## Classes

| [`Extractor`](#pudl.extract.censuspep.Extractor)   | Extractor for the excel dataset Census PEP FIPS Codes.   |
|----------------------------------------------------|----------------------------------------------------------|

## Functions

| [`raw_censuspep__geocodes`](#pudl.extract.censuspep.raw_censuspep__geocodes)(raw_censuspep_\_all_dfs)   | Extract raw Census PEP FIPS codes data into dataframes.   |
|---------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|

## Module Contents

### pudl.extract.censuspep.logger

### *class* pudl.extract.censuspep.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for the excel dataset Census PEP FIPS Codes.

#### METADATA

Instance of metadata object to use with this extractor.

#### cols_added *= []*

#### *static* get_dtypes(page, \*\*partition)

Returns nullable string dtypes for all columns.

FIPS codes often have leading zeros. This preserves them.

#### process_raw(df, page, \*\*partition)

Apply necessary pre-processing to the dataframe.

### pudl.extract.censuspep.raw_censuspep_\_all_dfs

### pudl.extract.censuspep.raw_censuspep_\_geocodes(raw_censuspep_\_all_dfs)

Extract raw Census PEP FIPS codes data into dataframes.

* **Returns:**
  An extracted Census PEP FIPS codes dataframe.
