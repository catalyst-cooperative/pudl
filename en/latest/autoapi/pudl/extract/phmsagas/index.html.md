# pudl.extract.phmsagas

Retrieves data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA’s published Excel spreadsheets.

## Attributes

| [`logger`](#pudl.extract.phmsagas.logger)                               |    |
|-------------------------------------------------------------------------|----|
| [`raw_phmsagas__all_dfs`](#pudl.extract.phmsagas.raw_phmsagas__all_dfs) |    |

## Classes

| [`Extractor`](#pudl.extract.phmsagas.Extractor)   | Extractor for the excel dataset PHMSA.   |
|---------------------------------------------------|------------------------------------------|

## Functions

| [`extract_phmsagas`](#pudl.extract.phmsagas.extract_phmsagas)(context, raw_phmsagas_\_all_dfs)   | Extract raw PHMSA gas data from excel sheets into dataframes.   |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|

## Module Contents

### pudl.extract.phmsagas.logger

### *class* pudl.extract.phmsagas.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.excel.ExcelExtractor`](../excel/index.md#pudl.extract.excel.ExcelExtractor)

Extractor for the excel dataset PHMSA.

#### METADATA

Instance of metadata object to use with this extractor.

#### cols_added *= []*

#### load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Run same load_source and then replace all periods w/ n’s.

There are a ton of identical column names in the raw dataset for 1984.
Typically these get processed in load_source via pd.read_excel which adds
a period and then an auto-incremented number as a suffix. Then this
data gets run through [`pudl.helpers.simplify_columns()`](../../helpers/index.md#pudl.helpers.simplify_columns) which converts
all non-alphanumeric (aka periods) into spaces and then condenses any multiple
spaces into one space. This would all be fine and good except for the fact that
there are 22 column names that are identical expect for trailing spaces in the
raw source. These trailing spaces effectively get removed in
[`pudl.helpers.simplify_columns()`](../../helpers/index.md#pudl.helpers.simplify_columns) and then we have duplicate column names.
This method runs the parent adds \_n#’s to these trailing space column names.

#### process_renamed(newdata: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Drop columns that get mapped to other assets and columns with unstructured data.

Old-ish years (1990-2009) of PHMSA data have one Excel tab in the raw data, while
newer data has multiple tabs. To extract data into tables that follow the newer
data format without duplicating the older data, we need to split older pages into
multiple tables by column. To prevent each table from containing all columns from
these older years, filter by the list of columns specified for the page, with a
warning.

The oldest years (before 1990) contain multiple years in one tab. The records
contain a report_year column but some of them are reported at a two digit year
(ex: 87 for 1987). We convert these into four digit years.

### pudl.extract.phmsagas.raw_phmsagas_\_all_dfs

### pudl.extract.phmsagas.extract_phmsagas(context, raw_phmsagas_\_all_dfs)

Extract raw PHMSA gas data from excel sheets into dataframes.
