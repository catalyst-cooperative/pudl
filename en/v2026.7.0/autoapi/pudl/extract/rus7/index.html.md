# pudl.extract.rus7

Extract USDA RUS Form 7 data from CSVs.

## Attributes

| [`raw_rus7__all_dfs`](#pudl.extract.rus7.raw_rus7__all_dfs)   |    |
|---------------------------------------------------------------|----|
| [`raw_rus7_assets`](#pudl.extract.rus7.raw_rus7_assets)       |    |

## Classes

| [`Extractor`](#pudl.extract.rus7.Extractor)   | Extractor for USDA RUS Form 7.   |
|-----------------------------------------------|----------------------------------|

## Functions

| [`raw_rus7_asset_factory`](#pudl.extract.rus7.raw_rus7_asset_factory)(table_name)   | Create raw RUS Form 7 asset for a specific page.   |
|-------------------------------------------------------------------------------------|----------------------------------------------------|

## Module Contents

### *class* pudl.extract.rus7.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.csv.CsvExtractor`](../csv/index.md#pudl.extract.csv.CsvExtractor)

Extractor for USDA RUS Form 7.

#### METADATA

Instance of metadata object to use with this extractor.

#### READ_CSV_KWARGS

Keyword arguments that are passed to `pandas.read_csv()`.

These allow customization of the CSV parsing process. For example, you can specify
the column delimiter, data types, date parsing, etc. This can greatly reduce peak
memory usage and speed up the extraction process. Unfortunately you must refer to
the column headers using their original names as they appear in the CSV.

TODO[zaneselvans] 2024-04-19: it would be useful to be able to specify different CSV
reading options for different pages within the same dataset. At the moment the same
arguments will be applied to all pages. This still allows some flexibility because
some `pandas.read_csv()` arguments like `dtype` don’t raise errors if the
columns they apply to aren’t present.

#### source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Get the file name for the right page and part.

In this instance we are using the same methodology from the excel metadata extractor.

#### process_raw(df, page, \*\*partition)

Adds source column and report_year column if missing.

### pudl.extract.rus7.raw_rus7_\_all_dfs

### pudl.extract.rus7.raw_rus7_asset_factory(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str))

Create raw RUS Form 7 asset for a specific page.

### pudl.extract.rus7.raw_rus7_assets
