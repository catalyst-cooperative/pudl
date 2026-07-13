# pudl.extract.eia191

Extract EIA Form 191 data from CSVs.

## Attributes

| [`raw_eia191__all_dfs`](#pudl.extract.eia191.raw_eia191__all_dfs)   |    |
|---------------------------------------------------------------------|----|

## Classes

| [`Extractor`](#pudl.extract.eia191.Extractor)   | Extractor for EIA form 191.   |
|-------------------------------------------------|-------------------------------|

## Functions

| [`raw_eia191__data`](#pudl.extract.eia191.raw_eia191__data)(raw_eia191_\_all_dfs)   | Extract raw EIA company data from CSV sheets into dataframes.   |
|-------------------------------------------------------------------------------------|-----------------------------------------------------------------|

## Module Contents

### *class* pudl.extract.eia191.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.csv.CsvExtractor`](../csv/index.md#pudl.extract.csv.CsvExtractor)

Extractor for EIA form 191.

#### METADATA

Instance of metadata object to use with this extractor.

#### get_page_cols(page: [str](https://docs.python.org/3/library/stdtypes.html#str), partition_key: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Get the columns for a particular page and partition key.

EIA 191 data has the same set of columns for all years,
so regardless of the partition key provided we select the same columns here.

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Rename columns using `any_year` partition.

### pudl.extract.eia191.raw_eia191_\_all_dfs

### pudl.extract.eia191.raw_eia191_\_data(raw_eia191_\_all_dfs)

Extract raw EIA company data from CSV sheets into dataframes.

* **Returns:**
  An extracted EIA 191 dataframe.
