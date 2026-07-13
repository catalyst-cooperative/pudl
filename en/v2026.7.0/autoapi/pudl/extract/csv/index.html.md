# pudl.extract.csv

Extractor for CSV data.

## Attributes

| [`logger`](#pudl.extract.csv.logger)   |    |
|----------------------------------------|----|

## Classes

| [`CsvExtractor`](#pudl.extract.csv.CsvExtractor)   | Class for extracting dataframes from CSV files.   |
|----------------------------------------------------|---------------------------------------------------|

## Module Contents

### pudl.extract.csv.logger

### *class* pudl.extract.csv.CsvExtractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Bases: [`pudl.extract.extractor.GenericExtractor`](../extractor/index.md#pudl.extract.extractor.GenericExtractor)

Class for extracting dataframes from CSV files.

The extraction logic is invoked by calling extract() method of this class.

#### READ_CSV_KWARGS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]*

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

Produce the source CSV file name as it will appear in the archive.

This method assumes the CSV files within each partition are structured as
the: f”{self._dataset_name}_{partition_selection}.csv”

If you have a dataset with multiple pages within each partition you’ll need
to use a filemap.csv like we use in the excel extractor. For an example of
how to do this in the CSV extractor framework, see the RUS extractors.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “data”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  string name of the CSV file

#### load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce the dataframe object for the given partition.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “data”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  pd.DataFrame instance containing CSV data
