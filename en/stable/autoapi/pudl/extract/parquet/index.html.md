# pudl.extract.parquet

Extractor for Parquet data.

## Attributes

| [`logger`](#pudl.extract.parquet.logger)   |    |
|--------------------------------------------|----|

## Classes

| [`ParquetExtractor`](#pudl.extract.parquet.ParquetExtractor)   | Class for extracting dataframes from parquet files.   |
|----------------------------------------------------------------|-------------------------------------------------------|

## Module Contents

### pudl.extract.parquet.logger

### *class* pudl.extract.parquet.ParquetExtractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Bases: [`pudl.extract.extractor.GenericExtractor`](../extractor/index.md#pudl.extract.extractor.GenericExtractor)

Class for extracting dataframes from parquet files.

The extraction logic is invoked by calling extract() method of this class.

#### source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Produce the source Parquet file name as it will appear in the archive.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “data”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
* **Returns:**
  string name of the parquet file

#### load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce the dataframe object for the given partition.

This method assumes that the archive includes one unzipped file per partition.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “data”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  pd.DataFrame instance containing CSV data
