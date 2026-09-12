# pudl.extract.nrelatb

Routines used for extracting the raw NREL ATB data.

## Attributes

| [`raw_nrelatb__all_dfs`](#pudl.extract.nrelatb.raw_nrelatb__all_dfs)   |    |
|-------------------------------------------------------------------------|----|

## Classes

| [`Extractor`](#pudl.extract.nrelatb.Extractor)   | Extractor for NREL ATB.   |
|--------------------------------------------------------------|---------------------------|

## Functions

| [`raw_nrelatb__data`](#pudl.extract.nrelatb.raw_nrelatb__data)(raw_nrelatb_\_all_dfs)   | Extract raw NREL ATB data from annual parquet files to one dataframe.   |
|---------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|

## Module Contents

### *class* pudl.extract.nrelatb.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.extractor.GenericExtractor`](../extractor/index.html.md#pudl.extract.extractor.GenericExtractor)

Extractor for NREL ATB.

#### METADATA

Instance of metadata object to use with this extractor.

#### source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.html.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Get the file name for the right page and part.

In this instance we are using the same methodology from the excel metadata extractor.

#### load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition)

Fetch the electricity parquet file from the NREL ATB zip archive.

This is based on the csv extraction framework.

### pudl.extract.nrelatb.raw_nrelatb_\_all_dfs

### pudl.extract.nrelatb.raw_nrelatb_\_data(raw_nrelatb_\_all_dfs)

Extract raw NREL ATB data from annual parquet files to one dataframe.

* **Returns:**
  An extracted NREL ATB dataframe.
