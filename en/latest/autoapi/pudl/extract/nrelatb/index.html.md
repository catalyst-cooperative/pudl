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

### *class* pudl.extract.nrelatb.Extractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.html.md#pudl.workspace.datastore.Datastore))

Bases: [`pudl.extract.extractor.GenericExtractor`](../extractor/index.html.md#pudl.extract.extractor.GenericExtractor)[[`pudl.extract.extractor.GenericMetadata`](../extractor/index.html.md#pudl.extract.extractor.GenericMetadata)]

Extractor for NREL ATB.

#### source_filename(page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.html.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Get the file name for the right page and part.

#### load_source(page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition)

Fetch the electricity parquet file from the NREL ATB zip archive.

This is based on the csv extraction framework.

### pudl.extract.nrelatb.raw_nrelatb_\_all_dfs

### pudl.extract.nrelatb.raw_nrelatb_\_data(raw_nrelatb_\_all_dfs)

Extract raw NREL ATB data from annual parquet files to one dataframe.

* **Returns:**
  An extracted NREL ATB dataframe.
