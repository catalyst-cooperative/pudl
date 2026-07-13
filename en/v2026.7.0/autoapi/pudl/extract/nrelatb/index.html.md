# pudl.extract.nrelatb

Routines used for extracting the raw NREL ATB data.

## Attributes

| [`raw_nrelatb__all_dfs`](#pudl.extract.nrelatb.raw_nrelatb__all_dfs)   |    |
|------------------------------------------------------------------------|----|

## Classes

| [`Extractor`](#pudl.extract.nrelatb.Extractor)   | Extractor for NREL ATB.   |
|--------------------------------------------------|---------------------------|

## Functions

| [`raw_nrelatb__data`](#pudl.extract.nrelatb.raw_nrelatb__data)(raw_nrelatb_\_all_dfs)   | Extract raw NREL ATB data from annual parquet files to one dataframe.   |
|-----------------------------------------------------------------------------------------|-------------------------------------------------------------------------|

## Module Contents

### *class* pudl.extract.nrelatb.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.parquet.ParquetExtractor`](../parquet/index.md#pudl.extract.parquet.ParquetExtractor)

Extractor for NREL ATB.

#### METADATA

Instance of metadata object to use with this extractor.

### pudl.extract.nrelatb.raw_nrelatb_\_all_dfs

### pudl.extract.nrelatb.raw_nrelatb_\_data(raw_nrelatb_\_all_dfs)

Extract raw NREL ATB data from annual parquet files to one dataframe.

* **Returns:**
  An extracted NREL ATB dataframe.
