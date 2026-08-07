# pudl.extract.ferc2

Extract FERC Form 2 data from SQLite DBs derived from original DBF files.

The Form No. 2 is a compilation of financial and operational information from major
interstate natural gas pipelines subject to the jurisdiction of the FERC. The form
contains data for a calendar year. Among other things, the form contains a Comparative
Balance Sheet, Statement of Income, Statement of Retained Earnings, Statement of Cash
Flows, and Notes to Financial Statements.

Major is defined as having combined gas transported or stored for a fee that exceeds 50
million dekatherms.

## Attributes

| [`logger`](#pudl.extract.ferc2.logger)   |    |
|------------------------------------------|----|

## Classes

| [`Ferc2DbfExtractor`](#pudl.extract.ferc2.Ferc2DbfExtractor)   | Wrapper for running the foxpro to sqlite conversion of FERC1 dataset.   |
|----------------------------------------------------------------|-------------------------------------------------------------------------|

## Module Contents

### pudl.extract.ferc2.logger

### *class* pudl.extract.ferc2.Ferc2DbfExtractor(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), data_config: [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Bases: [`pudl.extract.dbf.FercDbfExtractor`](../dbf/index.md#pudl.extract.dbf.FercDbfExtractor)

Wrapper for running the foxpro to sqlite conversion of FERC1 dataset.

#### DATASET *= 'ferc2'*

#### DATABASE_NAME *= 'ferc2_dbf.sqlite'*

#### finalize_schema(meta: sqlalchemy.MetaData) → sqlalchemy.MetaData

Add primary and foreign keys for respondent_id.

#### *static* is_valid_partition(fl: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any])

Drops partition with non-empty part fields.

#### aggregate_table_frames(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[pudl.extract.dbf.PartitionedDataFrame](../dbf/index.md#pudl.extract.dbf.PartitionedDataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Runs the deduplication on f2_s0_respondent_id table.

Other tables are aggregated as usual, meaning that the partial frames are simply
concatenated.
