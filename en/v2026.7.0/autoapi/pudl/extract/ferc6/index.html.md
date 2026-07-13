# pudl.extract.ferc6

Extract FERC Form 6 data from DBF archives.

## Classes

| [`Ferc6DbfExtractor`](#pudl.extract.ferc6.Ferc6DbfExtractor)   | Extracts FERC Form 6 data from the legacy DBF archives.   |
|----------------------------------------------------------------|-----------------------------------------------------------|

## Module Contents

### *class* pudl.extract.ferc6.Ferc6DbfExtractor(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), data_config: [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Bases: [`pudl.extract.dbf.FercDbfExtractor`](../dbf/index.md#pudl.extract.dbf.FercDbfExtractor)

Extracts FERC Form 6 data from the legacy DBF archives.

#### DATASET *= 'ferc6'*

#### DATABASE_NAME *= 'ferc6_dbf.sqlite'*

#### finalize_schema(meta: sqlalchemy.MetaData) → sqlalchemy.MetaData

Add primary and foreign keys for respondent_id.

#### aggregate_table_frames(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[pudl.extract.dbf.PartitionedDataFrame](../dbf/index.md#pudl.extract.dbf.PartitionedDataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Runs the deduplication on f6_s0_respondent_id table.

Other tables are aggregated as usual, meaning that the partial frames are simply
concatenated.
