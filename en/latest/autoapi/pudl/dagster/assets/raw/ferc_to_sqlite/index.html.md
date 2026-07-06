# pudl.dagster.assets.raw.ferc_to_sqlite

Dagster asset definitions for granular FERC-to-SQLite extraction.

This module defines the prerequisite assets that build the FERC DBF and XBRL derived
SQLite databases used elsewhere in the PUDL pipeline. It should contain asset factories,
resource requirements, and materialization metadata specific to those prerequisite
databases, rather than the downstream transforms that consume them.

## Attributes

| [`NETWORK_ERRORS`](#pudl.dagster.assets.raw.ferc_to_sqlite.NETWORK_ERRORS)                     |    |
|------------------------------------------------------------------------------------------------|----|
| [`logger`](#pudl.dagster.assets.raw.ferc_to_sqlite.logger)                                     |    |
| [`raw_ferc1_dbf__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc1_dbf__sqlite)       |    |
| [`raw_ferc2_dbf__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc2_dbf__sqlite)       |    |
| [`raw_ferc6_dbf__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc6_dbf__sqlite)       |    |
| [`raw_ferc60_dbf__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc60_dbf__sqlite)     |    |
| [`raw_ferc1_xbrl__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc1_xbrl__sqlite)     |    |
| [`raw_ferc2_xbrl__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc2_xbrl__sqlite)     |    |
| [`raw_ferc6_xbrl__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc6_xbrl__sqlite)     |    |
| [`raw_ferc60_xbrl__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc60_xbrl__sqlite)   |    |
| [`raw_ferc714_xbrl__sqlite`](#pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc714_xbrl__sqlite) |    |

## Classes

| [`FercPaths`](#pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths)   | Helper class to get paths to various FERC paths both local and remote.   |
|--------------------------------------------------------------------|--------------------------------------------------------------------------|

## Functions

| [`_download_zipped_outputs`](#pudl.dagster.assets.raw.ferc_to_sqlite._download_zipped_outputs)(paths, output_format)                        | Download nightly zipfile containing sqlite or parquet outputs and extract to local cache.   |
|---------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| [`_download_nightly_outputs`](#pudl.dagster.assets.raw.ferc_to_sqlite._download_nightly_outputs)(→ None)                                    | Download `ferc_to_sqlite` outputs from s3.                                                  |
| [`_check_for_cached_db_w_compatible_provenance`](#pudl.dagster.assets.raw.ferc_to_sqlite._check_for_cached_db_w_compatible_provenance)(...) | Check to see if there is a compatible outputs either locally, or in nightly builds.         |
| [`ferc_to_sqlite_asset_factory`](#pudl.dagster.assets.raw.ferc_to_sqlite.ferc_to_sqlite_asset_factory)(→ dagster.AssetsDefinition)          | Create a FERC-to-SQLite prerequisite asset for a specific FERC dataset.                     |

## Module Contents

### pudl.dagster.assets.raw.ferc_to_sqlite.NETWORK_ERRORS

### pudl.dagster.assets.raw.ferc_to_sqlite.logger

### *class* pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths

Helper class to get paths to various FERC paths both local and remote.

#### data_format *: Literal['dbf', 'xbrl']*

#### local_datapackage_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### nightly_datapackage_path *: upath.UPath*

#### local_sqlite_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### nightly_sqlite_path *: upath.UPath*

#### local_duckdb_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### nightly_duckdb_path *: upath.UPath | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### local_taxonomy_json_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### nightly_taxonomy_json_path *: upath.UPath | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### local_parquet_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### nightly_parquet_path *: upath.UPath | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### delete_local_outputs()

Helper function to delete local outputs before starting extraction.

#### *classmethod* from_dataset_format(dataset: [pudl.settings.FercForm](../../../../settings/index.md#pudl.settings.FercForm), data_format: Literal['dbf', 'xbrl'], paths: [pudl.workspace.setup.PudlPaths](../../../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [FercPaths](#pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths)

Initialize class based on `dataset` and `data_format`.

### pudl.dagster.assets.raw.ferc_to_sqlite.\_download_zipped_outputs(paths: [FercPaths](#pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths), output_format: Literal['sqlite', 'parquet'])

Download nightly zipfile containing sqlite or parquet outputs and extract to local cache.

### pudl.dagster.assets.raw.ferc_to_sqlite.\_download_nightly_outputs(data_format: Literal['dbf', 'xbrl'], paths: [FercPaths](#pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths)) → [None](https://docs.python.org/3/library/constants.html#None)

Download `ferc_to_sqlite` outputs from s3.

This will download all outputs produced by the `ferc_to_sqlite` process for the
provided `dataset` and `data_format`. For the ‘DBF’ format, this includes the
SQLite db and a datapackage JSON file, while ‘XBRL’ will include both of these
plus a DuckDB file, parquet files, and the taxonomy JSON file.

### pudl.dagster.assets.raw.ferc_to_sqlite.\_check_for_cached_db_w_compatible_provenance(dataset: [pudl.settings.FercForm](../../../../settings/index.md#pudl.settings.FercForm), data_format: Literal['dbf', 'xbrl'], zenodo_doi: [str](https://docs.python.org/3/library/stdtypes.html#str), paths: [FercPaths](#pudl.dagster.assets.raw.ferc_to_sqlite.FercPaths), ferc_to_sqlite: [pudl.settings.FercToSqliteDataConfig](../../../../settings/index.md#pudl.settings.FercToSqliteDataConfig)) → [pudl.dagster.provenance.FercSqliteProvenanceRecord](../../../provenance/index.md#pudl.dagster.provenance.FercSqliteProvenanceRecord) | [None](https://docs.python.org/3/library/constants.html#None)

Check to see if there is a compatible outputs either locally, or in nightly builds.

This function will first check the local datapackage for the specified `dataset`
and `data_format` to see if it contains a `FercSqliteProvenanceRecord` that
is compatible with the requirements of the current run. If the local datapckage doesn’t
exist or contains an incompatible record, it will then download the datapackage produced
by the most recent nightly build and perform the same check. If the nightly
outputs are found to be compatible with the current run, then it will
download all associated outputs from that run. For DBF outputs, this includes
the SQLite file and the datapackage JSON file, while XBRL outputs also
include a duckdb file, parquet files, and a taxonomy JSON file.

If the environment variable, `PUDL_FORCE_FERC_TO_SQLITE`, is set to `true`, then
this function will immediately return `None`, triggering the normal extraction.

* **Returns:**
  Compatible `FercSqliteProvenanceRecord` if one is found, otherwise `None`.

### pudl.dagster.assets.raw.ferc_to_sqlite.ferc_to_sqlite_asset_factory(, dataset: [pudl.settings.FercForm](../../../../settings/index.md#pudl.settings.FercForm), data_format: Literal['dbf', 'xbrl'], extract_function: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)], [None](https://docs.python.org/3/library/constants.html#None)], op_tags: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Create a FERC-to-SQLite prerequisite asset for a specific FERC dataset.

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc1_dbf_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc2_dbf_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc6_dbf_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc60_dbf_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc1_xbrl_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc2_xbrl_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc6_xbrl_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc60_xbrl_\_sqlite

### pudl.dagster.assets.raw.ferc_to_sqlite.raw_ferc714_xbrl_\_sqlite
