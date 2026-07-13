# pudl.dagster.provenance

Helpers for recording asset provenance and checking compatibility.

This module builds and interprets Dagster materialization metadata for assets so
downstream consumers can verify that the data they are using was created with compatible
inputs. Put provenance fingerprints, metadata builders, and compatibility checks here
when they describe the identity of a materialized asset, rather than the extraction
logic that produces the asset itself.

For the closest Dagster concept, see
[https://docs.dagster.io/guides/build/assets/metadata-and-tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags)

## Attributes

| [`logger`](#pudl.dagster.provenance.logger)                                           |    |
|---------------------------------------------------------------------------------------|----|
| [`FERC_TO_SQLITE_METADATA_KEY`](#pudl.dagster.provenance.FERC_TO_SQLITE_METADATA_KEY) |    |

## Classes

| [`FercSqliteProvenance`](#pudl.dagster.provenance.FercSqliteProvenance)             | The provenance requirements derived from the current run's data config.   |
|-------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| [`FercSqliteProvenanceRecord`](#pudl.dagster.provenance.FercSqliteProvenanceRecord) | Stored provenance + extra debugging fields from materialization time.     |

## Functions

| [`_get_ferc_to_sqlite_asset_key`](#pudl.dagster.provenance._get_ferc_to_sqlite_asset_key)(→ dagster.AssetKey)   | Return the asset key corresponding to a ferc_to_sqlite asset from dataset/format.   |
|-----------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [`get_xbrl_extractor_version`](#pudl.dagster.provenance.get_xbrl_extractor_version)(→ str)                      | Return the installed version of `catalystcoop.ferc_xbrl_extractor`.                 |
| [`ferc_sqlite_provenance_is_compatible`](#pudl.dagster.provenance.ferc_sqlite_provenance_is_compatible)(→ bool) | Ensure a persisted FERC SQLite prerequisite is compatible with this run.            |

## Module Contents

### pudl.dagster.provenance.logger

### pudl.dagster.provenance.FERC_TO_SQLITE_METADATA_KEY *= 'ferc_to_sqlite'*

### pudl.dagster.provenance.\_get_ferc_to_sqlite_asset_key(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), data_format: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)

Return the asset key corresponding to a ferc_to_sqlite asset from dataset/format.

### *class* pudl.dagster.provenance.FercSqliteProvenance

The provenance requirements derived from the current run’s data config.

Computed from `data_config` and `zenodo_dois` to describe what a
compatible FERC SQLite prerequisite must contain. Used by
`assert_ferc_sqlite_compatible()` to compare against the stored
[`FercSqliteProvenanceRecord`](#pudl.dagster.provenance.FercSqliteProvenanceRecord) that was written when the DB was built.

#### dataset *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### data_format *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### zenodo_doi *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

#### ferc_xbrl_extractor_version *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### *property* asset_key *: [dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)*

The AssetKey corresponding to the extracted SQLite database.

### *class* pudl.dagster.provenance.FercSqliteProvenanceRecord(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Stored provenance + extra debugging fields from materialization time.

#### dataset *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### data_format *: Literal['dbf', 'xbrl']*

#### status *: Literal['complete', 'not_configured']*

#### source *: Literal['nightly', 'local_cache', 'local_new']*

#### zenodo_doi *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### data_config *: [pudl.settings.FercToSqliteDataConfig](../../settings/index.md#pudl.settings.FercToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc_xbrl_extractor_version *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* from_dagster_instance(instance: [dagster.DagsterInstance](https://docs.dagster.io/api/dagster/internals/#dagster.DagsterInstance), dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), data_format: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [FercSqliteProvenanceRecord](#pudl.dagster.provenance.FercSqliteProvenanceRecord)

Return FercSqliteProvenanceRecord from dagster metadata if available.

* **Raises:**
  [**RuntimeError**](https://docs.python.org/3/library/exceptions.html#RuntimeError) – if no Dagster provenance metadata is available.

#### to_datapackage(datapackage_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Write Provenance data to datapackage JSON file.

#### *classmethod* from_datapackage(datapackage_path: upath.UPath, source: Literal['nightly', 'local_cache', 'local_new']) → [FercSqliteProvenanceRecord](#pudl.dagster.provenance.FercSqliteProvenanceRecord)

Read SQLite provenance metadata from datapackage JSON file.

Note that this method accepts `datapackage_path` as a `UPath` as we read
provenance metadata directly from nightly builds, but `to_datapackage` only
accepts a regular `Path`, as we should never try to write directly to s3.

### pudl.dagster.provenance.get_xbrl_extractor_version() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Return the installed version of `catalystcoop.ferc_xbrl_extractor`.

### pudl.dagster.provenance.ferc_sqlite_provenance_is_compatible(, observed_provenance: [FercSqliteProvenanceRecord](#pudl.dagster.provenance.FercSqliteProvenanceRecord) | [None](https://docs.python.org/3/library/constants.html#None), required_provenance: [FercSqliteProvenance](#pudl.dagster.provenance.FercSqliteProvenance)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Ensure a persisted FERC SQLite prerequisite is compatible with this run.

Compatibility requires three conditions to hold:

1. The Zenodo DOI recorded when the FERC SQLite DB was built must match the
   current [`ZenodoDoiSettings`](../../workspace/datastore/index.md#pudl.workspace.datastore.ZenodoDoiSettings). A mismatch
   means the raw archive has changed version and the DB must be rebuilt.
2. The years stored in the FERC SQLite DB must be a *superset* of the years
   needed by the current downstream data config. This allows a “full” FERC SQLite DB
   to serve a “fast” downstream run without an expensive rebuild.
3. The version of `ferc_xbrl_extractor` is the same for XBRL derived data.
