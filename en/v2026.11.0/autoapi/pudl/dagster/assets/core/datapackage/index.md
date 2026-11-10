# pudl.dagster.assets.core.datapackage

Dagster asset that generates the PUDL frictionless datapackage descriptor.

The descriptor is written to `$PUDL_OUTPUT/parquet/datapackage.json`, which
is the canonical frictionless datapackage filename.  It is enriched with
per-resource file statistics (bytes, SHA-256 hash), runtime provenance fields
(UUID `id`, `git_sha`, `git_tags`), per-source Zenodo DOIs, and links to
the PUDL documentation page for each data source.

## Functions

| [`build_datapackage_asset`](#pudl.dagster.assets.core.datapackage.build_datapackage_asset)(package, parquet_asset_keys, ...) → dagster.AssetsDefinition   | Return a Dagster asset that writes `datapackage.json` for PUDL parquet outputs.   |
|-----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|

## Module Contents

### pudl.dagster.assets.core.datapackage.build_datapackage_asset(package: [pudl.metadata.classes.Package](../../../../metadata/classes/index.md#pudl.metadata.classes.Package), parquet_asset_keys: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.AssetKey](https://dagster.io/docs/api/dagster/assets/#dagster.AssetKey)], asset_name: [str](https://docs.python.org/3/builtins/stdtypes.html#str), group_name: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'core_pudl', partitioned: [bool](https://docs.python.org/3/builtins/functions.html#bool) = False) → [dagster.AssetsDefinition](https://dagster.io/docs/api/dagster/assets/#dagster.AssetsDefinition)

Return a Dagster asset that writes `datapackage.json` for PUDL parquet outputs.

The asset depends on every asset in *parquet_asset_keys* so Dagster will
only run it once all parquet outputs for the current job are materialised.

* **Parameters:**
  * **package** – Frictionless datapackage to enhance and return.
  * **parquet_asset_keys** – Keys of all assets that write parquet files and
    should be described in the datapackage.
  * **asset_name** – Name of the generated Dagster asset.
  * **group_name** – Dagster asset group the generated asset belongs to.
  * **partitioned** – Whether the upstream parquet assets are partitioned. When
    `True`, each dependency uses a [`dagster.AllPartitionMapping`](https://dagster.io/docs/api/dagster/partitions/#dagster.AllPartitionMapping)
    so the descriptor is only written once all partitions of every
    upstream asset have been materialised.
