# pudl.dagster.assets.core.datapackage

Dagster asset that generates the PUDL frictionless datapackage descriptor.

The descriptor is written to `$PUDL_OUTPUT/parquet/datapackage.json`, which
is the canonical frictionless datapackage filename.  It is enriched with
per-resource file statistics (bytes, SHA-256 hash), runtime provenance fields
(UUID `id`, `git_sha`, `git_tags`), per-source Zenodo DOIs, and links to
the PUDL documentation page for each data source.

## Functions

| [`build_pudl_datapackage_asset`](#pudl.dagster.assets.core.datapackage.build_pudl_datapackage_asset)(→ dagster.AssetsDefinition)   | Return a Dagster asset that writes `datapackage.json` for PUDL parquet outputs.   |
|------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|

## Module Contents

### pudl.dagster.assets.core.datapackage.build_pudl_datapackage_asset(parquet_asset_keys: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)]) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Return a Dagster asset that writes `datapackage.json` for PUDL parquet outputs.

The asset depends on every asset in *parquet_asset_keys* so Dagster will
only run it once all parquet outputs for the current job are materialised.

* **Parameters:**
  **parquet_asset_keys** – Keys of all assets that write parquet files and
  should be described in the datapackage.
