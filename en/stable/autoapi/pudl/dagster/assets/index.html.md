# pudl.dagster.assets

Dagster asset loading and grouping for PUDL.

This module is responsible for collecting asset-definition modules into the grouped
asset sets that the PUDL code location exposes. Define group registries, package-level
asset lists, and helper utilities for working with loaded asset definitions here. Keep
individual asset implementations in the submodules that own them so this file remains a
thin registry and assembly layer.

Submodules in this package should define only assets that don’t fit well into the
pre-existing categorization of assets that PUDL uses, mostly oriented around the
layer of processing being applied in the ETL (raw, core, output) or the source dataset
(e.g. eia860, ferc714). If an asset doesn’t fit neatly into those categories, add it
to this package.

For the underlying Dagster concept, see [https://docs.dagster.io/guides/build/assets](https://docs.dagster.io/guides/build/assets)

## Submodules

* [pudl.dagster.assets.core](core/index.md)
* [pudl.dagster.assets.deploy](deploy/index.md)
* [pudl.dagster.assets.raw](raw/index.md)

## Attributes

| [`raw_module_groups`](#pudl.dagster.assets.raw_module_groups)                 |    |
|-------------------------------------------------------------------------------|----|
| [`core_module_groups`](#pudl.dagster.assets.core_module_groups)               |    |
| [`out_module_groups`](#pudl.dagster.assets.out_module_groups)                 |    |
| [`ferceqr_deployment_assets`](#pudl.dagster.assets.ferceqr_deployment_assets) |    |
| [`all_asset_modules`](#pudl.dagster.assets.all_asset_modules)                 |    |
| [`default_assets`](#pudl.dagster.assets.default_assets)                       |    |
| [`asset_keys`](#pudl.dagster.assets.asset_keys)                               |    |

## Functions

| [`get_keys_from_assets`](#pudl.dagster.assets.get_keys_from_assets)(→ list[dagster.AssetKey])   | Get a list of asset keys for an asset definition or spec.   |
|-------------------------------------------------------------------------------------------------|-------------------------------------------------------------|

## Package Contents

### pudl.dagster.assets.raw_module_groups

### pudl.dagster.assets.core_module_groups

### pudl.dagster.assets.out_module_groups

### pudl.dagster.assets.ferceqr_deployment_assets

### pudl.dagster.assets.all_asset_modules

### pudl.dagster.assets.default_assets

### pudl.dagster.assets.get_keys_from_assets(asset_def) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)]

Get a list of asset keys for an asset definition or spec.

### pudl.dagster.assets.asset_keys
