# pudl.dagster.assets.core.unmapped_ids

Dagster assets identifying FERC1/EIA plant & utility IDs missing manual mapping.

FERC Form 1 and EIA plants and utilities are linked together using a manually curated
ID mapping spreadsheet (see [`pudl.glue.ferc1_eia`](../../../../glue/ferc1_eia/index.html.md#module-pudl.glue.ferc1_eia)). As new years of data are
added, new plants and utilities show up that haven’t been manually mapped yet. The
assets defined here identify those unmapped IDs so they can be reviewed and added to
the mapping spreadsheet – see [PUDL ID Mapping](../../../../../../dev/pudl_id_mapping.html.md) for the overall process.

We expect these assets to always be empty in a healthy ETL run; the asset checks
registered against them in [`pudl.dagster.asset_checks`](../../../asset_checks/index.html.md#module-pudl.dagster.asset_checks) fail the run if any of
them contain rows. Each is also written out as a CSV under `$PUDL_OUTPUT` so the
unmapped IDs are easy to find and use when updating the mapping spreadsheet.

These CSVs are development-only artifacts: they should show up in the raw build
outputs on `builds.catalyst.coop`, but must never reach the public S3/GCS buckets
or Zenodo releases. `pudl.deploy.pudl.prepare_outputs_for_distribution` excludes
them from public distribution by matching the `missing_*.csv` filename pattern, so
every output asset name defined here must keep that `missing_` prefix.

## Attributes

| [`logger`](#pudl.dagster.assets.core.unmapped_ids.logger)                                |                                                                                                                                                                  |
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`_generic_ferc1_plant_transform_assets`](#pudl.dagster.assets.core.unmapped_ids._generic_ferc1_plant_transform_assets) |                                                                                                                                                                  |
| [`_eia923_plant_id_tables`](#pudl.dagster.assets.core.unmapped_ids._eia923_plant_id_tables)               |                                                                                                                                                                  |
| [`UNMAPPED_ID_ASSET_NAMES`](#pudl.dagster.assets.core.unmapped_ids.UNMAPPED_ID_ASSET_NAMES)               | Names of every unmapped-ID output asset, for use in [`pudl.dagster.asset_checks`](../../../asset_checks/index.html.md#module-pudl.dagster.asset_checks). |

## Functions

| [`_lower_strings`](#pudl.dagster.assets.core.unmapped_ids._lower_strings)(→ pandas.DataFrame)           | Lower-case all string values so case differences don't break ID matching.      |
|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`_write_unmapped_ids_csv`](#pudl.dagster.assets.core.unmapped_ids._write_unmapped_ids_csv)(→ None)              | Write an unmapped-IDs dataframe to `$PUDL_OUTPUT` as a CSV for manual mapping. |
| [`_util_ids_ferc1_raw_dbf`](#pudl.dagster.assets.core.unmapped_ids._util_ids_ferc1_raw_dbf)(→ pandas.DataFrame)  | Utility IDs reported in the raw FERC1 DBF database, for manual ID mapping.     |
| [`_util_ids_ferc1_raw_xbrl`](#pudl.dagster.assets.core.unmapped_ids._util_ids_ferc1_raw_xbrl)(→ pandas.DataFrame) | Utility IDs reported in the raw FERC1 XBRL database, for manual ID mapping.    |
| [`_plants_ferc1_raw`](#pudl.dagster.assets.core.unmapped_ids._plants_ferc1_raw)(→ pandas.DataFrame)        | All raw FERC1 plants across DBF & XBRL, compiled for manual ID mapping.        |
| [`_eia923_plant_ids`](#pudl.dagster.assets.core.unmapped_ids._eia923_plant_ids)(→ set)                     | All `plant_id_eia` values that appear in the core EIA-923 tables.              |
| [`unmapped_plants`](#pudl.dagster.assets.core.unmapped_ids.unmapped_plants)(...)                         | Identify FERC1 & EIA plants that appear in the data but aren't PUDL-mapped.    |
| [`unmapped_utilities`](#pudl.dagster.assets.core.unmapped_ids.unmapped_utilities)(...)                      | Identify FERC1 & EIA utilities that appear in the data but aren't PUDL-mapped. |

## Module Contents

### pudl.dagster.assets.core.unmapped_ids.logger

### pudl.dagster.assets.core.unmapped_ids.\_lower_strings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Lower-case all string values so case differences don’t break ID matching.

### pudl.dagster.assets.core.unmapped_ids.\_write_unmapped_ids_csv(context: [dagster.AssetExecutionContext](https://dagster.io/docs/api/dagster/execution/#dagster.AssetExecutionContext), name: [str](https://docs.python.org/3/builtins/stdtypes.html#str), df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [None](https://docs.python.org/3/builtins/constants.html#None)

Write an unmapped-IDs dataframe to `$PUDL_OUTPUT` as a CSV for manual mapping.

### pudl.dagster.assets.core.unmapped_ids.\_util_ids_ferc1_raw_dbf(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Utility IDs reported in the raw FERC1 DBF database, for manual ID mapping.

### pudl.dagster.assets.core.unmapped_ids.\_util_ids_ferc1_raw_xbrl(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Utility IDs reported in the raw FERC1 XBRL database, for manual ID mapping.

### pudl.dagster.assets.core.unmapped_ids.\_generic_ferc1_plant_transform_assets

### pudl.dagster.assets.core.unmapped_ids.\_plants_ferc1_raw(\*\*generic_plant_tables: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

All raw FERC1 plants across DBF & XBRL, compiled for manual ID mapping.

### pudl.dagster.assets.core.unmapped_ids.\_eia923_plant_id_tables

### pudl.dagster.assets.core.unmapped_ids.\_eia923_plant_ids(\*\*eia923_dfs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [set](https://docs.python.org/3/builtins/stdtypes.html#set)

All `plant_id_eia` values that appear in the core EIA-923 tables.

### pudl.dagster.assets.core.unmapped_ids.unmapped_plants(context: [dagster.AssetExecutionContext](https://dagster.io/docs/api/dagster/execution/#dagster.AssetExecutionContext), core_pudl_\_entity_plants_pudl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_ferc1_pudl_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_eia_pudl_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_plants_ferc1_raw: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[dagster.Output](https://dagster.io/docs/api/dagster/ops/#dagster.Output)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]]

Identify FERC1 & EIA plants that appear in the data but aren’t PUDL-mapped.

### pudl.dagster.assets.core.unmapped_ids.unmapped_utilities(context: [dagster.AssetExecutionContext](https://dagster.io/docs/api/dagster/execution/#dagster.AssetExecutionContext), core_pudl_\_entity_utilities_pudl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_ferc1_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_ferc1_dbf_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_ferc1_xbrl_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_ferc1_pudl_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_eia_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_util_ids_ferc1_raw_dbf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_util_ids_ferc1_raw_xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_eia923_plant_ids: [set](https://docs.python.org/3/builtins/stdtypes.html#set), core_eia860_\_scd_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[dagster.Output](https://dagster.io/docs/api/dagster/ops/#dagster.Output)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]]

Identify FERC1 & EIA utilities that appear in the data but aren’t PUDL-mapped.

### pudl.dagster.assets.core.unmapped_ids.UNMAPPED_ID_ASSET_NAMES *: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), ...]* *= ('missing_plant_id_pudl_in_plants_ferc1', 'missing_plants_in_plants_ferc1',...*

Names of every unmapped-ID output asset, for use in [`pudl.dagster.asset_checks`](../../../asset_checks/index.html.md#module-pudl.dagster.asset_checks).
