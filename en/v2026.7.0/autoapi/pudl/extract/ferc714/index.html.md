# pudl.extract.ferc714

Routines used for extracting the raw FERC 714 data.

## Attributes

| [`logger`](#pudl.extract.ferc714.logger)                                               |                                                                                   |
|----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`FERC714_XBRL_SQLITE_ASSET_KEY`](#pudl.extract.ferc714.FERC714_XBRL_SQLITE_ASSET_KEY) |                                                                                   |
| [`FERC714_CSV_ENCODING`](#pudl.extract.ferc714.FERC714_CSV_ENCODING)                   | Dictionary mapping PUDL tables to FERC-714 CSV filenames and character encodings. |
| [`TABLE_NAME_MAP_FERC714`](#pudl.extract.ferc714.TABLE_NAME_MAP_FERC714)               | A mapping of PUDL DB table names to their XBRL and CSV source table names.        |
| [`raw_ferc714_csv_assets`](#pudl.extract.ferc714.raw_ferc714_csv_assets)               |                                                                                   |
| [`raw_ferc714_xbrl_assets`](#pudl.extract.ferc714.raw_ferc714_xbrl_assets)             |                                                                                   |

## Functions

| [`raw_ferc714_csv_asset_factory`](#pudl.extract.ferc714.raw_ferc714_csv_asset_factory)(→ dagster.AssetsDefinition)    | Generates an asset for building the raw CSV-based FERC 714 dataframe.   |
|-----------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`raw_ferc714_xbrl__metadata_json`](#pudl.extract.ferc714.raw_ferc714_xbrl__metadata_json)(→ dict[str, dict[str, ...) | Extract the FERC 714 XBRL Taxonomy metadata we've stored as JSON.       |
| [`create_raw_ferc714_xbrl_assets`](#pudl.extract.ferc714.create_raw_ferc714_xbrl_assets)(→ list[dagster.AssetSpec])   | Create AssetSpecs for raw FERC 714 XBRL tables.                         |

## Module Contents

### pudl.extract.ferc714.logger

### pudl.extract.ferc714.FERC714_XBRL_SQLITE_ASSET_KEY

### pudl.extract.ferc714.FERC714_CSV_ENCODING *: [collections.OrderedDict](https://docs.python.org/3/library/collections.html#collections.OrderedDict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]*

Dictionary mapping PUDL tables to FERC-714 CSV filenames and character encodings.

### pudl.extract.ferc714.TABLE_NAME_MAP_FERC714 *: [collections.OrderedDict](https://docs.python.org/3/library/collections.html#collections.OrderedDict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A mapping of PUDL DB table names to their XBRL and CSV source table names.

### pudl.extract.ferc714.raw_ferc714_csv_asset_factory(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Generates an asset for building the raw CSV-based FERC 714 dataframe.

### pudl.extract.ferc714.raw_ferc714_xbrl_\_metadata_json(context) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]]]

Extract the FERC 714 XBRL Taxonomy metadata we’ve stored as JSON.

* **Returns:**
  A dictionary keyed by PUDL table name, with an instant and a duration entry
  for each table, corresponding to the metadata for each of the respective instant
  or duration tables from XBRL if they exist. Table metadata is returned as a list
  of dictionaries, each of which can be interpreted as a row in a tabular
  structure, with each row annotating a separate XBRL concept from the FERC 714
  filings.

### pudl.extract.ferc714.create_raw_ferc714_xbrl_assets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetSpec](https://docs.dagster.io/api/dagster/assets/#dagster.AssetSpec)]

Create AssetSpecs for raw FERC 714 XBRL tables.

AssetSpecs allow you to specify and access assets that are generated elsewhere.  In
our case, the XBRL database contains the raw FERC 714 assets from 2021 onward. Prior
to that, the assets are distributed as CSVs and are extracted with the
`raw_ferc714_csv_asset_factory` function.

* **Returns:**
  A list of FERC 714 AssetSpecs.

### pudl.extract.ferc714.raw_ferc714_csv_assets

### pudl.extract.ferc714.raw_ferc714_xbrl_assets
