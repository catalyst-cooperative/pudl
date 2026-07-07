# pudl.extract.ferc1

Extract FERC Form 1 data from SQLite DBs derived from original DBF or XBRL files.

The FERC Form 1 data is available in two primary formats, spanning different years. The
early digital data (1994-2020) was distributed using annual Visual FoxPro databases.
Starting in 2021, the agency moved to using XBRL (a dialect of XML) published via an
RSS feed one filing at a time. First we convert both of those difficult to use original
formats into relational databases (currently stored in SQLite). We use those
databases as the starting point for our extensive cleaning and reorganization of a small
portion of the available tables into a well normalized database that covers all the
years of available data. The complete input databases are published separately to
provide users access to all of the original tables, since we’ve only been able to
clean up a small subset of them.

The conversion from both DBF and XBRL to SQLite is coordinated by the
[`pudl.dagster.assets.raw.ferc_to_sqlite`](../../dagster/assets/raw/ferc_to_sqlite/index.md#module-pudl.dagster.assets.raw.ferc_to_sqlite) asset module. The code for the XBRL to
SQLite conversion is used across all the modern FERC forms, and is contained in a
standalone
[ferc-xbrl-extractor package](https://github.com/catalyst-cooperative/ferc-xbrl-extractor).

The code for converting the older FERC 1 DBF files into an SQLite DB is contained in
this module.

One challenge with both of these data sources is that each year of data is treated as a
standalone resource by FERC. The databases are not explicitly linked together across
years. Over time the structure of the Visual FoxPro DB has changed as new tables and
fields have been added. In order to be able to use the data to do analyses across many
years, we need to bring all of it into a unified structure. These structural changes
have only ever been additive – more recent versions of the DBF databases contain all
the tables and fields that existed in earlier versions.

PUDL uses the most recently released year of DBF data (2020) as a template for the
database schema, since it is capable of containing all the= fields and tables found in
the other years.  The structure of the database is also informed by other documentation
we have been able to compile over the years from the FERC website and other sources.
Copies of these resources are included in the [FERC Form 1 data source
documentation](../../../../data_sources/ferc1.md)

Using this inferred structure PUDL creates an SQLite database mirroring the FERC
database using `sqlalchemy`. Then we use a python package called [dbfread](https://dbfread.readthedocs.io/en/latest/) to extract the data from the DBF tables,
and insert it virtually unchanged into the SQLite database.

Note that many quantities in the Visual FoxPro databases are tied not just to a
particular table and column, but to a row number within an individual filing, and
those row numbers have changed slowly over the years for some tables as rows have been
added or removed from the form. The `f1_row_lit_tbl` table contains a record of these
changes, and can be used to align reported quantities across time.

The one significant change we make to the raw input data is to ensure that there’s a
master table of the all the respondent IDs and respondent names. All the other tables
refer to this table. Unlike the other tables the `f1_respondent_id` table has no
`report_year` and so it represents a merge of all the years of data. In the event that
the name associated with a given respondent ID has changed over time, we retain the most
recently reported name.

Note that there are a small number of respondent IDs that **do not** appear in any year
of the `f1_respondent_id` table, but that **do** appear in the data tables. We add
these observed but not directly reported IDs to the `f1_respondent_id` table and have
done our best to identify what utility they correspond to based on the assets associated
with those respondent IDs.

This SQLite compilation of the original FERC Form 1 databases accommodates all
116 tables from all the published years of DBF data (1994-2020) and takes up about 1GB
of space on disk. You can interact with the most recent development version of this
database online [here](https://data.catalyst.coop/ferc1_dbf/).

## Attributes

| [`logger`](#pudl.extract.ferc1.logger)                                           |                                                                            |
|----------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [`FERC1_DBF_SQLITE_ASSET_KEY`](#pudl.extract.ferc1.FERC1_DBF_SQLITE_ASSET_KEY)   |                                                                            |
| [`FERC1_XBRL_SQLITE_ASSET_KEY`](#pudl.extract.ferc1.FERC1_XBRL_SQLITE_ASSET_KEY) |                                                                            |
| [`RawFercTableName`](#pudl.extract.ferc1.RawFercTableName)                       |                                                                            |
| [`TABLE_NAME_MAP_FERC1`](#pudl.extract.ferc1.TABLE_NAME_MAP_FERC1)               | A mapping of PUDL DB table names to their XBRL and DBF source table names. |
| [`XBRL_META_ONLY_FERC1`](#pudl.extract.ferc1.XBRL_META_ONLY_FERC1)               | A mapping of XBRL to (future) PUDL table names for tables not yet in PUDL. |
| [`raw_ferc1_assets`](#pudl.extract.ferc1.raw_ferc1_assets)                       |                                                                            |

## Classes

| [`RawTableMapping`](#pudl.extract.ferc1.RawTableMapping)     | Mapping between normalized PUDL table and raw DBF/XBRL table names.   |
|--------------------------------------------------------------|-----------------------------------------------------------------------|
| [`Ferc1DbfExtractor`](#pudl.extract.ferc1.Ferc1DbfExtractor) | Wrapper for running the foxpro to sqlite conversion of FERC1 dataset. |

## Functions

| [`create_raw_ferc1_assets`](#pudl.extract.ferc1.create_raw_ferc1_assets)(→ list[dagster.AssetSpec])             | Create AssetSpecs for raw ferc1 tables.                         |
|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`raw_ferc1_xbrl__metadata_json`](#pudl.extract.ferc1.raw_ferc1_xbrl__metadata_json)(→ dict[str, dict[str, ...) | Extract the FERC 1 XBRL Taxonomy metadata we've stored as JSON. |

## Module Contents

### pudl.extract.ferc1.logger

### pudl.extract.ferc1.FERC1_DBF_SQLITE_ASSET_KEY

### pudl.extract.ferc1.FERC1_XBRL_SQLITE_ASSET_KEY

### pudl.extract.ferc1.RawFercTableName

### *class* pudl.extract.ferc1.RawTableMapping

Bases: `TypedDict`

Mapping between normalized PUDL table and raw DBF/XBRL table names.

#### dbf *: [RawFercTableName](#pudl.extract.ferc1.RawFercTableName)*

#### xbrl *: [RawFercTableName](#pudl.extract.ferc1.RawFercTableName)*

### pudl.extract.ferc1.TABLE_NAME_MAP_FERC1 *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [RawTableMapping](#pudl.extract.ferc1.RawTableMapping)]*

A mapping of PUDL DB table names to their XBRL and DBF source table names.

### pudl.extract.ferc1.XBRL_META_ONLY_FERC1 *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [RawTableMapping](#pudl.extract.ferc1.RawTableMapping)]*

A mapping of XBRL to (future) PUDL table names for tables not yet in PUDL.

We need this mapping so that we can have a meaningful reference to outside tables in
some inter-table XBRL calculations, even though we aren’t yet reading those tables into
PUDL. We pull information about these tables from the XBRL metadata, but the data is
not extracted from the DBF and XBRL derived SQLite DBs.

### *class* pudl.extract.ferc1.Ferc1DbfExtractor(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), data_config: [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Bases: [`pudl.extract.dbf.FercDbfExtractor`](../dbf/index.md#pudl.extract.dbf.FercDbfExtractor)

Wrapper for running the foxpro to sqlite conversion of FERC1 dataset.

#### DATASET *= 'ferc1'*

#### DATABASE_NAME *= 'ferc1_dbf.sqlite'*

#### finalize_schema(meta: sqlalchemy.MetaData) → sqlalchemy.MetaData

Modifies schema before it’s written to sqlite database.

This marks f1_responent_id.respondent_id as a primary key and adds foreign key
constraints on all tables with respondent_id column.

#### postprocess()

Applies final transformations on the data in sqlite database.

This identifies respondents that are referenced by other tables but that are not
in the f1_respondent_id tables. These missing respondents are inserted back into
f1_respondent_id table.

#### PUDL_RIDS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Missing FERC 1 Respondent IDs for which we have identified the respondent.

#### add_missing_respondents()

Add missing respondents to f1_respondent_id table.

Some respondent_ids are referenced by other tables, but are not listed in the
f1_respondent_id table. This function finds all of these missing respondents and
backfills them into the f1_respondent_id table.

#### get_observed_respondents() → [set](https://docs.python.org/3/library/stdtypes.html#set)[[int](https://docs.python.org/3/library/functions.html#int)]

Compile the set of all observed respondent IDs found in the FERC 1 database.

A significant number of FERC 1 respondent IDs appear in the data tables, but not
in the f1_respondent_id table. In order to construct a self-consistent database
with we need to find all of those missing respondent IDs and inject them into
the table when we clone the database.

* **Returns:**
  Every respondent ID reported in any of the FERC 1 DB tables.

#### aggregate_table_frames(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[pudl.extract.dbf.PartitionedDataFrame](../dbf/index.md#pudl.extract.dbf.PartitionedDataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Deduplicates records in f1_respondent_id table.

### pudl.extract.ferc1.create_raw_ferc1_assets() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetSpec](https://docs.dagster.io/api/dagster/assets/#dagster.AssetSpec)]

Create AssetSpecs for raw ferc1 tables.

An [`dagster.AssetSpec`](https://docs.dagster.io/api/dagster/assets/#dagster.AssetSpec) allows you to access assets that are generated
elsewhere.  In our case, the xbrl and dbf database are created in a separate dagster
Definition.

* **Returns:**
  A list of ferc1 AssetSpecs.

### pudl.extract.ferc1.raw_ferc1_assets

### pudl.extract.ferc1.raw_ferc1_xbrl_\_metadata_json(context) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]]]

Extract the FERC 1 XBRL Taxonomy metadata we’ve stored as JSON.

* **Returns:**
  A dictionary keyed by PUDL table name, with an instant and a duration entry
  for each table, corresponding to the metadata for each of the respective instant
  or duration tables from XBRL if they exist. Table metadata is returned as a list
  of dictionaries, each of which can be interpreted as a row in a tabular
  structure, with each row annotating a separate XBRL concept from the FERC 1
  filings. If there is no instant/duration table, an empty list is returned
  instead.
