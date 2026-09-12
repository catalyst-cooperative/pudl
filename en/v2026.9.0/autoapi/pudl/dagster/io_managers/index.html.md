# pudl.dagster.io_managers

Dagster IO managers used by PUDL assets.

This module defines the IO-manager implementations that translate between Dagster asset
execution and PUDL’s storage formats, including Parquet (with native GeoParquet support
for assets that return a [`geopandas.GeoDataFrame`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) and the FERC prerequisite
SQLite databases. Put [`dagster.IOManager`](https://docs.dagster.io/api/dagster/io-managers/#dagster.IOManager) and [`dagster.ConfigurableIOManager`](https://docs.dagster.io/api/dagster/io-managers/#dagster.ConfigurableIOManager)
classes here, along with configured singleton instances that the default code location
reuses. Keep data-processing logic out of this module; it should focus on persistence,
loading, and storage-compatibility concerns.

For the underlying Dagster concept, see [https://docs.dagster.io/guides/build/io-managers](https://docs.dagster.io/guides/build/io-managers)

## Attributes

| [`logger`](#pudl.dagster.io_managers.logger)                         |    |
|---------------------------------------------------------------------------------|----|
| [`parquet_io_manager`](#pudl.dagster.io_managers.parquet_io_manager)             |    |
| [`ferc1_dbf_sqlite_io_manager`](#pudl.dagster.io_managers.ferc1_dbf_sqlite_io_manager)    |    |
| [`ferc1_xbrl_sqlite_io_manager`](#pudl.dagster.io_managers.ferc1_xbrl_sqlite_io_manager)   |    |
| [`ferc714_xbrl_sqlite_io_manager`](#pudl.dagster.io_managers.ferc714_xbrl_sqlite_io_manager) |    |
| [`default_io_managers`](#pudl.dagster.io_managers.default_io_managers)            |    |

## Classes

| [`PudlParquetIOManager`](#pudl.dagster.io_managers.PudlParquetIOManager)    | IOManager that writes pudl tables to pyarrow parquet files.       |
|--------------------------------------------------------------------------|-------------------------------------------------------------------|
| [`FercSqliteIOManagerBase`](#pudl.dagster.io_managers.FercSqliteIOManagerBase) | Shared lazy-loading behavior for FERC SQLite Dagster IO managers. |
| [`FercDbfSqliteIOManager`](#pudl.dagster.io_managers.FercDbfSqliteIOManager)  | IO manager for reading tables from FERC DBF SQLite databases.     |
| [`FercXbrlSqliteIOManager`](#pudl.dagster.io_managers.FercXbrlSqliteIOManager) | IO manager for reading tables from a FERC XBRL SQLite database.   |

## Functions

| [`_get_dagster_instance_if_available`](#pudl.dagster.io_managers._get_dagster_instance_if_available)(...)   | Return the Dagster instance from an input context if one was provided.   |
|--------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`get_table_name_from_context`](#pudl.dagster.io_managers.get_table_name_from_context)(→ str)        | Retrieves the table name from the context object.                        |

## Module Contents

### pudl.dagster.io_managers.logger

### pudl.dagster.io_managers.\_get_dagster_instance_if_available(context: [dagster.InputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.InputContext)) → [dagster.DagsterInstance](https://docs.dagster.io/api/dagster/internals/#dagster.DagsterInstance) | [None](https://docs.python.org/3/library/constants.html#None)

Return the Dagster instance from an input context if one was provided.

Returns `None` in two cases where provenance checks should be skipped:

* The context has no attached instance (e.g. ad hoc `InputContext` objects built
  by notebook or integration-test helpers).
* The instance is ephemeral (created by `execute_in_process()` without an explicit
  `instance=` argument). An ephemeral instance has an empty event log, so
  provenance checks against it would always raise rather than meaningfully validate.

### pudl.dagster.io_managers.get_table_name_from_context(context: [dagster.InputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.InputContext) | [dagster.OutputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.OutputContext)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Retrieves the table name from the context object.

### *class* pudl.dagster.io_managers.PudlParquetIOManager(\*\*data: Any)

Bases: [`dagster.ConfigurableIOManager`](https://docs.dagster.io/api/dagster/io-managers/#dagster.ConfigurableIOManager)

IOManager that writes pudl tables to pyarrow parquet files.

#### pudl_paths *: dagster.ResourceDependency[[pudl.dagster.resources.PudlPathsResource](../resources/index.html.md#pudl.dagster.resources.PudlPathsResource)]*

#### *static* \_record_parquet_file_metadata(context: [dagster.OutputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.OutputContext), parquet_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [None](https://docs.python.org/3/library/constants.html#None)

Attach file size and SHA-256 hash to the Dagster output metadata.

This metadata is later retrieved by the `pudl_datapackage` asset to
populate the frictionless datapackage descriptor without re-reading the
parquet files.

#### handle_output(context: [dagster.OutputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.OutputContext), obj: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame) | polars.LazyFrame) → [None](https://docs.python.org/3/library/constants.html#None)

Writes a pudl dataframe to a Parquet file.

GeoDataFrames are written as GeoParquet using native geopandas output,
which produces spec-compliant CRS metadata readable by DuckDB >= 1.5.
Regular DataFrames and Polars LazyFrames use the PUDL PyArrow schema to
enforce exact column types on disk.

#### load_input(context: [dagster.InputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.InputContext)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame) | polars.LazyFrame

Loads pudl table from parquet file.

### pudl.dagster.io_managers.parquet_io_manager

### *class* pudl.dagster.io_managers.FercSqliteIOManagerBase(\*\*data: Any)

Bases: [`dagster.ConfigurableIOManager`](https://docs.dagster.io/api/dagster/io-managers/#dagster.ConfigurableIOManager)

Shared lazy-loading behavior for FERC SQLite Dagster IO managers.

Subclasses provide the query details for a particular FERC SQLite backend, while
this base class owns three shared responsibilities:

1. lazily creating and caching a SQLAlchemy engine for the configured database
2. lazily reflecting and caching SQLAlchemy metadata once the database exists
3. checking Dagster provenance metadata before each read

#### global_data_config *: dagster.ResourceDependency[[pudl.dagster.resources.GlobalDataConfigResource](../resources/index.html.md#pudl.dagster.resources.GlobalDataConfigResource)]*

#### pudl_paths *: dagster.ResourceDependency[[pudl.dagster.resources.PudlPathsResource](../resources/index.html.md#pudl.dagster.resources.PudlPathsResource)]*

#### zenodo_dois *: dagster.ResourceDependency[[pudl.dagster.resources.ZenodoDoiSettingsResource](../resources/index.html.md#pudl.dagster.resources.ZenodoDoiSettingsResource)]*

#### dataset *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### data_format *: ClassVar[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### \_engine *: sqlalchemy.Engine | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### \_metadata *: sqlalchemy.MetaData | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *property* \_years_key *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### *property* db_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Return the SQLite database name for this dataset and data format.

#### *property* db_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Return the canonical SQLite path for this dataset and data format.

#### *property* engine *: sqlalchemy.Engine*

Return a cached SQLAlchemy engine for this FERC SQLite database.

#### *property* metadata *: sqlalchemy.MetaData*

Return cached reflected metadata for this database.

The metadata is reflected on first access and reused for subsequent reads.
Accessing this property requires the SQLite database to already exist.

#### \_get_sqlalchemy_table(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → sqlalchemy.Table

Return reflected SQLAlchemy table metadata for a FERC SQLite table.

#### \_check_provenance(context: [dagster.InputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.InputContext)) → [None](https://docs.python.org/3/library/constants.html#None)

Check that the existing FERC SQLite database is compatible with this run.

This is intentionally separate from engine and metadata caching because the
compatibility check depends on the Dagster run context rather than on local
process state.

#### load_input(context: [dagster.InputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.InputContext)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load a dataframe from the configured FERC SQLite database.

Ensure that the database exists and its schema has been reflected, then verify
the upstream FERC-to-SQLite provenance recorded in Dagster before delegating to
the subclass-specific query implementation.

#### *abstractmethod* handle_output(context: [dagster.OutputContext](https://docs.dagster.io/api/dagster/io-managers/#dagster.OutputContext), obj: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Reject writes because these IO managers currently support reads only.

#### *abstractmethod* \_query(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Execute a filtered read against the FERC SQLite database.

### *class* pudl.dagster.io_managers.FercDbfSqliteIOManager(\*\*data: Any)

Bases: [`FercSqliteIOManagerBase`](#pudl.dagster.io_managers.FercSqliteIOManagerBase)

IO manager for reading tables from FERC DBF SQLite databases.

Instantiate with `dataset` (`ferc1`, `ferc714`, etc.)

#### data_format *: ClassVar[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= 'dbf'*

#### \_query(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Execute the year-filtered read against the FERC DBF SQLite database.

### *class* pudl.dagster.io_managers.FercXbrlSqliteIOManager(\*\*data: Any)

Bases: [`FercSqliteIOManagerBase`](#pudl.dagster.io_managers.FercSqliteIOManagerBase)

IO manager for reading tables from a FERC XBRL SQLite database.

Instantiate with `dataset` (`ferc1`, `ferc714`, etc.).

#### data_format *: ClassVar[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= 'xbrl'*

#### *static* refine_report_year(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), xbrl_years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Set a fact’s report year by its actual dates.

Sometimes a fact belongs to a context which has no ReportYear associated with
it; other times there are multiple ReportYears associated with a single filing.
In these cases the report year of a specific fact may be associated with the
other years in the filing.

In many cases we can infer the actual report year from the fact’s associated
time period - either duration or instant.

#### \_query(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Execute the full-table read against the FERC XBRL SQLite database.

* **Parameters:**
  * **table_name** – Name of the table to query (without the `raw_<db_name>__`
    prefix).
  * **years** – Years to include in the result set (passed to
    [`refine_report_year()`](#pudl.dagster.io_managers.FercXbrlSqliteIOManager.refine_report_year)).

### pudl.dagster.io_managers.ferc1_dbf_sqlite_io_manager

### pudl.dagster.io_managers.ferc1_xbrl_sqlite_io_manager

### pudl.dagster.io_managers.ferc714_xbrl_sqlite_io_manager

### pudl.dagster.io_managers.default_io_managers *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]*
