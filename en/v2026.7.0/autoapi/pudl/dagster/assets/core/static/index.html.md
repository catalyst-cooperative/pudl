# pudl.dagster.assets.core.static

Dagster assets for static reference tables.

This module defines assets that materialize small, stable lookup tables and other
reference data that PUDL ships as part of the pipeline itself. Put asset definitions
here when the data comes from packaged metadata or code-maintained constants rather
than an external extract step, and keep source-specific extract logic elsewhere.

## Attributes

| [`logger`](#pudl.dagster.assets.core.static.logger)   |    |
|-------------------------------------------------------|----|

## Functions

| [`_read_static_encoding_tables`](#pudl.dagster.assets.core.static._read_static_encoding_tables)(→ dict[str, pandas.DataFrame])   | Build dataframes of static tables from a data source for use as foreign keys.   |
|----------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`static_pudl_tables`](#pudl.dagster.assets.core.static.static_pudl_tables)(context)                                             | Read static tables compiled as part of PUDL and not from any agency dataset.    |
| [`static_eia_tables`](#pudl.dagster.assets.core.static.static_eia_tables)(context)                                               | Create static EIA tables.                                                       |
| [`static_ferc1_tables`](#pudl.dagster.assets.core.static.static_ferc1_tables)(context)                                           | Compile static tables for FERC1 for foreign key constraints.                    |
| [`static_rus_tables`](#pudl.dagster.assets.core.static.static_rus_tables)(context)                                               | Create static RUS tables.                                                       |

## Module Contents

### pudl.dagster.assets.core.static.logger

### pudl.dagster.assets.core.static.\_read_static_encoding_tables(etl_group: Literal['static_eia', 'static_ferc1', 'static_rus']) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Build dataframes of static tables from a data source for use as foreign keys.

There are many values specified within the data that are essentially constant, but
which we need to store for data validation purposes, for use as foreign keys.  E.g.
the list of valid EIA fuel type codes, or the possible state and country codes
indicating a coal delivery’s location of origin. For now these values are primarily
stored in a large collection of lists, dictionaries, and dataframes which are
specified in the [`pudl.metadata`](../../../../metadata/index.md#module-pudl.metadata) subpackage.  This function uses those data
structures to populate a bunch of small infrastructural tables within the PUDL DB.

* **Parameters:**
  **etl_group** – name of static table etl group.
* **Returns:**
  a dictionary with table names as keys and dataframes as values for all tables
  labeled as static tables in their resource `etl_group`

### pudl.dagster.assets.core.static.static_pudl_tables(context)

Read static tables compiled as part of PUDL and not from any agency dataset.

### pudl.dagster.assets.core.static.static_eia_tables(context)

Create static EIA tables.

### pudl.dagster.assets.core.static.static_ferc1_tables(context)

Compile static tables for FERC1 for foreign key constraints.

This function grabs static encoded tables via [`_read_static_encoding_tables()`](#pudl.dagster.assets.core.static._read_static_encoding_tables)
as well as two static tables that are non-encoded tables (`ferc_accounts`).

### pudl.dagster.assets.core.static.static_rus_tables(context)

Create static RUS tables.
