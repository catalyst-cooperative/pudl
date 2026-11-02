# pudl.scripts.generate_pudl_duckdb

Generate a DuckDB database of views onto Parquet files for each datapackage.

Each registered datapackage gets its own standalone database, named
`<package>_views.duckdb` (e.g. `pudl_views.duckdb`). The `_views` suffix
distinguishes these from databases that hold the data itself, like `pudl.duckdb`. Every
table is exposed as a view reading the Parquet files directly, in a schema named for
where the files live (`local`, `nightly`, `stable`, `published`). Nothing is
copied.

## Attributes

| [`REMOTE_BASE_PATHS`](#pudl.scripts.generate_pudl_duckdb.REMOTE_BASE_PATHS)   | Remote locations of each package's Parquet files, keyed by package then schema.   |
|----------------------------------------------------------------------|-----------------------------------------------------------------------------------|

## Functions

| [`_view_location`](#pudl.scripts.generate_pudl_duckdb._view_location)(resource, base_path) → tuple[str, str]   | Return what a resource's view should read, and the path to check for existence.   |
|----------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`create_views`](#pudl.scripts.generate_pudl_duckdb.create_views)(conn, package, base_paths) → list[str]     | Create a view for each of a package's resources in each of its schemas.           |
| [`main`](#pudl.scripts.generate_pudl_duckdb.main)() → int                                            | Create a DuckDB database of views onto Parquet files for each datapackage.        |

## Module Contents

### pudl.scripts.generate_pudl_duckdb.REMOTE_BASE_PATHS *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]]*

Remote locations of each package’s Parquet files, keyed by package then schema.

A `local` schema pointing at `$PUDL_OUTPUT/parquet` is always added. A package with
no entry here only gets the `local` schema.

### pudl.scripts.generate_pudl_duckdb.\_view_location(resource: [pudl.metadata.classes.Resource](../../metadata/classes/index.md#pudl.metadata.classes.Resource), base_path: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Return what a resource’s view should read, and the path to check for existence.

The layout comes from the resource’s own declared `path`, not from the package it
belongs to. A path with a directory component is a directory of partition files
and is read with a glob; a bare file name is a single Parquet file.

* **Returns:**
  The file or glob to select from, and the file or directory that must exist.

### pudl.scripts.generate_pudl_duckdb.create_views(conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection), package: [pudl.metadata.classes.Package](../../metadata/classes/index.md#pudl.metadata.classes.Package), base_paths: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Create a view for each of a package’s resources in each of its schemas.

* **Parameters:**
  * **conn** – Connection to the database to create the views in.
  * **package** – Datapackage whose resources should be exposed as views.
  * **base_paths** – Where the package’s Parquet files live, keyed by schema name.
* **Returns:**
  The `schema.table` name of every view created. A resource whose files don’t
  exist at a location is skipped there, since local outputs are often incomplete.

### pudl.scripts.generate_pudl_duckdb.main() → [int](https://docs.python.org/3/builtins/functions.html#int)

Create a DuckDB database of views onto Parquet files for each datapackage.
