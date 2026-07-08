# pudl.metadata.dtypes

Canonical PUDL dtype mappings and dtype-application helpers.

This module serves two related purposes:

1. Define the canonical mapping from PUDL’s simplified field types
   : (`string`, `integer`, `geometry`, etc.) to the concrete dtype objects used
     by supported tabular backends like pandas, Polars, SQLite, DuckDB, and PyArrow.
2. Expose helper functions that resolve those backend dtypes for either the global
   : field metadata or a concrete PUDL resource schema, and apply them to pandas or
     Polars dataframes.

When a concrete `resource` is provided to [`get_pudl_dtypes()`](#pudl.metadata.dtypes.get_pudl_dtypes), the resource
schema is authoritative. That means resource-specific field typing and enum/category
information already encoded in `PUDL_PACKAGE` will be used directly where possible.

Not every backend supports every canonical PUDL field type. In particular, some
backends do not yet support PUDL’s `geometry` fields. In those cases the dtype
helpers intentionally omit unsupported fields rather than returning an incompatible
dtype mapping.

This module intentionally keeps the import of `PUDL_PACKAGE` local to the helper
functions that need it, so the metadata class graph does not introduce a module import
cycle.

## Attributes

| [`FIELD_DTYPES_POLARS`](#pudl.metadata.dtypes.FIELD_DTYPES_POLARS)       | Polars data type by simplified PUDL field type.                       |
|--------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`FIELD_DTYPES_DUCKDB`](#pudl.metadata.dtypes.FIELD_DTYPES_DUCKDB)       | DuckDB data type by simplified PUDL field type.                       |
| [`FIELD_DTYPES_PANDAS`](#pudl.metadata.dtypes.FIELD_DTYPES_PANDAS)       | Pandas data type by simplified PUDL field type.                       |
| [`FIELD_DTYPES_PYARROW`](#pudl.metadata.dtypes.FIELD_DTYPES_PYARROW)     |                                                                       |
| [`FIELD_DTYPES_SQLITE`](#pudl.metadata.dtypes.FIELD_DTYPES_SQLITE)       | SQLAlchemy column types by simplified PUDL field type.                |
| [`CONSTRAINT_DTYPES`](#pudl.metadata.dtypes.CONSTRAINT_DTYPES)           | Python types for field constraints by simplified PUDL field type.     |
| [`PERIODS`](#pudl.metadata.dtypes.PERIODS)                               | Functions converting datetimes to period start times, by time period. |
| [`PudlDtypeBackend`](#pudl.metadata.dtypes.PudlDtypeBackend)             |                                                                       |
| [`_DTYPE_MAPS_BY_BACKEND`](#pudl.metadata.dtypes._DTYPE_MAPS_BY_BACKEND) |                                                                       |

## Functions

| [`_get_applicable_dtypes`](#pudl.metadata.dtypes._get_applicable_dtypes)(→ dict[str, Any])         | Return the subset of resolved dtypes needed to cast the given columns.            |
|----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`get_pudl_dtypes`](#pudl.metadata.dtypes.get_pudl_dtypes)(→ dict[str, Any])                       | Compile a dictionary of field dtypes.                                             |
| [`_get_pudl_namespace_dtypes`](#pudl.metadata.dtypes._get_pudl_namespace_dtypes)(→ dict[str, Any]) | Compile a dictionary of field dtypes based on the namespace-level field metadata. |
| [`_get_pudl_resource_dtypes`](#pudl.metadata.dtypes._get_pudl_resource_dtypes)(→ dict[str, Any])   | Compile a dictionary of field dtypes for a specific PUDL resource.                |
| [`apply_pudl_dtypes`](#pudl.metadata.dtypes.apply_pudl_dtypes)(...)                                | Apply dtypes to those columns in a dataframe that have PUDL types defined.        |
| [`apply_pudl_dtypes_polars`](#pudl.metadata.dtypes.apply_pudl_dtypes_polars)(→ polars.LazyFrame)   | Apply dtypes to those columns in a dataframe that have PUDL types defined.        |

## Module Contents

### pudl.metadata.dtypes.FIELD_DTYPES_POLARS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../classes/index.md#pudl.metadata.classes.Field.type)[polars.DataType] | polars.DataType]*

Polars data type by simplified PUDL field type.

### pudl.metadata.dtypes.FIELD_DTYPES_DUCKDB *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), duckdb.sqltypes.DuckDBPyType]*

DuckDB data type by simplified PUDL field type.

### pudl.metadata.dtypes.FIELD_DTYPES_PANDAS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Pandas data type by simplified PUDL field type.

### pudl.metadata.dtypes.FIELD_DTYPES_PYARROW *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pyarrow.DataType](https://arrow.apache.org/docs/python/generated/pyarrow.DataType.html#pyarrow.DataType)]*

### pudl.metadata.dtypes.FIELD_DTYPES_SQLITE *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../classes/index.md#pudl.metadata.classes.Field.type)[[sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine)] | [sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine)]*

SQLAlchemy column types by simplified PUDL field type.

### pudl.metadata.dtypes.CONSTRAINT_DTYPES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../classes/index.md#pudl.metadata.classes.Field.type)]*

Python types for field constraints by simplified PUDL field type.

### pudl.metadata.dtypes.PERIODS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)], [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series) | [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]]*

Functions converting datetimes to period start times, by time period.

### pudl.metadata.dtypes.PudlDtypeBackend

### pudl.metadata.dtypes.\_DTYPE_MAPS_BY_BACKEND *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[PudlDtypeBackend](#pudl.metadata.dtypes.PudlDtypeBackend), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

### pudl.metadata.dtypes.\_get_applicable_dtypes(columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], field_namespace: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), resource: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), dtype_backend: [PudlDtypeBackend](#pudl.metadata.dtypes.PudlDtypeBackend), strict: [bool](https://docs.python.org/3/library/functions.html#bool)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Return the subset of resolved dtypes needed to cast the given columns.

### pudl.metadata.dtypes.get_pudl_dtypes(field_namespace: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, resource: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, dtype_backend: [PudlDtypeBackend](#pudl.metadata.dtypes.PudlDtypeBackend) = 'pandas') → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Compile a dictionary of field dtypes.

* **Parameters:**
  * **field_namespace** – The field namespace (e.g. ferc1, eia) to use for overriding
    the default field types. If None, no namespace overrides are applied.
  * **resource** – The resource (table) name whose schema should define the field
    types. If provided, resource field types are authoritative.
  * **dtype_backend** – Named dtype backend to compile. Supported values are
    `"pandas"`, `"polars"`, `"sqlite"`, `"duckdb"`, and `"pyarrow"`.
* **Returns:**
  A mapping of PUDL field names to their associated data types.

### pudl.metadata.dtypes.\_get_pudl_namespace_dtypes(field_namespace: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, dtype_backend: [PudlDtypeBackend](#pudl.metadata.dtypes.PudlDtypeBackend) = 'pandas') → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Compile a dictionary of field dtypes based on the namespace-level field metadata.

If no field_namespace is provided, the global PUDL field metadata is used.

* **Parameters:**
  * **field_namespace** – The field namespace (e.g. ferc1, eia) whose schema should
    define the field types. If None, no namespace overrides are applied.
  * **dtype_backend** – Named dtype backend to compile. Supported values are
    `"pandas"`, `"polars"`, `"sqlite"`, `"duckdb"`, and `"pyarrow"`.
* **Returns:**
  A mapping of PUDL field names to their associated data types.

### pudl.metadata.dtypes.\_get_pudl_resource_dtypes(resource: [str](https://docs.python.org/3/library/stdtypes.html#str), dtype_backend: [PudlDtypeBackend](#pudl.metadata.dtypes.PudlDtypeBackend) = 'pandas') → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Compile a dictionary of field dtypes for a specific PUDL resource.

* **Parameters:**
  * **resource** – The resource (table) name whose schema should define the field
    types. If provided, resource field types are authoritative.
  * **dtype_backend** – Named dtype backend to compile. Supported values are
    `"pandas"`, `"polars"`, `"sqlite"`, `"duckdb"`, and `"pyarrow"`.
* **Returns:**
  A mapping of PUDL field names to their associated data types.

### pudl.metadata.dtypes.apply_pudl_dtypes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), field_namespace: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, resource: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, strict: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Apply dtypes to those columns in a dataframe that have PUDL types defined.

* **Parameters:**
  * **df** – The dataframe to apply types to. Not all columns need to have types
    defined in the PUDL metadata unless you pass `strict=True`.
  * **field_namespace** – The field namespace to use for overrides, if any.
  * **resource** – The resource (table) name whose schema should define the field
    types. If provided, resource field types are authoritative.
  * **strict** – whether or not all columns need a corresponding field.
* **Returns:**
  The input dataframe, but with standard PUDL types applied.

### pudl.metadata.dtypes.apply_pudl_dtypes_polars(lf: polars.LazyFrame, field_namespace: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, resource: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, strict: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → polars.LazyFrame

Apply dtypes to those columns in a dataframe that have PUDL types defined.

* **Parameters:**
  * **lf** – The LazyFrame to apply types to. Not all columns need to have types
    defined in the PUDL metadata unless you pass `strict=True`.
  * **field_namespace** – The field namespace to use for overrides, if any.
  * **resource** – The resource (table) name whose schema should define the field
    types. If provided, resource field types are authoritative.
  * **strict** – whether or not all columns need a corresponding field.
* **Returns:**
  The input LazyFrame, but with standard PUDL types applied.
