"""Canonical PUDL dtype mappings and dtype-application helpers.

This module serves two related purposes:

1. Define the canonical mapping from PUDL's simplified field types
    (``string``, ``integer``, ``geometry``, etc.) to the concrete dtype objects used
    by supported tabular backends like pandas, Polars, SQLite, DuckDB, and PyArrow.
2. Expose helper functions that resolve those backend dtypes for either the global
    field metadata or a concrete PUDL resource schema, and apply them to pandas or
    Polars dataframes.

When a concrete ``resource`` is provided to :func:`get_pudl_dtypes`, the resource
schema is authoritative. That means resource-specific field typing and enum/category
information already encoded in ``PUDL_PACKAGE`` will be used directly where possible.

Not every backend supports every canonical PUDL field type. In particular, some
backends do not yet support PUDL's ``geometry`` fields. In those cases the dtype
helpers intentionally omit unsupported fields rather than returning an incompatible
dtype mapping.

This module intentionally keeps the import of ``PUDL_PACKAGE`` local to the helper
functions that need it, so the metadata class graph does not introduce a module import
cycle.
"""

import datetime
from collections.abc import Callable
from copy import deepcopy
from typing import Any, Literal

import duckdb.sqltypes
import geoarrow.pyarrow as geoarrow
import geopandas
import pandas as pd
import polars as pl
import polars.datatypes as polars_datatypes
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import DATETIME as SQLITE_DATETIME
from sqlalchemy.types import TypeEngine as SATypeEngine

from pudl.metadata.fields import FIELD_METADATA, FIELD_METADATA_BY_NAMESPACE

FIELD_DTYPES_POLARS: dict[str, type[pl.DataType] | pl.DataType] = {
    "boolean": polars_datatypes.Boolean,
    "date": polars_datatypes.Date,
    "datetime": polars_datatypes.Datetime(time_unit="us"),
    "integer": polars_datatypes.Int64,
    "number": polars_datatypes.Float64,
    "string": polars_datatypes.String,
    "year": polars_datatypes.Datetime(time_unit="us"),
}
"""Polars data type by simplified PUDL field type."""

FIELD_DTYPES_DUCKDB: dict[str, duckdb.sqltypes.DuckDBPyType] = {
    "boolean": duckdb.sqltypes.BOOLEAN,
    "date": duckdb.sqltypes.DATE,
    "datetime": duckdb.sqltypes.TIMESTAMP,
    "integer": duckdb.sqltypes.BIGINT,
    "number": duckdb.sqltypes.DOUBLE,
    "string": duckdb.sqltypes.VARCHAR,
    "year": duckdb.sqltypes.TIMESTAMP,
}
"""DuckDB data type by simplified PUDL field type."""

FIELD_DTYPES_PANDAS: dict[str, str] = {
    "boolean": "boolean",
    "date": "datetime64[us]",
    "datetime": "datetime64[us]",
    "geometry": "geometry",
    "integer": "Int64",
    "number": "float64",
    "string": "string",
    "year": "datetime64[us]",
}
"""Pandas data type by simplified PUDL field type."""

FIELD_DTYPES_PYARROW: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "geometry": geoarrow.wkb(),
    "integer": pa.int64(),
    "number": pa.float64(),
    "string": pa.string(),
    "year": pa.int64(),
}
"""Pyarrow data type by simplified PUDL field type."""

FIELD_DTYPES_SQLITE: dict[str, type[SATypeEngine] | SATypeEngine] = {
    "boolean": sa.Boolean,
    "date": sa.Date,
    "datetime": SQLITE_DATETIME(),
    "integer": sa.Integer,
    "number": sa.Float,
    "string": sa.Text,
    "year": sa.Integer,
}
"""SQLAlchemy column types by simplified PUDL field type."""

CONSTRAINT_DTYPES: dict[str, type] = {
    "boolean": bool,
    "date": datetime.date,
    "datetime": datetime.datetime,
    "geometry": geopandas.array.GeometryDtype,
    "integer": int,
    "number": float,
    "string": str,
    "year": int,
}
"""Python types for field constraints by simplified PUDL field type."""

PERIODS: dict[str, Callable[[pd.Series], pd.Series | pd.DataFrame]] = {
    "year": lambda x: pd.Series(x.to_numpy().astype("datetime64[Y]")),
    "quarter": lambda x: x.apply(
        pd.tseries.offsets.QuarterBegin(startingMonth=1).rollback
    ),
    "month": lambda x: pd.Series(x.to_numpy().astype("datetime64[M]")),
    "date": lambda x: pd.Series(x.to_numpy().astype("datetime64[D]")),
}
"""Functions converting datetimes to period start times, by time period."""

PudlDtypeBackend = Literal["pandas", "polars", "sqlite", "duckdb", "pyarrow"]

_DTYPE_MAPS_BY_BACKEND: dict[PudlDtypeBackend, dict[str, Any]] = {
    "pandas": FIELD_DTYPES_PANDAS,
    "polars": FIELD_DTYPES_POLARS,
    "sqlite": FIELD_DTYPES_SQLITE,
    "duckdb": FIELD_DTYPES_DUCKDB,
    "pyarrow": FIELD_DTYPES_PYARROW,
}


def _get_applicable_dtypes(
    columns: list[str],
    field_namespace: str | None,
    resource: str | None,
    dtype_backend: PudlDtypeBackend,
    strict: bool,
) -> dict[str, Any]:
    """Return the subset of resolved dtypes needed to cast the given columns."""
    dtypes = get_pudl_dtypes(
        field_namespace=field_namespace,
        resource=resource,
        dtype_backend=dtype_backend,
    )

    if resource is not None:
        known_fields = set(dtypes.keys())
    else:
        field_namespace_overrides = (
            FIELD_METADATA_BY_NAMESPACE.get(field_namespace, {})
            if field_namespace is not None
            else {}
        )
        known_fields = set(FIELD_METADATA.keys()) | set(
            field_namespace_overrides.keys()
        )

    unspecified_fields = sorted(set(columns) - known_fields)
    if strict and len(unspecified_fields) > 0:
        raise ValueError(f"Found unspecified fields: {unspecified_fields}")
    return {column: dtypes[column] for column in columns if column in dtypes}


def get_pudl_dtypes(
    field_namespace: str | None = None,
    resource: str | None = None,
    dtype_backend: PudlDtypeBackend = "pandas",
) -> dict[str, Any]:
    """Compile a dictionary of field dtypes.

    Args:
        field_namespace: The field namespace (e.g. ferc1, eia) to use for overriding
            the default field types. If None, no namespace overrides are applied.
        resource: The resource (table) name whose schema should define the field
            types. If provided, resource field types are authoritative.
        dtype_backend: Named dtype backend to compile. Supported values are
            ``"pandas"``, ``"polars"``, ``"sqlite"``, ``"duckdb"``, and ``"pyarrow"``.

    Returns:
        A mapping of PUDL field names to their associated data types.
    """
    if (field_namespace is not None) and (resource is not None):
        raise ValueError(
            "field_namespace and resource are mutually exclusive when selecting "
            "PUDL dtypes. Suggestion: if you know the resource name use only "
            "that. resource is more authoritative."
        )

    if resource is not None:
        dtypes = _get_pudl_resource_dtypes(
            resource=resource, dtype_backend=dtype_backend
        )
    else:
        # This also covers the generic case in which field_namespace is None
        dtypes = _get_pudl_namespace_dtypes(
            field_namespace=field_namespace, dtype_backend=dtype_backend
        )

    return dtypes


def _get_pudl_namespace_dtypes(
    field_namespace: str | None = None,
    dtype_backend: PudlDtypeBackend = "pandas",
) -> dict[str, Any]:
    """Compile a dictionary of field dtypes based on the namespace-level field metadata.

    If no field_namespace is provided, the global PUDL field metadata is used.

    Args:
        field_namespace: The field namespace (e.g. ferc1, eia) whose schema should
            define the field types. If None, no namespace overrides are applied.
        dtype_backend: Named dtype backend to compile. Supported values are
            ``"pandas"``, ``"polars"``, ``"sqlite"``, ``"duckdb"``, and ``"pyarrow"``.

    Returns:
        A mapping of PUDL field names to their associated data types.
    """
    # This is here to avoid circular imports.
    from pudl.metadata.classes import FIELD_NAMESPACES

    if (field_namespace is not None) and (field_namespace not in FIELD_NAMESPACES):
        raise ValueError(
            f"Unknown PUDL field namespace: {field_namespace!r}. "
            f"Valid namespaces are: {list(FIELD_NAMESPACES)}"
        )
    dtype_map = _DTYPE_MAPS_BY_BACKEND[dtype_backend]
    field_meta = deepcopy(FIELD_METADATA)
    field_namespace_overrides = (
        FIELD_METADATA_BY_NAMESPACE.get(field_namespace, {})
        if field_namespace is not None
        else {}
    )
    dtypes = {}
    for field_name in field_meta:
        if field_name in field_namespace_overrides:
            field_meta[field_name].update(field_namespace_overrides[field_name])
        if field_meta[field_name]["type"] in dtype_map:
            dtypes[field_name] = dtype_map[field_meta[field_name]["type"]]
    return dtypes


def _get_pudl_resource_dtypes(
    resource: str,
    dtype_backend: PudlDtypeBackend = "pandas",
) -> dict[str, Any]:
    """Compile a dictionary of field dtypes for a specific PUDL resource.

    Args:
        resource: The resource (table) name whose schema should define the field
            types. If provided, resource field types are authoritative.
        dtype_backend: Named dtype backend to compile. Supported values are
            ``"pandas"``, ``"polars"``, ``"sqlite"``, ``"duckdb"``, and ``"pyarrow"``.

    Returns:
        A mapping of PUDL field names to their associated data types.
    """
    # This is here to avoid circular imports.
    from pudl.metadata.classes import PUDL_PACKAGE

    # We need to build up resource-specific dtypes
    resource_metadata = PUDL_PACKAGE.get_resource(resource)
    if dtype_backend == "pandas":
        # Pandas is easy, because it defines all dtypes and we have a
        # resource-level helper method to get them all at once.
        dtypes = resource_metadata.to_pandas_dtypes()
    elif dtype_backend == "pyarrow":
        # pyarrow has the geometry dtype, but no resource-level helper method.
        dtypes = {
            field.name: field.to_pyarrow_dtype()
            for field in resource_metadata.schema.fields
        }
    elif dtype_backend == "polars":
        # For polars we need to build a mapping of only the fields which have
        # dtypes defined, because it is missing the geometry dtype.
        dtypes = {
            field.name: field.to_polars_dtype()
            for field in resource_metadata.schema.fields
            if field.type in FIELD_DTYPES_POLARS
        }
    elif dtype_backend == "sqlite":
        # Similarly SQLite is also missing the geometry dtype.
        dtypes = {
            field.name: field.to_sqlite_dtype()
            for field in resource_metadata.schema.fields
            if field.type in FIELD_DTYPES_SQLITE
        }
    else:
        assert dtype_backend == "duckdb", f"Unknown dtype backend: {dtype_backend}"
        # We can't do the same field specific typing for DuckDB because it requires
        # a database connection to define a custom ENUM type, so it just gets the
        # more generic mapping
        dtype_map = _DTYPE_MAPS_BY_BACKEND[dtype_backend]
        dtypes = {
            field.name: dtype_map[field.type]
            for field in resource_metadata.schema.fields
            if field.type in dtype_map
        }
    return dtypes


def apply_pudl_dtypes(
    df: pd.DataFrame | geopandas.GeoDataFrame,
    field_namespace: str | None = None,
    resource: str | None = None,
    strict: bool = False,
) -> pd.DataFrame | geopandas.GeoDataFrame:
    """Apply dtypes to those columns in a dataframe that have PUDL types defined.

    Args:
        df: The dataframe to apply types to. Not all columns need to have types
            defined in the PUDL metadata unless you pass ``strict=True``.
        field_namespace: The field namespace to use for overrides, if any.
        resource: The resource (table) name whose schema should define the field
            types. If provided, resource field types are authoritative.
        strict: whether or not all columns need a corresponding field.

    Returns:
        The input dataframe, but with standard PUDL types applied.
    """
    dtypes = _get_applicable_dtypes(
        columns=df.columns.tolist(),
        field_namespace=field_namespace,
        resource=resource,
        dtype_backend="pandas",
        strict=strict,
    )
    return df.astype({col: dtypes[col] for col in df.columns if col in dtypes})


def apply_pudl_dtypes_polars(
    lf: pl.LazyFrame,
    field_namespace: str | None = None,
    resource: str | None = None,
    strict: bool = False,
) -> pl.LazyFrame:
    """Apply dtypes to those columns in a dataframe that have PUDL types defined.

    Args:
        lf: The LazyFrame to apply types to. Not all columns need to have types
            defined in the PUDL metadata unless you pass ``strict=True``.
        field_namespace: The field namespace to use for overrides, if any.
        resource: The resource (table) name whose schema should define the field
            types. If provided, resource field types are authoritative.
        strict: whether or not all columns need a corresponding field.

    Returns:
        The input LazyFrame, but with standard PUDL types applied.
    """
    columns = lf.collect_schema().names()
    dtypes = _get_applicable_dtypes(
        columns=columns,
        field_namespace=field_namespace,
        resource=resource,
        dtype_backend="polars",
        strict=strict,
    )
    return lf.cast({key: value for key, value in dtypes.items() if key in columns})
