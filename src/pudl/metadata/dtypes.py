"""Mappings of simplified PUDL field types to other tabular data types."""

import datetime
from collections.abc import Callable

import duckdb.sqltypes
import geoarrow.pyarrow as ga
import geopandas
import pandas as pd
import polars as pl
import polars.datatypes as polars_datatypes
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import DATETIME as SQLITE_DATETIME
from sqlalchemy.types import TypeEngine as SATypeEngine

FIELD_DTYPES_POLARS: dict[str, type[pl.DataType] | pl.DataType] = {
    "boolean": polars_datatypes.Boolean,
    "date": polars_datatypes.Date,
    "datetime": polars_datatypes.Datetime(time_unit="ms"),
    "integer": polars_datatypes.Int64,
    "number": polars_datatypes.Float64,
    "string": polars_datatypes.String,
    "year": polars_datatypes.Datetime,
}
"""Polars data type by simplified PUDL field type."""

FIELD_DTYPES_DUCKDB: dict[str, duckdb.sqltypes.DuckDBPyType] = {
    "boolean": duckdb.sqltypes.BOOLEAN,
    "date": duckdb.sqltypes.DATE,
    "datetime": duckdb.sqltypes.TIMESTAMP_MS,
    "integer": duckdb.sqltypes.INTEGER,
    "number": duckdb.sqltypes.DOUBLE,
    "string": duckdb.sqltypes.VARCHAR,
    "year": duckdb.sqltypes.TIMESTAMP_MS,
}
"""DuckDB data type by simplified PUDL field type."""

FIELD_DTYPES_PANDAS: dict[str, str] = {
    "boolean": "boolean",
    "date": "datetime64[s]",
    "datetime": "datetime64[s]",
    "geometry": "geometry",
    "integer": "Int64",
    "number": "float64",
    "string": "string",
    "year": "datetime64[s]",
}
"""Pandas data type by simplified PUDL field type."""

FIELD_DTYPES_PYARROW: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "datetime": pa.timestamp("ms"),
    "geometry": ga.wkb(),
    "integer": pa.int32(),
    "number": pa.float32(),
    "string": pa.string(),
    "year": pa.int32(),
}

FIELD_DTYPES_SQLITE: dict[str, type[SATypeEngine] | SATypeEngine] = {
    "boolean": sa.Boolean,
    "date": sa.Date,
    # Ensure SQLite's string representation of datetime uses only whole seconds:
    "datetime": SQLITE_DATETIME(
        storage_format="%(year)04d-%(month)02d-%(day)02d %(hour)02d:%(minute)02d:%(second)02d"
    ),
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
