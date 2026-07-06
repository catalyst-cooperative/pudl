# pudl.metadata.constants

Metadata and operational constants.

## Attributes

| [`FIELD_DTYPES_POLARS`](#pudl.metadata.constants.FIELD_DTYPES_POLARS)   | Polars data type by simplified PUDL field type.                       |
|-------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`FIELD_DTYPES_DUCKDB`](#pudl.metadata.constants.FIELD_DTYPES_DUCKDB)   | DuckDB data type by simplified PUDL field type.                       |
| [`FIELD_DTYPES_PANDAS`](#pudl.metadata.constants.FIELD_DTYPES_PANDAS)   | Pandas data type by simplified PUDL field type.                       |
| [`FIELD_DTYPES_PYARROW`](#pudl.metadata.constants.FIELD_DTYPES_PYARROW) |                                                                       |
| [`FIELD_DTYPES_SQL`](#pudl.metadata.constants.FIELD_DTYPES_SQL)         | SQLAlchemy column types by simplified PUDL field type.                |
| [`CONSTRAINT_DTYPES`](#pudl.metadata.constants.CONSTRAINT_DTYPES)       | Python types for field constraints by simplified PUDL field type.     |
| [`LICENSES`](#pudl.metadata.constants.LICENSES)                         | License attributes.                                                   |
| [`PERIODS`](#pudl.metadata.constants.PERIODS)                           | Functions converting datetimes to period start times, by time period. |
| [`CONTRIBUTORS`](#pudl.metadata.constants.CONTRIBUTORS)                 | PUDL Contributors for attribution.                                    |
| [`KEYWORDS`](#pudl.metadata.constants.KEYWORDS)                         |                                                                       |
| [`XBRL_TABLES`](#pudl.metadata.constants.XBRL_TABLES)                   | List of all known to be valid FERC Form 1 XBRL tables.                |

## Module Contents

### pudl.metadata.constants.FIELD_DTYPES_POLARS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Polars data type by simplified PUDL field type.

### pudl.metadata.constants.FIELD_DTYPES_DUCKDB *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

DuckDB data type by simplified PUDL field type.

### pudl.metadata.constants.FIELD_DTYPES_PANDAS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Pandas data type by simplified PUDL field type.

### pudl.metadata.constants.FIELD_DTYPES_PYARROW *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pyarrow.DataType](https://arrow.apache.org/docs/python/generated/pyarrow.DataType.html#pyarrow.DataType)]*

### pudl.metadata.constants.FIELD_DTYPES_SQL *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../classes/index.md#pudl.metadata.classes.Field.type)]*

SQLAlchemy column types by simplified PUDL field type.

### pudl.metadata.constants.CONSTRAINT_DTYPES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../classes/index.md#pudl.metadata.classes.Field.type)]*

Python types for field constraints by simplified PUDL field type.

### pudl.metadata.constants.LICENSES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]*

License attributes.

### pudl.metadata.constants.PERIODS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)], [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)]]*

Functions converting datetimes to period start times, by time period.

### pudl.metadata.constants.CONTRIBUTORS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)]*

PUDL Contributors for attribution.

See the Data Package spec [https://specs.frictionlessdata.io/data-package/#metadata](https://specs.frictionlessdata.io/data-package/#metadata)
For `zenodo_role` see the Zenodo documentation
[https://developers.zenodo.org/#representation](https://developers.zenodo.org/#representation).

### pudl.metadata.constants.KEYWORDS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

### pudl.metadata.constants.XBRL_TABLES *= ['corporate_officer_certification_001_duration', 'corporate_officer_certification_001_instant',...*

List of all known to be valid FERC Form 1 XBRL tables.
