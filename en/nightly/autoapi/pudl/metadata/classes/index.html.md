# pudl.metadata.classes

Metadata data classes.

## Attributes

| [`logger`](#pudl.metadata.classes.logger)                     |                                                                                                                       |
|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| [`String`](#pudl.metadata.classes.String)                     | Non-empty [`str`](https://docs.python.org/3/library/stdtypes.html#str) with no trailing or leading whitespace.        |
| [`SnakeCase`](#pudl.metadata.classes.SnakeCase)               | Snake-case variable name [`str`](https://docs.python.org/3/library/stdtypes.html#str) (e.g. 'pudl', 'entity_eia860'). |
| [`PositiveInt`](#pudl.metadata.classes.PositiveInt)           | Positive [`int`](https://docs.python.org/3/library/functions.html#int).                                               |
| [`PositiveFloat`](#pudl.metadata.classes.PositiveFloat)       | Positive [`float`](https://docs.python.org/3/library/functions.html#float).                                           |
| [`T`](#pudl.metadata.classes.T)                               |                                                                                                                       |
| [`StrictList`](#pudl.metadata.classes.StrictList)             | Non-empty [`list`](https://docs.python.org/3/library/stdtypes.html#list).                                             |
| [`FieldNamespace`](#pudl.metadata.classes.FieldNamespace)     | Canonical field namespace identifiers used by PUDL resources.                                                         |
| [`FIELD_NAMESPACES`](#pudl.metadata.classes.FIELD_NAMESPACES) | All valid PUDL field namespace identifiers.                                                                           |
| [`EtlGroup`](#pudl.metadata.classes.EtlGroup)                 | Canonical ETL group identifiers used by PUDL resources.                                                               |
| [`ETL_GROUPS`](#pudl.metadata.classes.ETL_GROUPS)             | All valid PUDL ETL group identifiers.                                                                                 |
| [`PUDL_PACKAGE`](#pudl.metadata.classes.PUDL_PACKAGE)         | Define a global PUDL package object for use across the entire codebase.                                               |

## Classes

| [`PudlMeta`](#pudl.metadata.classes.PudlMeta)                             | A base model that configures some options for PUDL metadata classes.    |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`FieldConstraints`](#pudl.metadata.classes.FieldConstraints)             | Field constraints (resource.schema.fields[...].constraints).            |
| [`FieldHarvest`](#pudl.metadata.classes.FieldHarvest)                     | Field harvest parameters (resource.schema.fields[...].harvest).         |
| [`Encoder`](#pudl.metadata.classes.Encoder)                               | A class that allows us to standardize reported categorical codes.       |
| [`Field`](#pudl.metadata.classes.Field)                                   | Field (resource.schema.fields[...]).                                    |
| [`ForeignKeyReference`](#pudl.metadata.classes.ForeignKeyReference)       | Foreign key reference (resource.schema.foreign_keys[...].reference).    |
| [`ForeignKey`](#pudl.metadata.classes.ForeignKey)                         | Foreign key (resource.schema.foreign_keys[...]).                        |
| [`Schema`](#pudl.metadata.classes.Schema)                                 | Table schema (resource.schema).                                         |
| [`License`](#pudl.metadata.classes.License)                               | Data license (package|resource.licenses[...]).                          |
| [`Contributor`](#pudl.metadata.classes.Contributor)                       | Data contributor (package.contributors[...]).                           |
| [`DataSource`](#pudl.metadata.classes.DataSource)                         | A data source that has been integrated into PUDL.                       |
| [`ResourceHarvest`](#pudl.metadata.classes.ResourceHarvest)               | Resource harvest parameters (resource.harvest).                         |
| [`PudlResourceDescriptor`](#pudl.metadata.classes.PudlResourceDescriptor) | The form we expect the RESOURCE_METADATA elements to take.              |
| [`Resource`](#pudl.metadata.classes.Resource)                             | Tabular data resource (package.resources[...]).                         |
| [`Package`](#pudl.metadata.classes.Package)                               | Tabular data package.                                                   |
| [`CodeMetadata`](#pudl.metadata.classes.CodeMetadata)                     | A list of Encoders for standardizing and documenting categorical codes. |

## Functions

| [`_unique`](#pudl.metadata.classes._unique)(→ list)                                       | Return a list of all unique values, in order of first appearance.   |
|-------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`_format_for_sql`](#pudl.metadata.classes._format_for_sql)(→ str)                        | Format value for use in raw SQL(ite).                               |
| [`_get_jinja_environment`](#pudl.metadata.classes._get_jinja_environment)([template_dir]) |                                                                     |
| [`_check_unique`](#pudl.metadata.classes._check_unique)(→ list | None)                    | Check that input list has unique values.                            |
| [`_validator`](#pudl.metadata.classes._validator)(→ collections.abc.Callable)             | Construct reusable Pydantic validator.                              |

## Module Contents

### pudl.metadata.classes.logger

### pudl.metadata.classes.\_unique(\*args: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)) → [list](https://docs.python.org/3/library/stdtypes.html#list)

Return a list of all unique values, in order of first appearance.

* **Parameters:**
  **args** – Iterables of values.

### Examples

```pycon
>>> _unique([0, 2], (2, 1))
[0, 2, 1]
>>> _unique([{'x': 0, 'y': 1}, {'y': 1, 'x': 0}], [{'z': 2}])
[{'x': 0, 'y': 1}, {'z': 2}]
```

### pudl.metadata.classes.\_format_for_sql(x: Any, identifier: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Format value for use in raw SQL(ite).

* **Parameters:**
  * **x** – Value to format.
  * **identifier** – Whether `x` represents an identifier
    (e.g. table, column) name.

### Examples

```pycon
>>> _format_for_sql('table_name', identifier=True)
'"table_name"'
>>> _format_for_sql('any string')
"'any string'"
>>> _format_for_sql("Single's quote")
"'Single''s quote'"
>>> _format_for_sql(None)
'null'
>>> _format_for_sql(1)
'1'
>>> _format_for_sql(True)
'True'
>>> _format_for_sql(False)
'False'
>>> _format_for_sql(re.compile("^[^']*$"))
"'^[^'']*$'"
>>> _format_for_sql(datetime.date(2020, 1, 2))
"'2020-01-02'"
>>> _format_for_sql(datetime.datetime(2020, 1, 2, 3, 4, 5, 6))
"'2020-01-02 03:04:05'"
```

### pudl.metadata.classes.\_get_jinja_environment(template_dir: pydantic.DirectoryPath = None)

### pudl.metadata.classes.String

Non-empty [`str`](https://docs.python.org/3/library/stdtypes.html#str) with no trailing or leading whitespace.

### pudl.metadata.classes.SnakeCase

Snake-case variable name [`str`](https://docs.python.org/3/library/stdtypes.html#str) (e.g. ‘pudl’, ‘entity_eia860’).

### pudl.metadata.classes.PositiveInt

Positive [`int`](https://docs.python.org/3/library/functions.html#int).

### pudl.metadata.classes.PositiveFloat

Positive [`float`](https://docs.python.org/3/library/functions.html#float).

### pudl.metadata.classes.T

### pudl.metadata.classes.StrictList

Non-empty [`list`](https://docs.python.org/3/library/stdtypes.html#list).

Allows [`list`](https://docs.python.org/3/library/stdtypes.html#list), [`tuple`](https://docs.python.org/3/library/stdtypes.html#tuple), [`set`](https://docs.python.org/3/library/stdtypes.html#set), [`frozenset`](https://docs.python.org/3/library/stdtypes.html#frozenset),
[`collections.deque`](https://docs.python.org/3/library/collections.html#collections.deque), or generators and casts to a [`list`](https://docs.python.org/3/library/stdtypes.html#list).

### pudl.metadata.classes.FieldNamespace

Canonical field namespace identifiers used by PUDL resources.

### pudl.metadata.classes.FIELD_NAMESPACES *: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[FieldNamespace](#pudl.metadata.classes.FieldNamespace), Ellipsis]*

All valid PUDL field namespace identifiers.

### pudl.metadata.classes.EtlGroup

Canonical ETL group identifiers used by PUDL resources.

### pudl.metadata.classes.ETL_GROUPS *: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[EtlGroup](#pudl.metadata.classes.EtlGroup), Ellipsis]*

All valid PUDL ETL group identifiers.

### pudl.metadata.classes.\_check_unique(value: [list](https://docs.python.org/3/library/stdtypes.html#list) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list) | [None](https://docs.python.org/3/library/constants.html#None)

Check that input list has unique values.

### pudl.metadata.classes.\_validator(\*names, fn: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)) → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

Construct reusable Pydantic validator.

* **Parameters:**
  * **names** – Names of attributes to validate.
  * **fn** – Validation function (see `pydantic.field_validator()`).

### Examples

```pycon
>>> class Class(BaseModel):
...     x: list = None
...     _check_unique = _validator("x", fn=_check_unique)
>>> Class(x=[0, 0])
Traceback (most recent call last):
ValidationError: ...
```

### *class* pudl.metadata.classes.PudlMeta(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

A base model that configures some options for PUDL metadata classes.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

### *class* pudl.metadata.classes.FieldConstraints(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Field constraints (resource.schema.fields[…].constraints).

See [https://specs.frictionlessdata.io/table-schema/#constraints](https://specs.frictionlessdata.io/table-schema/#constraints).

#### required *: pydantic.StrictBool* *= False*

#### unique *: pydantic.StrictBool* *= False*

#### min_length *: [PositiveInt](#pudl.metadata.classes.PositiveInt) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### max_length *: [PositiveInt](#pudl.metadata.classes.PositiveInt) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### minimum *: pydantic.StrictInt | pydantic.StrictFloat | [datetime.date](https://docs.python.org/3/library/datetime.html#datetime.date) | [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### maximum *: pydantic.StrictInt | pydantic.StrictFloat | [datetime.date](https://docs.python.org/3/library/datetime.html#datetime.date) | [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### pattern *: [re.Pattern](https://docs.python.org/3/library/re.html#re.Pattern) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### enum *: [StrictList](#pudl.metadata.classes.StrictList)[[String](#pudl.metadata.classes.String) | pydantic.StrictInt | pydantic.StrictFloat | pydantic.StrictBool | [datetime.date](https://docs.python.org/3/library/datetime.html#datetime.date) | [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### \_check_unique

#### *classmethod* \_check_max_length(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### *classmethod* \_check_max(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### to_pandera_checks(use_pandas_backend: [bool](https://docs.python.org/3/library/functions.html#bool)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[pandera.polars.Check]

Convert these constraints to pandera Column checks.

### *class* pudl.metadata.classes.FieldHarvest(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Field harvest parameters (resource.schema.fields[…].harvest).

#### aggregate *: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)], [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)]* *= None*

Computes a single value from all field values in a group.

#### tolerance *: [PositiveFloat](#pudl.metadata.classes.PositiveFloat)* *= 0.0*

Fraction of invalid groups above which result is considered invalid.

### *class* pudl.metadata.classes.Encoder(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

A class that allows us to standardize reported categorical codes.

Often the original data we are integrating uses short codes to indicate a
categorical value, like `ST` in place of “steam turbine” or `LIG` in place of
“lignite coal”. Many of these coded fields contain non-standard codes due to
data-entry errors. The codes have also evolved over the years.

In order to allow easy comparison of records across all years and tables, we define
a standard set of codes, a mapping from non-standard codes to standard codes (where
possible), and a set of known but unfixable codes which will be ignored and replaced
with NA values. These definitions can be found in [`pudl.metadata.codes`](../codes/index.md#module-pudl.metadata.codes) and we
refer to these as coding tables.

In our metadata structures, each coding table is defined just like any other DB
table, with the addition of an associated `Encoder` object defining the standard,
fixable, and ignored codes.

In addition, a [`Package`](#pudl.metadata.classes.Package) class that has been instantiated using the
[`Package.from_resource_ids()`](#pudl.metadata.classes.Package.from_resource_ids) method will associate an `Encoder` object with any
column that has a foreign key constraint referring to a coding table (This
column-level encoder is same as the encoder associated with the referenced table).
This `Encoder` can be used to standardize the codes found within the column.

[`Field`](#pudl.metadata.classes.Field) and [`Resource`](#pudl.metadata.classes.Resource) objects have `encode()` methods that will
use the column-level encoders to recode the original values, either for a single
column or for all coded columns within a Resource, given either a corresponding
[`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series) or [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) containing actual values.

If any unrecognized values are encountered, an exception will be raised, alerting
us that a new code has been identified, and needs to be classified as fixable or
to be ignored.

#### df *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

A table associating short codes with long descriptions and other information.

Each coding table contains at least a `code` column containing the standard codes
and a `description` column with a human readable explanation of what the code
stands for. Additional metadata pertaining to the codes and their categories may
also appear in this dataframe, which will be loaded into the PUDL DB as a static
table. The `code` column is a natural primary key and must contain no duplicate
values.

#### ignored_codes *: [list](https://docs.python.org/3/library/stdtypes.html#list)[pydantic.StrictInt | [str](https://docs.python.org/3/library/stdtypes.html#str)]* *= []*

A list of non-standard codes which appear in the data, and will be set to NA.

These codes may be the result of data entry errors, and we are unable to map them to
the appropriate canonical code. They are discarded from the raw input data.

#### code_fixes *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[pydantic.StrictInt | [String](#pudl.metadata.classes.String), pydantic.StrictInt | [String](#pudl.metadata.classes.String)]*

A dictionary mapping non-standard codes to canonical, standardized codes.

The intended meanings of some non-standard codes are clear, and therefore they can
be mapped to the standardized, canonical codes with confidence. Sometimes these are
the result of data entry errors or changes in the standard codes over time.

#### name *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

The name of the code.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### *classmethod* \_df_is_encoding_table(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Verify that the coding table provides both codes and descriptions.

#### *classmethod* \_good_and_ignored_codes_are_disjoint(ignored_codes, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Check that there’s no overlap between good and ignored codes.

#### *classmethod* \_good_and_fixable_codes_are_disjoint(code_fixes, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Check that there’s no overlap between the good and fixable codes.

#### *classmethod* \_fixable_and_ignored_codes_are_disjoint(code_fixes, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Check that there’s no overlap between the ignored and fixable codes.

#### *classmethod* \_check_fixed_codes_are_good_codes(code_fixes, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Check that every fixed code is also one of the good codes.

#### *property* code_map *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | pandas._libs.missing.NAType]*

A mapping of all known codes to their standardized values, or NA.

#### encode(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), dtype: [type](#pudl.metadata.classes.Field.type) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Apply the stored code mapping to an input Series.

#### *static* dict_from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Look up the encoder by coding table name in the metadata.

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Encoder](#pudl.metadata.classes.Encoder)

Construct an Encoder based on `Resource.name` of a coding table.

#### *classmethod* from_code_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Encoder](#pudl.metadata.classes.Encoder)

Construct an Encoder by looking up name of coding table in codes metadata.

#### to_rst(top_dir: pydantic.DirectoryPath, csv_subdir: pydantic.DirectoryPath, is_header: pydantic.StrictBool) → [String](#pudl.metadata.classes.String)

Output dataframe to a csv for use in jinja template.

Then output to an RST file.

#### generate_encodable_data(size: [int](https://docs.python.org/3/library/functions.html#int) = 10) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Produce a series of data which can be encoded by this encoder.

Selects values randomly from valid, ignored, and fixable codes.

### *class* pudl.metadata.classes.Field(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Field (resource.schema.fields[…]).

See [https://specs.frictionlessdata.io/table-schema/#field-descriptors](https://specs.frictionlessdata.io/table-schema/#field-descriptors).

### Examples

```pycon
>>> field = Field(name='x', type='string', description='X', constraints={'enum': ['x', 'y']})
>>> field.to_pandas_dtype()
CategoricalDtype(categories=['x', 'y'], ordered=False, categories_dtype=object)
>>> field.to_sql()
Column('x', Enum('x', 'y'), CheckConstraint(...), table=None, comment='X')
>>> field = Field.from_id('utility_id_eia')
>>> field.name
'utility_id_eia'
```

#### name *: [SnakeCase](#pudl.metadata.classes.SnakeCase)*

#### type *: Literal['string', 'number', 'integer', 'boolean', 'date', 'datetime', 'year', 'geometry']*

#### title *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### format_ *: Literal['default']* *= None*

#### description *: [String](#pudl.metadata.classes.String)*

#### unit *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### constraints *: [FieldConstraints](#pudl.metadata.classes.FieldConstraints)*

#### harvest *: [FieldHarvest](#pudl.metadata.classes.FieldHarvest)*

#### encoder *: [Encoder](#pudl.metadata.classes.Encoder) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* \_check_constraints(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### *classmethod* \_check_encoder(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### *static* dict_from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Construct dictionary from PUDL identifier (Field.name).

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Field](#pudl.metadata.classes.Field)

Construct from PUDL identifier (Field.name).

#### to_duckdb_dtype(conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → duckdb.sqltypes.DuckDBPyType

Return duckdb data type.

#### to_polars_dtype() → polars.DataType

Return polars data type.

#### to_pandas_dtype(compact: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [pandas.CategoricalDtype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.CategoricalDtype.html#pandas.CategoricalDtype)

Return Pandas data type.

* **Parameters:**
  **compact** – Whether to return a low-memory data type (32-bit integer or float).

#### to_sql_dtype() → [type](#pudl.metadata.classes.Field.type)

Return SQLAlchemy data type.

#### to_pyarrow_dtype() → [pyarrow.DataType](https://arrow.apache.org/docs/python/generated/pyarrow.DataType.html#pyarrow.DataType)

Return PyArrow data type.

#### to_pyarrow() → [pyarrow.Field](https://arrow.apache.org/docs/python/generated/pyarrow.Field.html#pyarrow.Field)

Return a PyArrow Field appropriate to the field.

#### to_sql(dialect: Literal['sqlite'] = 'sqlite', check_types: [bool](https://docs.python.org/3/library/functions.html#bool) = True, check_values: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → sqlalchemy.Column

Return equivalent SQL column.

#### encode(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), dtype: [type](#pudl.metadata.classes.Field.type) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Recode the Field if it has an associated encoder.

#### to_frictionless() → frictionless.Field

Convert to a Frictionless Field.

Builds a typed frictionless Field via `Field.from_descriptor()` so that the
`type` (and any non-default constraints) appear in the serialised descriptor.
PUDL’s `geometry` type has no frictionless equivalent and falls back to
`"string"` with a custom `"geometry_format": "wkt"` annotation.

#### to_pandera_column(use_pandas_backend: [bool](https://docs.python.org/3/library/functions.html#bool)) → pandera.polars.Column

Encode this field def as a Pandera column.

### *class* pudl.metadata.classes.ForeignKeyReference(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Foreign key reference (resource.schema.foreign_keys[…].reference).

See [https://specs.frictionlessdata.io/table-schema/#foreign-keys](https://specs.frictionlessdata.io/table-schema/#foreign-keys).

#### resource *: [SnakeCase](#pudl.metadata.classes.SnakeCase)*

#### fields *: [StrictList](#pudl.metadata.classes.StrictList)[[SnakeCase](#pudl.metadata.classes.SnakeCase)]*

#### \_check_unique

### *class* pudl.metadata.classes.ForeignKey(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Foreign key (resource.schema.foreign_keys[…]).

See [https://specs.frictionlessdata.io/table-schema/#foreign-keys](https://specs.frictionlessdata.io/table-schema/#foreign-keys).

#### fields *: [StrictList](#pudl.metadata.classes.StrictList)[[SnakeCase](#pudl.metadata.classes.SnakeCase)]*

#### reference *: [ForeignKeyReference](#pudl.metadata.classes.ForeignKeyReference)*

#### \_check_unique

#### *classmethod* \_check_fields_equal_length(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### is_simple() → [bool](https://docs.python.org/3/library/functions.html#bool)

Indicate whether the FK relationship contains a single column.

#### to_sql() → sqlalchemy.ForeignKeyConstraint

Return equivalent SQL Foreign Key.

#### to_frictionless() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Convert to a frictionless foreign key descriptor dict.

### *class* pudl.metadata.classes.Schema(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Table schema (resource.schema).

See [https://specs.frictionlessdata.io/table-schema](https://specs.frictionlessdata.io/table-schema).

#### fields *: [StrictList](#pudl.metadata.classes.StrictList)[[Field](#pudl.metadata.classes.Field)]*

#### missing_values *: [list](https://docs.python.org/3/library/stdtypes.html#list)[pydantic.StrictStr]* *= ['']*

#### primary_key *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[SnakeCase](#pudl.metadata.classes.SnakeCase)]* *= []*

#### foreign_keys *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[ForeignKey](#pudl.metadata.classes.ForeignKey)]* *= []*

#### \_check_unique

#### *classmethod* \_check_field_names_unique(fields: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Field](#pudl.metadata.classes.Field)])

#### *classmethod* \_check_primary_key_in_fields(pk, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Verify that all primary key elements also appear in the schema fields.

#### \_check_foreign_key_in_fields()

Verify that all foreign key elements also appear in the schema fields.

#### to_pandera() → pandera.polars.DataFrameSchema

Turn PUDL Schema into Pandera schema, so dagster can understand it.

### *class* pudl.metadata.classes.License(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Data license (package|resource.licenses[…]).

See [https://specs.frictionlessdata.io/data-package/#licenses](https://specs.frictionlessdata.io/data-package/#licenses).

#### name *: [String](#pudl.metadata.classes.String)*

#### title *: [String](#pudl.metadata.classes.String)*

#### path *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)*

#### *static* dict_from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Construct dictionary from PUDL identifier.

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [License](#pudl.metadata.classes.License)

Construct from PUDL identifier.

### *class* pudl.metadata.classes.Contributor(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Data contributor (package.contributors[…]).

See [https://specs.frictionlessdata.io/data-package/#contributors](https://specs.frictionlessdata.io/data-package/#contributors).

#### title *: [String](#pudl.metadata.classes.String)*

#### path *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### email *: pydantic.EmailStr | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### roles *: [list](https://docs.python.org/3/library/stdtypes.html#list)[Literal['author', 'contributor', 'maintainer', 'publisher', 'wrangler']]* *= ['contributor']*

#### zenodo_role *: Literal['contact person', 'data collector', 'data curator', 'data manager', 'distributor', 'editor', 'hosting institution', 'other', 'producer', 'project leader', 'project member', 'registration agency', 'registration authority', 'related person', 'researcher', 'rights holder', 'sponsor', 'supervisor', 'work package leader']* *= 'project member'*

#### organization *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### orcid *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### name *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *static* dict_from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Construct dictionary from PUDL identifier.

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Contributor](#pudl.metadata.classes.Contributor)

Construct from PUDL identifier.

#### \_\_hash_\_()

Implements simple hash method.

Allows use of `set()` on a list of Contributor

### *class* pudl.metadata.classes.DataSource(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

A data source that has been integrated into PUDL.

This metadata is used for:

* Generating PUDL documentation.
* Annotating long-term archives of the raw input data on Zenodo.
* Defining what data partitions can be processed using PUDL.

It can also be used to populate the “source” fields of frictionless
data packages and data resources (package|resource.sources[…]).

See [https://specs.frictionlessdata.io/data-package/#sources](https://specs.frictionlessdata.io/data-package/#sources).

#### name *: [SnakeCase](#pudl.metadata.classes.SnakeCase)*

#### title *: [String](#pudl.metadata.classes.String)*

#### description *: [String](#pudl.metadata.classes.String)*

#### keywords *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= []*

#### path *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)*

#### contributors *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Contributor](#pudl.metadata.classes.Contributor)]* *= None*

#### license_raw *: [License](#pudl.metadata.classes.License)*

#### license_pudl *: [License](#pudl.metadata.classes.License)*

#### concept_doi *: [pudl.workspace.datastore.ZenodoDoi](../../workspace/datastore/index.md#pudl.workspace.datastore.ZenodoDoi) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### working_partitions *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[SnakeCase](#pudl.metadata.classes.SnakeCase), Any]*

#### source_file_dict *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[SnakeCase](#pudl.metadata.classes.SnakeCase), Any]*

#### email *: pydantic.EmailStr | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### get_resource_ids() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Compile list of resource IDs associated with this data source.

#### get_temporal_partitions() → [list](https://docs.python.org/3/library/stdtypes.html#list)

Return a list of temporal partitions encoding the time span covered by the data source.

#### get_temporal_coverage(partitions: [dict](https://docs.python.org/3/library/stdtypes.html#dict) = None) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Return a string describing the time span covered by the data source.

#### add_datastore_metadata(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Get source file metadata from the datastore.

#### to_rst(docs_dir: pydantic.DirectoryPath, source_resources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Resource](#pudl.metadata.classes.Resource)], extra_resources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Resource](#pudl.metadata.classes.Resource)], output_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Output a representation of the data source in RST for documentation.

#### to_frictionless() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Serialize to a frictionless data source descriptor.

The frictionless spec defines `title`, `path`, and `email` as
standard source fields.  PUDL-specific fields (`name`,
`description`, `keywords`, `concept_doi`, `license_raw`,
`license_pudl`, `contributors`) are included as extensions and
are preserved by the frictionless library.

#### *static* dict_from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str), sources: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Look up the source by source name in the metadata.

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str), sources: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = SOURCES) → [DataSource](#pudl.metadata.classes.DataSource)

Construct Source by source name in the metadata.

### *class* pudl.metadata.classes.ResourceHarvest(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Resource harvest parameters (resource.harvest).

#### harvest *: pydantic.StrictBool* *= False*

Whether to harvest from dataframes based on field names.

If `False`, the dataframe with the same name is used and the process is limited to
dropping unwanted fields.

#### tolerance *: [PositiveFloat](#pudl.metadata.classes.PositiveFloat)* *= 0.0*

Fraction of invalid fields above which result is considered invalid.

### *class* pudl.metadata.classes.PudlResourceDescriptor(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

The form we expect the RESOURCE_METADATA elements to take.

This differs from [`Resource`](#pudl.metadata.classes.Resource) and [`Schema`](#pudl.metadata.classes.Schema), etc., in that we represent
many complex types ([`Field`](#pudl.metadata.classes.Field), [`DataSource`](#pudl.metadata.classes.DataSource), etc.) with string IDs that
we then turn into instances of those types with lookups. We also use
`foreign_key_rules` to generate the actual `foreign_key` relationships that are
represented in a [`Schema`](#pudl.metadata.classes.Schema).

This is all very useful in that we can describe the resources more concisely!

TODO: In the future, we could convert from a [`PudlResourceDescriptor`](#pudl.metadata.classes.PudlResourceDescriptor) to
various standard formats, such as a Frictionless resource or a `pandera`
schema. This would require some of the logic currently in [`Resource`](#pudl.metadata.classes.Resource) to move
into this class.

#### *class* PudlSchemaDescriptor(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Container to hold the schema shape.

#### *class* PudlForeignKeyRules(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Container to describe what foreign key rules look like.

#### field_id_lists *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]* *= None*

#### exclude_ids *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= None*

#### field_ids *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= None*

#### primary_key_ids *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= None*

#### foreign_key_rules *: [PudlResourceDescriptor.PudlSchemaDescriptor.PudlForeignKeyRules](#pudl.metadata.classes.PudlResourceDescriptor.PudlSchemaDescriptor.PudlForeignKeyRules)*

#### *class* PudlCodeMetadata(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Describes a bunch of codes.

#### *class* CodeDataFrame

Bases: `pandera.pandas.DataFrameModel`

The DataFrame we use to represent code/label/description associations.

#### code *: pandera.pandas.typing.Series[Any]*

#### label *: pandera.pandas.typing.Series[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)*

#### description *: pandera.pandas.typing.Series[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### operational_status *: pandera.pandas.typing.Series[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)*

#### df *: pandera.pandas.typing.DataFrame[[PudlResourceDescriptor.PudlCodeMetadata.CodeDataFrame](#pudl.metadata.classes.PudlResourceDescriptor.PudlCodeMetadata.CodeDataFrame)]*

#### code_fixes *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

#### ignored_codes *: [list](https://docs.python.org/3/library/stdtypes.html#list)* *= []*

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### serialize_df(df: pandera.pandas.typing.DataFrame[[CodeDataFrame](#pudl.metadata.classes.PudlResourceDescriptor.PudlCodeMetadata.CodeDataFrame)], \_info) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Return DataFrame to avoid warnings from default serializer.

#### *class* PudlDescriptionComponents(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Container to hold description configuration information.

All of these parameters have reasonable defaults for most resources if left
unset.  You must specify [`PudlResourceDescriptor.description`](#pudl.metadata.classes.PudlResourceDescriptor.description) as a
dictionary, but you do not have to put anything in it so long as the resource id
follows the standard pattern.

#### table_type_code *: Literal['assn', 'codes', 'entity', 'scd', 'timeseries', 'forensics'] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Indicates the type of asset stored in this resource.

If None or otherwise left unset, will be filled in with a default type parsed
from the resource id string.

#### timeseries_resolution_code *: Literal['quarterly', 'yearly', 'monthly', 'hourly'] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

If this resource has
[`table_type_code`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.table_type_code)
timeseries, indicates the temporal resolution, otherwise None.  If
[`table_type_code`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.table_type_code) is
timeseries and this value is None or otherwise left unset, will be filled in
with a default resolution parsed from the resource id string.

#### layer_code *: Literal['raw', '_core', 'core', 'out', 'out_narrow', 'test'] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Indicates the degree of processing applied to the data in this resource.  If
None or otherwise left unset, will be filled in with a default layer parsed from
the resource id string.

#### source_code *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Indicates the source we wish to display for this resource; distinct from
[`PudlResourceDescriptor.source_ids`](#pudl.metadata.classes.PudlResourceDescriptor.source_ids) because here we want the majority
source (or grouped source if truly mixed) and not a complete list of all sources
used for this resource.  If set, should be a known data source shortcode like
“eia923” or one of the grouped shortcodes from
`source_descriptions`.  If None or otherwise
left unset, will be filled in with a default source parsed from the resource id
string.

#### usage_warnings *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str) | [dict](https://docs.python.org/3/library/stdtypes.html#dict)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

List of string keys (for common warnings; see [`warnings`](https://docs.python.org/3/library/warnings.html#module-warnings)) and dicts (for
custom warnings) stating necessary precautions for using this resource.

Usage Warnings are a way for us to quickly and skim-ably tell users about
analysis hazards when using a particular table.  It has two goals:

1. help users quickly reach a point of success in their use of our data, and
2. reduce the incidence of repeated questions and bug-like reports due to these
   inescapable hazards.

Reserve this field for severe and/or frequent problems an unfamiliar user may
encounter, and list lighter or edge-case problems in
[`additional_details_text`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_details_text).

The list can contain two kinds of entries:

* a string, which should match one of the keys in
  [`USAGE_WARNINGS`](../warnings/index.md#pudl.metadata.warnings.USAGE_WARNINGS)
* a dict, which should contain two keys:
  * “type” - a short code for the warning, which doesn’t need to be unique and
    will only appear in preview & debugging tooling, not to users
  * “description” - the one-to-two-sentence summary of a warning used only on
    this particular resource

The system will automatically detect and include the following warnings based on
the resource id string and schema information (see
`_assemble_usage_warnings()`):

* multiple_inputs
* ferc_is_hard

Any items provided here will be listed before the automatically detected
warnings.

If None or otherwise left unset, will be filled in with auto warnings only. If
no auto warnings apply, hides the Usage Warnings section entirely.

#### availability_offset *: [int](https://docs.python.org/3/library/functions.html#int)* *= 0*

Partition offset of most recent data available from that claimed by the data
source.

Only permitted when `availability_text` is None or otherwise unset.
Useful in cases where a particular resource both has no natural date column
for row count partitioning, and is updated on a slower cadence than its
source. For example, EIA 923 has yearly partitions, but while the
monthly output tables receive data from the new year as soon as it is
added to the source, the annual output tables aren’t updated until the
following year.

#### availability_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Most recent data available. If None or otherwise left unset, will be
filled in with the most recent partition listed in the row counts file
for this resource. If this is None/unset **and** the row counts
partition is null, then will be filled in with the most recent partition
listed for the data source, optionally offset by `availability_offset`
partitions.

Generally only set when a discontinued table does not use temporal partitioning
in the row counts file, but we still know when its freshest data was from.

#### additional_summary_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

A brief (~one-line) description of the contents of this resource.
If None or otherwise left unset, will be left blank.

If filled, should support whichever of the following scenarios is most
appropriate for this resource:

* the [`table_type_code`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.table_type_code)
  is set or can be automatically detected: this value should complete the
  sentence corresponding to
  `table_type_fragments` for this resource’s
  [`table_type_code`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.table_type_code)
* the [`table_type_code`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.table_type_code)
  is None/unset *and* the resource is not named according to a standard table
  type listed in `table_type_fragments`: this
  value should be a complete sentence summarizing the contents of this resource
  at a similar level of detail.

#### additional_layer_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Unusual details about this resource’s level of processing that don’t fall
into the normal definition of raw/core/_core/out/etc.  If None or otherwise left
unset, will be left blank.  This should only be set in truly obscure situations.
If set, should be a complete sentence.

#### additional_source_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

A brief refinement on the source data for this table, such as indicating the
Schedule or other section number.  If None or otherwise left unset, will be left
blank.  If set, should make sense when displayed directly after the title of a
datasource (see `source_descriptions`);
parentheticals work best here.

#### additional_primary_key_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

For resources with no primary key, a brief summary of what each row contains,
and perhaps why a primary key doesn’t make sense for this table.  If None or
otherwise left unset, will be left blank.  If set, should be a complete sentence
or two.

This is generally not set when there is a primary key for the table.  If a
primary key is available,
[`additional_primary_key_text`](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents.additional_primary_key_text)
will appear after the comma-delimited list of primary key columns.

#### additional_details_text *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

All other information about this resource’s construction and intended use,
including guidelines and recommendations for best results.  If None or otherwise
left unset, will be left blank; hides the Additional Details section entirely.

Q3 2025 Migration Mode variance: if [`PudlResourceDescriptor.description`](#pudl.metadata.classes.PudlResourceDescriptor.description)
is a string, it gets moved here so you can see the old description content in
the Additional Details section of the preview.

May also include more-detailed explanations of listed usage warnings.

#### title *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### description *: [PudlResourceDescriptor.PudlDescriptionComponents](#pudl.metadata.classes.PudlResourceDescriptor.PudlDescriptionComponents) | [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### schema_ *: [PudlResourceDescriptor.PudlSchemaDescriptor](#pudl.metadata.classes.PudlResourceDescriptor.PudlSchemaDescriptor)* *= None*

#### encoder *: [PudlResourceDescriptor.PudlCodeMetadata](#pudl.metadata.classes.PudlResourceDescriptor.PudlCodeMetadata) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### source_ids *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= None*

#### etl_group_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= None*

#### field_namespace_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= None*

#### create_database_schema *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

#### path *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### extrapaths *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

### *class* pudl.metadata.classes.Resource(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Tabular data resource (package.resources[…]).

See [https://specs.frictionlessdata.io/tabular-data-resource](https://specs.frictionlessdata.io/tabular-data-resource).

### Examples

A simple example illustrates the conversion to SQLAlchemy objects.

```pycon
>>> fields = [{'name': 'x', 'type': 'year', 'description': 'X'}, {'name': 'y', 'type': 'string', 'description': 'Y'}]
>>> fkeys = [{'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}]
>>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': fkeys}
>>> resource = Resource(name='a', schema=schema, description='A')
>>> table = resource.to_sql()
>>> table.columns.x
Column('x', Integer(), ForeignKey('b.x'), CheckConstraint(...), table=<a>, primary_key=True, nullable=False, comment='X')
>>> table.columns.y
Column('y', Text(), ForeignKey('b.y'), CheckConstraint(...), table=<a>, comment='Y')
```

To illustrate harvesting operations,
say we have a resource with two fields - a primary key (id) and a data field -
which we want to harvest from two different dataframes.

```pycon
>>> from pudl.metadata.helpers import unique, as_dict
>>> fields = [
...     {'name': 'id', 'type': 'integer', 'description': 'ID'},
...     {'name': 'x', 'type': 'integer', 'harvest': {'aggregate': unique, 'tolerance': 0.25}, 'description': 'X'}
... ]
>>> resource = Resource(**{
...     'name': 'a',
...     'harvest': {'harvest': True},
...     'schema': {'fields': fields, 'primary_key': ['id']},
...     'description': 'A',
... })
>>> dfs = {
...     'a': pd.DataFrame({'id': [1, 1, 2, 2], 'x': [1, 1, 2, 2]}),
...     'b': pd.DataFrame({'id': [2, 3, 3], 'x': [3, 4, 4]})
... }
```

Skip aggregation to access all the rows concatenated from the input dataframes.
The names of the input dataframes are used as the index.

```pycon
>>> df, _ = resource.harvest_dfs(dfs, aggregate=False)
>>> df
    id  x
df
a    1  1
a    1  1
a    2  2
a    2  2
b    2  3
b    3  4
b    3  4
```

Field names and data types are enforced.

```pycon
>>> resource.to_pandas_dtypes() == df.dtypes.apply(str).to_dict()
True
```

Alternatively, aggregate by primary key
(the default when [`harvest`](#pudl.metadata.classes.Resource.harvest). `harvest=True`)
and report aggregation errors.

```pycon
>>> df, report = resource.harvest_dfs(dfs)
>>> df
       x
id
1      1
2   <NA>
3      4
>>> report['stats']
{'all': 2, 'invalid': 1, 'tolerance': 0.0, 'actual': 0.5}
>>> report['fields']['x']['stats']
{'all': 3, 'invalid': 1, 'tolerance': 0.25, 'actual': 0.33...}
>>> report['fields']['x']['errors']
id
2    Not unique.
Name: x, dtype: object
```

Customize the error values in the error report.

```pycon
>>> error = lambda x, e: as_dict(x)
>>> df, report = resource.harvest_dfs(
...    dfs, aggregate_kwargs={'raised': False, 'error': error}
... )
>>> report['fields']['x']['errors']
id
2    {'a': [2, 2], 'b': [3]}
Name: x, dtype: object
```

Limit harvesting to the input dataframe of the same name
by setting [`harvest`](#pudl.metadata.classes.Resource.harvest). `harvest=False`.

```pycon
>>> resource.harvest.harvest = False
>>> df, _ = resource.harvest_dfs(dfs, aggregate_kwargs={'raised': False})
>>> df
    id  x
df
a    1  1
a    1  1
a    2  2
a    2  2
```

Harvesting can also handle conversion to longer time periods.
Period harvesting requires primary key fields with a `datetime` data type,
except for `year` fields which can be integer.

```pycon
>>> fields = [{'name': 'report_year', 'type': 'year', 'description': 'Report year'}]
>>> resource = Resource(**{
...     'name': 'table', 'harvest': {'harvest': True},
...     'schema': {'fields': fields, 'primary_key': ['report_year']},
...     'description': 'Table',
... })
>>> df = pd.DataFrame({'report_date': ['2000-02-02', '2000-03-03']})
>>> resource.format_df(df)
  report_year
0  2000-01-01
1  2000-01-01
>>> df = pd.DataFrame({'report_year': [2000, 2000]})
>>> resource.format_df(df)
  report_year
0  2000-01-01
1  2000-01-01
```

#### name *: [SnakeCase](#pudl.metadata.classes.SnakeCase)*

#### title *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### description *: [String](#pudl.metadata.classes.String)*

#### harvest *: [ResourceHarvest](#pudl.metadata.classes.ResourceHarvest)*

#### schema *: [Schema](#pudl.metadata.classes.Schema)*

#### format_ *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### mediatype *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### dialect *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### contributors *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Contributor](#pudl.metadata.classes.Contributor)]* *= []*

#### licenses *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[License](#pudl.metadata.classes.License)]* *= []*

#### sources *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DataSource](#pudl.metadata.classes.DataSource)]* *= []*

#### keywords *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[String](#pudl.metadata.classes.String)]* *= []*

#### encoder *: [Encoder](#pudl.metadata.classes.Encoder) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### path *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= None*

#### extrapaths *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### field_namespace *: [FieldNamespace](#pudl.metadata.classes.FieldNamespace) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### etl_group *: [EtlGroup](#pudl.metadata.classes.EtlGroup) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### create_database_schema *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

#### \_check_unique

#### *property* sphinx_ref_name

Get legal Sphinx ref name.

Sphinx throws an error when creating a cross ref target for
a resource that has a preceding underscore. It is
also possible for resources to have identical names
when the preceding underscore is removed. This function
adds a preceding ‘i’ to cross ref targets for resources
with preceding underscores. The ‘i’ will not be rendered
in the docs, only in the .rst files the hyperlinks.

#### *classmethod* \_check_harvest_primary_key(value, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

#### *static* dict_from_id(resource_id: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Construct dictionary from PUDL identifier (resource.name).

#### *static* \_resolve_references_from_resource_descriptor(resource_id: [str](https://docs.python.org/3/library/stdtypes.html#str), descriptor: [PudlResourceDescriptor](#pudl.metadata.classes.PudlResourceDescriptor)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Partially constructs a Resource-shaped dict from a PudlResourceDescriptor.

* `schema.fields`
  * Field names are expanded ([`Field.from_id()`](#pudl.metadata.classes.Field.from_id)).
  * Field attributes are replaced with any specific to the
    `resource.group` and `field.name`.
* `sources`: Source ids are expanded (`Source.from_id()`).
* `licenses`: License ids are expanded ([`License.from_id()`](#pudl.metadata.classes.License.from_id)).
* `contributors`: Contributor ids are fetched by source ids,
  then expanded ([`Contributor.from_id()`](#pudl.metadata.classes.Contributor.from_id)).
* `keywords`: Keywords are fetched by source ids.
* `schema.foreign_keys`: Foreign keys are fetched by resource name.

**Does not compute** resource description text and field encoders.

#### *static* dict_from_resource_descriptor(resource_id: [str](https://docs.python.org/3/library/stdtypes.html#str), descriptor: [PudlResourceDescriptor](#pudl.metadata.classes.PudlResourceDescriptor)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Get a Resource-shaped dict from a PudlResourceDescriptor.

* schema.fields
  * Field names are expanded ([`Field.from_id()`](#pudl.metadata.classes.Field.from_id)).
  * Field attributes are replaced with any specific to the
    resource.group and field.name.
* sources: Source ids are expanded (`Source.from_id()`).
* licenses: License ids are expanded ([`License.from_id()`](#pudl.metadata.classes.License.from_id)).
* contributors: Contributor ids are fetched by source ids,
  then expanded ([`Contributor.from_id()`](#pudl.metadata.classes.Contributor.from_id)).
* keywords: Keywords are fetched by source ids.
* schema.foreign_keys: Foreign keys are fetched by resource name.
* description: Full description text block is rendered from its component parts.

#### *classmethod* from_id(x: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Resource](#pudl.metadata.classes.Resource)

Construct from PUDL identifier (resource.name).

#### get_field(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Field](#pudl.metadata.classes.Field)

Return field with the given name if it’s part of the Resources.

#### get_field_names() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Return a list of all the field names in the resource schema.

#### to_sql(metadata: sqlalchemy.MetaData = None, check_types: [bool](https://docs.python.org/3/library/functions.html#bool) = True, check_values: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → sqlalchemy.Table

Return equivalent SQL Table.

#### to_frictionless() → frictionless.Resource

Convert to a Frictionless Resource.

#### to_pyarrow() → [pyarrow.Schema](https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema)

Construct a PyArrow schema for the resource.

#### to_duckdb_dtypes(conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), duckdb.sqltypes.DuckDBPyType]

Return Polars data type of each field by field name.

#### to_polars_dtypes() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), polars.DataType]

Return Polars data type of each field by field name.

#### to_pandas_dtypes(\*\*kwargs: Any) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [pandas.CategoricalDtype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.CategoricalDtype.html#pandas.CategoricalDtype)]

Return Pandas data type of each field by field name.

* **Parameters:**
  **kwargs** – Arguments to [`Field.to_pandas_dtype()`](#pudl.metadata.classes.Field.to_pandas_dtype).

#### match_primary_key(names: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)

Match primary key fields to input field names.

An exact match is required unless [`harvest`](#pudl.metadata.classes.Resource.harvest) .\`harvest=True\`,
in which case periodic names may also match a basename with a smaller period.

* **Parameters:**
  **names** – Field names.
* **Raises:**
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Field names are not unique.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Multiple field names match primary key field.
* **Returns:**
  The name matching each primary key field (if any) as a [`dict`](https://docs.python.org/3/library/stdtypes.html#dict),
  or `None` if not all primary key fields have a match.

### Examples

```pycon
>>> fields = [{'name': 'x_year', 'type': 'year', 'description': 'Year'}]
>>> schema = {'fields': fields, 'primary_key': ['x_year']}
>>> resource = Resource(name='r', schema=schema, description='R')
```

By default, when [`harvest`](#pudl.metadata.classes.Resource.harvest) .\`harvest=False\`,
exact matches are required.

```pycon
>>> resource.harvest.harvest
False
>>> resource.match_primary_key(['x_month']) is None
True
>>> resource.match_primary_key(['x_year', 'x_month'])
{'x_year': 'x_year'}
```

When [`harvest`](#pudl.metadata.classes.Resource.harvest) .\`harvest=True\`,
in the absence of an exact match,
periodic names may also match a basename with a smaller period.

```pycon
>>> resource.harvest.harvest = True
>>> resource.match_primary_key(['x_year', 'x_month'])
{'x_year': 'x_year'}
>>> resource.match_primary_key(['x_month'])
{'x_month': 'x_year'}
>>> resource.match_primary_key(['x_month', 'x_date'])
Traceback (most recent call last):
ValueError: ... {'x_month', 'x_date'} match primary key field 'x_year'
```

#### format_df(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame) | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*kwargs: Any) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Format a dataframe according to the resources’s table schema.

* DataFrame columns not in the schema are dropped.
* Any columns missing from the DataFrame are added with the right dtype, but
  will be empty.
* All columns are cast to their specified pandas dtypes.
* Primary key columns must be present and non-null.
* Periodic primary key fields are snapped to the start of the desired period.
* If the primary key fields could not be matched to columns in `df`
  ([`match_primary_key()`](#pudl.metadata.classes.Resource.match_primary_key)) or if `df=None`, an empty dataframe is returned.

* **Parameters:**
  * **df** – Dataframe to format.
  * **kwargs** – Arguments to `Field.to_pandas_dtypes()`.
* **Returns:**
  Dataframe with column names and data types matching the resource fields.

#### enforce_schema(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Drop columns not in the DB schema and enforce specified types.

#### aggregate_df(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raised: [bool](https://docs.python.org/3/library/functions.html#bool) = False, error: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable) = None) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [dict](https://docs.python.org/3/library/stdtypes.html#dict)]

Aggregate dataframe by primary key.

The dataframe is grouped by primary key fields
and aggregated with the aggregate function of each field
(`schema_`. `fields[*].harvest.aggregate`).

The report is formatted as follows:

* `valid` (bool): Whether resource is valid.
* `stats` (dict): Error statistics for resource fields.
* `fields` (dict):
  * `<field_name>` (str)
    * `valid` (bool): Whether field is valid.
    * `stats` (dict): Error statistics for field groups.
    * `errors` ([`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)): Error values indexed by primary key.
  * …

Each `stats` (dict) contains the following:

* `all` (int): Number of entities (field or field group).
* `invalid` (int): Invalid number of entities.
* `tolerance` (float): Fraction of invalid entities below which
  parent entity is considered valid.
* `actual` (float): Actual fraction of invalid entities.

* **Parameters:**
  * **df** – Dataframe to aggregate. It is assumed to have column names and
    data types matching the resource fields.
  * **raised** – Whether aggregation errors are raised or
    replaced with `np.nan` and returned in an error report.
  * **error** – A function with signature `f(x, e) -> Any`,
    where `x` are the original field values as a [`pandas.Series`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)
    and `e` is the original error.
    If provided, the returned value is reported instead of `e`.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – A primary key is required for aggregating.
* **Returns:**
  The aggregated dataframe indexed by primary key fields,
  and an aggregation report (descripted above)
  that includes all aggregation errors and whether the result
  meets the resource’s and fields’ tolerance.

#### \_build_aggregation_report(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), errors: [dict](https://docs.python.org/3/library/stdtypes.html#dict)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Build report from aggregation errors.

* **Parameters:**
  * **df** – Harvested dataframe (see [`harvest_dfs()`](#pudl.metadata.classes.Resource.harvest_dfs)).
  * **errors** – Aggregation errors (see `groupby_aggregate()`).
* **Returns:**
  Aggregation report, as described in [`aggregate_df()`](#pudl.metadata.classes.Resource.aggregate_df).

#### harvest_dfs(dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], aggregate: [bool](https://docs.python.org/3/library/functions.html#bool) = None, aggregate_kwargs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = {}, format_kwargs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] = {}) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [dict](https://docs.python.org/3/library/stdtypes.html#dict)]

Harvest from named dataframes.

For standard resources ([`harvest`](#pudl.metadata.classes.Resource.harvest). `harvest=False`), the columns
matching all primary key fields and any data fields are extracted from
the input dataframe of the same name.

For harvested resources ([`harvest`](#pudl.metadata.classes.Resource.harvest). `harvest=True`), the columns
matching all primary key fields and any data fields are extracted from
each compatible input dataframe, and concatenated into a single
dataframe.  Periodic key fields (e.g. ‘report_month’) are matched to any
column of the same name with an equal or smaller period (e.g.
‘report_day’) and snapped to the start of the desired period.

If `aggregate=False`, rows are indexed by the name of the input dataframe.
If `aggregate=True`, rows are indexed by primary key fields.

* **Parameters:**
  * **dfs** – Dataframes to harvest.
  * **aggregate** – Whether to aggregate the harvested rows by their primary key.
    By default, this is `True` if `self.harvest.harvest=True` and
    `False` otherwise.
  * **aggregate_kwargs** – Optional arguments to [`aggregate_df()`](#pudl.metadata.classes.Resource.aggregate_df).
  * **format_kwargs** – Optional arguments to [`format_df()`](#pudl.metadata.classes.Resource.format_df).
* **Returns:**
  A dataframe harvested from the dataframes, with column names and
  data types matching the resource fields, alongside an aggregation
  report.

#### to_rst(docs_dir: pydantic.DirectoryPath, path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Output to an RST file.

#### encode(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Standardize coded columns using the foreign column they refer to.

### *class* pudl.metadata.classes.Package(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

Tabular data package.

See [https://specs.frictionlessdata.io/data-package](https://specs.frictionlessdata.io/data-package).

### Examples

Foreign keys between resources are checked for completeness and consistency.

```pycon
>>> fields = [{'name': 'x', 'type': 'year', 'description': 'X'}, {'name': 'y', 'type': 'string', 'description': 'Y'}]
>>> fkey = {'fields': ['x', 'y'], 'reference': {'resource': 'b', 'fields': ['x', 'y']}}
>>> schema = {'fields': fields, 'primary_key': ['x'], 'foreign_keys': [fkey]}
>>> a = Resource(name='a', schema=schema, description='A')
>>> b = Resource(name='b', schema=Schema(fields=fields, primary_key=['x']), description='B')
>>> Package(name='ab', resources=[a, b])
Traceback (most recent call last):
ValidationError: ...
>>> b.schema.primary_key = ['x', 'y']
>>> package = Package(name='ab', resources=[a, b])
```

SQL Alchemy can sort tables, based on foreign keys,
in the order in which they need to be loaded into a database.

```pycon
>>> metadata = package.to_sql()
>>> [table.name for table in metadata.sorted_tables]
['b', 'a']
```

#### name *: [String](#pudl.metadata.classes.String)*

#### title *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### description *: [String](#pudl.metadata.classes.String) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### version *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### keywords *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[String](#pudl.metadata.classes.String)]* *= []*

#### homepage *: [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)*

#### created *: [datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)*

#### contributors *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Contributor](#pudl.metadata.classes.Contributor)]* *= []*

#### sources *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[DataSource](#pudl.metadata.classes.DataSource)]* *= []*

#### licenses *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[License](#pudl.metadata.classes.License)]* *= []*

#### resources *: [StrictList](#pudl.metadata.classes.StrictList)[[Resource](#pudl.metadata.classes.Resource)]*

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

#### *classmethod* \_check_foreign_keys(resources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Resource](#pudl.metadata.classes.Resource)])

#### *static* \_compile_from_resources(resources: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Resource](#pudl.metadata.classes.Resource)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[Any]]

Compile deduplicated contributors, licenses, keywords, and sources from resources.

Returns a dict with keys `contributors`, `licenses`, `keywords`, and
`sources`, each containing a deduplicated list of values drawn from
`resources` in order of first appearance.

#### \_populate_from_resources()

Populate Package attributes from similar deduplicated Resource attributes.

Resources and Packages share some descriptive attributes. When building a
Package out of a collection of Resources, we want the Package to reflect the
union of all the analogous values found in the Resources, but we don’t want
any duplicates. We may also get values directly from the Package inputs.

#### *classmethod* from_resource_ids(resource_ids: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = tuple(sorted(RESOURCE_METADATA)), resolve_foreign_keys: [bool](https://docs.python.org/3/library/functions.html#bool) = False, excluded_etl_groups: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = (), title: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, description: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, version: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [Package](#pudl.metadata.classes.Package)

Construct a collection of Resources from PUDL identifiers (resource.name).

Identify any fields that have foreign key relationships referencing the
coding tables defined in [`pudl.metadata.codes`](../codes/index.md#module-pudl.metadata.codes) and if so, associate the
coding table’s encoder with those columns for later use cleaning them up.

The result is cached, since we so often need to generate the metadata for
the full collection of PUDL tables.

* **Parameters:**
  * **resource_ids** – Resource PUDL identifiers (resource.name). Needs to
    be a Tuple so that the set of identifiers is hashable, allowing
    return value caching through lru_cache.
  * **resolve_foreign_keys** – Whether to add resources as needed based on
    foreign keys.
  * **excluded_etl_groups** – Collection of ETL groups used to filter resources
    out of Package.
  * **title** – Human-readable title for the package.
  * **description** – Human-readable description of the package.
  * **version** – Version string for the package.

#### *static* get_etl_group_tables(etl_group: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Get a sorted tuple of table names for an etl_group.

* **Parameters:**
  **etl_group** – the etl_group key.
* **Returns:**
  A sorted tuple of table names for the etl_group.

#### get_resource(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [Resource](#pudl.metadata.classes.Resource)

Return the resource with the given name if it is in the Package.

#### to_rst(docs_dir: pydantic.DirectoryPath, path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Output to an RST file.

#### to_sql(check_types: [bool](https://docs.python.org/3/library/functions.html#bool) = True, check_values: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → sqlalchemy.MetaData

Return equivalent SQL MetaData.

#### get_sorted_resources() → [StrictList](#pudl.metadata.classes.StrictList)[[Resource](#pudl.metadata.classes.Resource)]

Get a list of sorted Resources.

Currently Resources are listed in reverse alphabetical order based
on their name which results in the following order to promote output
tables to users and push intermediate tables to the bottom of the
docs: output, core, intermediate.
In the future we might want to have more fine grain control over how
Resources are sorted.

* **Returns:**
  A sorted list of resources.

#### *property* encoders *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[SnakeCase](#pudl.metadata.classes.SnakeCase), [Encoder](#pudl.metadata.classes.Encoder)]*

Compile a mapping of field names to their encoders, if they exist.

This dictionary will be used many times, so it makes sense to build it once
when the Package is instantiated so it can be reused.

#### encode(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), copy: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean up all coded columns in a dataframe based on PUDL coding tables.

Running with `copy=False` is intended for memory-intensive data frames where no
upstream process retains a reference to the data. Use care with this option,
and keep an eye out for spooky data changes showing up in unexpected places.

* **Parameters:**
  * **df** – DataFrame whose code columns are being cleaned up.
  * **copy** – (Default True) Return a copy, making no changes to the original data.
* **Returns:**
  A dataframe with clean code columns.

#### to_frictionless(exclude_pattern: [str](https://docs.python.org/3/library/stdtypes.html#str) | [re.Pattern](https://docs.python.org/3/library/re.html#re.Pattern)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None, include_pattern: [str](https://docs.python.org/3/library/stdtypes.html#str) | [re.Pattern](https://docs.python.org/3/library/re.html#re.Pattern)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → frictionless.Package

Convert to a Frictionless Datapackage.

Allows filtering out specific resources by passing regex patterns to include
or exclude resources by name. This is used to generate an independent
‘datapackage.json’ file for `ferceqr` assets, which are distributed separately
from the rest of PUDL. This method will only look for table names that exactly
match the supplied patterns, not substring matches.

* **Parameters:**
  * **exclude_pattern** – Exclude resources whose names exactly match this pattern.
  * **include_pattern** – Only include resources whose names exactly match this pattern.

### pudl.metadata.classes.PUDL_PACKAGE

Define a global PUDL package object for use across the entire codebase.

This needs to happen after the definition of the Package class above, and it is used in
some of the class definitions below, but having it defined in the middle of this module
is kind of obscure, so it is imported in the \_\_init_\_.py for this subpackage and then
imported in other modules from that more prominent location.

### *class* pudl.metadata.classes.CodeMetadata(/, \*\*data: Any)

Bases: [`PudlMeta`](#pudl.metadata.classes.PudlMeta)

A list of Encoders for standardizing and documenting categorical codes.

Used to export static coding metadata to PUDL documentation automatically

#### encoder_list *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[Encoder](#pudl.metadata.classes.Encoder)]* *= []*

#### *classmethod* from_code_ids(code_ids: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [CodeMetadata](#pudl.metadata.classes.CodeMetadata)

Construct a list of encoders from code dictionaries.

* **Parameters:**
  **code_ids** – A list of Code PUDL identifiers, keys to entries in the
  CODE_METADATA dictionary.

#### to_rst(top_dir: pydantic.DirectoryPath, csv_subdir: pydantic.DirectoryPath, rst_path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Iterate through encoders and output to an RST file.
