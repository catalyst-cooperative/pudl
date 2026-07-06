# pudl.transform.classes

Classes for defining & coordinating the transformation of tabular data sources.

We define our data transformations in four separate components:

> * The data being transformed (`pd.DataFrame` or `pd.Series`).
> * The functions & methods doing the transformations.
> * Non-data parameters that control the behavior of the transform functions & methods.
> * Classes that organize the functions & parameters that transform a given input table.

Separating out the transformation functions and the parameters that control them allows
us to re-use the same transforms in many different contexts without duplicating the
code.

Transform functions take data (either a Series or DataFrame) and a TransformParams
object as inputs, and return transformed data of the same type that they consumed
(Series or DataFrame). They operate on the data, and their particular behavior is
controlled by the TransformParams. Like the TableTransformer classes discussed below,
they are organized into 3 separate levels of abstraction:

> * general-purpose: always available from the abstract base class.
> * dataset-specific: used repeatedly by a dataset, from an intermediate abstract class.
> * table-specific: used only once for a particular table, defined in a concrete class.

These functions are not generally meant to be used independent of a `TableTransformer`
class. They are wrapped by methods within the class definitions which handle logging
and intermediate dataframe caching.

> * Transform functions that operate on individual columns should implement the
>   [`ColumnTransformFunc`](#pudl.transform.classes.ColumnTransformFunc) `Protocol`.
> * Transform functions that need to operate on whole tables should implement the
>   [`TableTransformFunc`](#pudl.transform.classes.TableTransformFunc) `Protocol`.
> * To iteratively apply a [`ColumnTransformFunc`](#pudl.transform.classes.ColumnTransformFunc) to several columns in a table,
>   use [`multicol_transform_factory()`](#pudl.transform.classes.multicol_transform_factory) to construct a
>   [`MultiColumnTransformFunc`](#pudl.transform.classes.MultiColumnTransformFunc)

Using a hierarchy of `TableTransformer` classes to organize the functions and
parameters allows us to apply a particular set of transformations uniformly across every
table that’s part of a family of similar data. It also allows us to keep transform
functions that only apply to a particular collection of tables or an individual table
separated from other data that it should not be used with.

Currently there are 3 levels of abstraction in the TableTransformer classes:

> * The [`AbstractTableTransformer`](#pudl.transform.classes.AbstractTableTransformer) abstract base class that defines methods
>   useful across a wide range of data sources.
> * A dataset-specific abstract class that can define transforms which are consistently
>   useful across many tables in the dataset (e.g. the
>   [`pudl.transform.ferc1.Ferc1AbstractTableTransformer`](../ferc1/index.md#pudl.transform.ferc1.Ferc1AbstractTableTransformer) class).
> * Table-specific concrete classes that inherit from both of the higher levels, and
>   contain any bespoke transformations or parameters that only pertain to that table.
>   (e.g. the `pudl.transform.ferc1.SteamPlantsFerc1TableTransformer` class).

The [`TransformParams`](#pudl.transform.classes.TransformParams) classes are immutable `pydantic` models that store and
the parameters which are passed to the transform functions / methods described above.
These models are defined alongside the functions they’re used with. General purpose
transforms have their parameter models defined in this module. Dataset-specific
transforms should have their parameters defined in the module that defines the
associated transform function. The [`MultiColumnTransformParams`](#pudl.transform.classes.MultiColumnTransformParams) models are
dictionaries keyed by column name, that must map to per-column parameters which are all
of the same type.

Specific [`TransformParams`](#pudl.transform.classes.TransformParams) classes are instantiated using dictionaries of values
defined in the per-dataset modules under [`pudl.transform.params`](../params/index.md#module-pudl.transform.params) e.g.
[`pudl.transform.params.ferc1`](../params/ferc1/index.md#module-pudl.transform.params.ferc1).

## Attributes

| [`logger`](#pudl.transform.classes.logger)                                                       |                                                                                                              |
|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| [`normalize_strings_multicol`](#pudl.transform.classes.normalize_strings_multicol)               | A multi-column version of the [`normalize_strings()`](#pudl.transform.classes.normalize_strings) function.   |
| [`enforce_snake_case_multicol`](#pudl.transform.classes.enforce_snake_case_multicol)             |                                                                                                              |
| [`strip_non_numeric_values_multicol`](#pudl.transform.classes.strip_non_numeric_values_multicol) |                                                                                                              |
| [`categorize_strings_multicol`](#pudl.transform.classes.categorize_strings_multicol)             | A multi-column version of the [`categorize_strings()`](#pudl.transform.classes.categorize_strings) function. |
| [`convert_units_multicol`](#pudl.transform.classes.convert_units_multicol)                       | A multi-column version of the [`convert_units()`](#pudl.transform.classes.convert_units) function.           |
| [`nullify_outliers_multicol`](#pudl.transform.classes.nullify_outliers_multicol)                 | A multi-column version of the [`nullify_outliers()`](#pudl.transform.classes.nullify_outliers) function.     |
| [`replace_with_na_multicol`](#pudl.transform.classes.replace_with_na_multicol)                   | A multi-column version of the [`nullify_outliers()`](#pudl.transform.classes.nullify_outliers) function.     |

## Classes

| [`TransformParams`](#pudl.transform.classes.TransformParams)                       | An immutable base model for transformation parameters.                                                               |
|------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| [`MultiColumnTransformParams`](#pudl.transform.classes.MultiColumnTransformParams) | A dictionary of [`TransformParams`](#pudl.transform.classes.TransformParams) to apply to several columns in a table. |
| [`ColumnTransformFunc`](#pudl.transform.classes.ColumnTransformFunc)               | Callback protocol defining a per-column transformation function.                                                     |
| [`TableTransformFunc`](#pudl.transform.classes.TableTransformFunc)                 | Callback protocol defining a per-table transformation function.                                                      |
| [`MultiColumnTransformFunc`](#pudl.transform.classes.MultiColumnTransformFunc)     | Callback protocol defining a per-table transformation function.                                                      |
| [`RenameColumns`](#pudl.transform.classes.RenameColumns)                           | A dictionary for mapping old column names to new column names in a dataframe.                                        |
| [`StringNormalization`](#pudl.transform.classes.StringNormalization)               | Options to control string normalization.                                                                             |
| [`EnforceSnakeCase`](#pudl.transform.classes.EnforceSnakeCase)                     | Boolean parameter for [`enforce_snake_case()`](#pudl.transform.classes.enforce_snake_case).                          |
| [`StripNonNumericValues`](#pudl.transform.classes.StripNonNumericValues)           | Boolean parameter for [`strip_non_numeric_values()`](#pudl.transform.classes.strip_non_numeric_values).              |
| [`StringCategories`](#pudl.transform.classes.StringCategories)                     | Mappings to categorize the values in freeform string columns.                                                        |
| [`UnitConversion`](#pudl.transform.classes.UnitConversion)                         | A column-wise unit conversion which can also rename the column.                                                      |
| [`ValidRange`](#pudl.transform.classes.ValidRange)                                 | Column level specification of min and/or max values.                                                                 |
| [`UnitCorrections`](#pudl.transform.classes.UnitCorrections)                       | Fix outlying values resulting from apparent unit errors.                                                             |
| [`InvalidRows`](#pudl.transform.classes.InvalidRows)                               | Pameters that identify invalid rows to drop.                                                                         |
| [`ReplaceWithNa`](#pudl.transform.classes.ReplaceWithNa)                           | Pameters that replace certain values with NA.                                                                        |
| [`SpotFixes`](#pudl.transform.classes.SpotFixes)                                   | Parameters that replace certain values with a manually corrected value.                                              |
| [`TableTransformParams`](#pudl.transform.classes.TableTransformParams)             | A collection of all the generic transformation parameters for a table.                                               |
| [`AbstractTableTransformer`](#pudl.transform.classes.AbstractTableTransformer)     | An abstract base table transformer class.                                                                            |

## Functions

| [`multicol_transform_factory`](#pudl.transform.classes.multicol_transform_factory)(→ MultiColumnTransformFunc)   | Construct [`MultiColumnTransformFunc`](#pudl.transform.classes.MultiColumnTransformFunc) from a [`ColumnTransformFunc`](#pudl.transform.classes.ColumnTransformFunc).   |
|------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`rename_columns`](#pudl.transform.classes.rename_columns)(→ pandas.DataFrame)                                   | Rename the whole collection of dataframe columns using input params.                                                                                                    |
| [`normalize_strings`](#pudl.transform.classes.normalize_strings)(→ pandas.Series)                                | Derive a canonical, simplified version of the strings in the column.                                                                                                    |
| [`enforce_snake_case`](#pudl.transform.classes.enforce_snake_case)(→ pandas.Series)                              | Enforce snake_case for a column.                                                                                                                                        |
| [`strip_non_numeric_values`](#pudl.transform.classes.strip_non_numeric_values)(→ pandas.Series)                  | Strip a column of any non numeric values.                                                                                                                               |
| [`categorize_strings`](#pudl.transform.classes.categorize_strings)(→ pandas.Series)                              | Impose a controlled vocabulary on a freeform string column.                                                                                                             |
| [`convert_units`](#pudl.transform.classes.convert_units)(→ pandas.Series)                                        | Convert column units and rename the column to reflect the change.                                                                                                       |
| [`nullify_outliers`](#pudl.transform.classes.nullify_outliers)(→ pandas.Series)                                  | Set any values outside the valid range to NA.                                                                                                                           |
| [`correct_units`](#pudl.transform.classes.correct_units)(→ pandas.DataFrame)                                     | Correct outlying values based on inferred discrepancies in reported units.                                                                                              |
| [`drop_invalid_rows`](#pudl.transform.classes.drop_invalid_rows)(→ pandas.DataFrame)                             | Drop rows with only invalid values in all specified columns.                                                                                                            |
| [`replace_with_na`](#pudl.transform.classes.replace_with_na)(→ pandas.Series)                                    | Replace specified values with NA.                                                                                                                                       |
| [`spot_fix_values`](#pudl.transform.classes.spot_fix_values)(→ pandas.DataFrame)                                 | Manually fix one-off singular missing values and typos across a DataFrame.                                                                                              |
| [`cache_df`](#pudl.transform.classes.cache_df)(→ collections.abc.Callable[Ellipsis, ...)                         | A decorator for caching dataframes within an [`AbstractTableTransformer`](#pudl.transform.classes.AbstractTableTransformer).                                            |

## Module Contents

### pudl.transform.classes.logger

### *class* pudl.transform.classes.TransformParams(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

An immutable base model for transformation parameters.

`TransformParams` instances created without any arguments should have no effect
when applied by their associated function.

#### model_config

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

### *class* pudl.transform.classes.MultiColumnTransformParams(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

A dictionary of [`TransformParams`](#pudl.transform.classes.TransformParams) to apply to several columns in a table.

These parameter dictionaries are dynamically generated for each multi-column
transformation specified within a [`TableTransformParams`](#pudl.transform.classes.TableTransformParams) object, and passed
in to the [`MultiColumnTransformFunc`](#pudl.transform.classes.MultiColumnTransformFunc) callables which are constructed by
[`multicol_transform_factory()`](#pudl.transform.classes.multicol_transform_factory)

The keys are column names, values must all be the same type of
[`TransformParams`](#pudl.transform.classes.TransformParams) object. For examples, see e.g. the `categorize_strings`
or `convert_units` elements within
`pudl.transform.ferc1.TRANSFORM_PARAMS`.

The dictionary structure is not explicitly stated in this class, because it’s messy
to use Pydantic for validation when the data to be validated isn’t contained within
a Pydantic model. When Pydantic v2 is available, it will be easy, and we’ll do it:
[https://pydantic-docs.helpmanual.io/blog/pydantic-v2/#validation-without-a-model](https://pydantic-docs.helpmanual.io/blog/pydantic-v2/#validation-without-a-model)

#### single_param_type(info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Check that all TransformParams in the dictionary are of the same type.

### *class* pudl.transform.classes.ColumnTransformFunc

Bases: `Protocol`

Callback protocol defining a per-column transformation function.

#### \_\_call_\_(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [TransformParams](#pudl.transform.classes.TransformParams)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Create a callable.

### *class* pudl.transform.classes.TableTransformFunc

Bases: `Protocol`

Callback protocol defining a per-table transformation function.

#### \_\_call_\_(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [TransformParams](#pudl.transform.classes.TransformParams)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Create a callable.

### *class* pudl.transform.classes.MultiColumnTransformFunc

Bases: `Protocol`

Callback protocol defining a per-table transformation function.

#### \_\_call_\_(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [MultiColumnTransformParams](#pudl.transform.classes.MultiColumnTransformParams)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Create a callable.

### pudl.transform.classes.multicol_transform_factory(col_func: [ColumnTransformFunc](#pudl.transform.classes.ColumnTransformFunc), drop=True) → [MultiColumnTransformFunc](#pudl.transform.classes.MultiColumnTransformFunc)

Construct [`MultiColumnTransformFunc`](#pudl.transform.classes.MultiColumnTransformFunc) from a [`ColumnTransformFunc`](#pudl.transform.classes.ColumnTransformFunc).

This factory function saves us from having to iterate over dataframes in many
separate places, applying the same transform functions with different parameters to
multiple columns. Instead, we define a function that transforms a column given
some parameters, and then easily apply that function to many columns using a
dictionary of parameters (a [`MultiColumnTransformParams`](#pudl.transform.classes.MultiColumnTransformParams)). Uniform logging
output is also integrated into the constructed function.

* **Parameters:**
  **col_func** – A single column transform function.
* **Returns:**
  A multi-column transform function.

### Examples

```pycon
>>> class AddInt(TransformParams):
...     val: int
...
>>> def add_int(col: pd.Series, params: AddInt):
...     return col + params.val
...
>>> add_int_multicol = multicol_transform_factory(add_int)
...
>>> df = pd.DataFrame(
...     {
...         "col1": [1, 2, 3],
...         "col2": [10, 20, 30],
...     }
... )
...
>>> actual = add_int_multicol(
...     df,
...     params={
...         "col1": AddInt(val=1),
...         "col2": AddInt(val=2),
...     }
... )
...
>>> expected = pd.DataFrame(
...     {
...         "col1": [2, 3, 4],
...         "col2": [12, 22, 32],
...     }
... )
...
>>> pd.testing.assert_frame_equal(actual, expected)
```

### *class* pudl.transform.classes.RenameColumns(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

A dictionary for mapping old column names to new column names in a dataframe.

This parameter model has no associated transform function since it is used with the
`pd.DataFrame.rename()` method. Because it renames all of the columns in a
dataframe at once, it’s a table transformation (though it could also have been
implemented as a column transform).

#### columns *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

A dictionary of columns to be renamed.

### pudl.transform.classes.rename_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [RenameColumns](#pudl.transform.classes.RenameColumns) | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Rename the whole collection of dataframe columns using input params.

Raise an error if there’s any mismatch between the columns in the dataframe, and
the columns that have been defined in the mapping for renaming.

### *class* pudl.transform.classes.StringNormalization(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Options to control string normalization.

Most of what takes place in the string normalization is standardized and controlled
by the [`normalize_strings()`](#pudl.transform.classes.normalize_strings) function since we need the normalizations of
different columns to be comparable, but there are a couple of column-specific
parameterizations that are useful, and they are encapsulated by this class.

#### remove_chars *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

A string of individual ASCII characters removed at the end of normalization.

#### nullable *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

Whether the normalized string should be cast to `pd.StringDtype`.

### pudl.transform.classes.normalize_strings(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [StringNormalization](#pudl.transform.classes.StringNormalization)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Derive a canonical, simplified version of the strings in the column.

Transformations include:

* Convert to `pd.StringDtype`.
* Decompose composite unicode characters.
* Translate to ASCII character equivalents if they exist.
* Translate to lower case.
* Strip leading and trailing whitespace.
* Consolidate multiple internal whitespace characters into a single space.

* **Parameters:**
  * **col** – series of strings to normalize.
  * **params** – settings enumerating any particular characters to remove, and whether
    the resulting series should be a nullable string.

### pudl.transform.classes.normalize_strings_multicol

A multi-column version of the [`normalize_strings()`](#pudl.transform.classes.normalize_strings) function.

### *class* pudl.transform.classes.EnforceSnakeCase(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Boolean parameter for [`enforce_snake_case()`](#pudl.transform.classes.enforce_snake_case).

#### enforce_snake_case *: [bool](https://docs.python.org/3/library/functions.html#bool)*

### pudl.transform.classes.enforce_snake_case(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [EnforceSnakeCase](#pudl.transform.classes.EnforceSnakeCase) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Enforce snake_case for a column.

Removes leading whitespaces, lower-cases, replaces spaces with underscore and
removes remaining non alpha numeric snake case values.

* **Parameters:**
  * **col** – a column of strings.
  * **params** – an [`EnforceSnakeCase`](#pudl.transform.classes.EnforceSnakeCase) parameter object. Default is None which
    will instantiate an instance of [`EnforceSnakeCase`](#pudl.transform.classes.EnforceSnakeCase) where
    `enforce_snake_case` is `True`, which will enforce snake case on the
    `col`. If `enforce_snake_case` is `False`, the column will be
    returned unaltered.

### pudl.transform.classes.enforce_snake_case_multicol

### *class* pudl.transform.classes.StripNonNumericValues(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Boolean parameter for [`strip_non_numeric_values()`](#pudl.transform.classes.strip_non_numeric_values).

Stores a named boolean variable that is employed in
[`strip_non_numeric_values()`](#pudl.transform.classes.strip_non_numeric_values) to determine whether of not the transform
treatment should be applied. Pydantic 2.0 will allow validation of these simple
variables without needing to define a model.

#### strip_non_numeric_values *: [bool](https://docs.python.org/3/library/functions.html#bool)*

### pudl.transform.classes.strip_non_numeric_values(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [StripNonNumericValues](#pudl.transform.classes.StripNonNumericValues) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Strip a column of any non numeric values.

Using the following options in `pd.Series.extract()` :

* an optional `+` or `-` followed by at least one digit followed by an optional
  decimal place followed by any number of digits (including zero)
* OR an optional `+` or `-` followed by a period followed by at least one digit

Unless the found match is followed by a letter (this is done using a negative
lookback).

Note: This will not work with exponential values. If there are two possible matches
of numeric values within a value, only the first match will be returned (ex:
`"FERC1 Licenses 1234 & 5678"` will return `"1234"`).

### pudl.transform.classes.strip_non_numeric_values_multicol

### *class* pudl.transform.classes.StringCategories(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Mappings to categorize the values in freeform string columns.

#### categories *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

Mapping from a categorical string to the set of the values it should replace.

When specifying StringCategories in dictionary format, you may store either
a dict or a [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path) at this key. If a Path, it must point to a
YAML file which encodes the dictionary like so:

```default
categories: dict[str, set[str]] = {...}
with open("categoryfile.yml", "w") as f:
    yaml.dump(
        {
            cat: sorted(val)
            for cat, val in categories.items()
        },
        f
    )
```

We recommend putting any YAML files within a dataset directory in `src/pudl/package_data`.

#### na_category *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'na_category'*

All strings mapped to this category will be set to NA at the end.

The NA category is a special case because testing whether a value is NA is complex,
given the many different values which can be used to represent NA. See
[`categorize_strings()`](#pudl.transform.classes.categorize_strings) to see how it is used.

#### *classmethod* maybe_load_categories(v)

If categories was specified as a Path, load it from disk.

#### *classmethod* categories_are_disjoint(v)

Ensure that each string to be categorized only appears in one category.

#### *classmethod* categories_are_idempotent(v)

Ensure that every category contains the string it will map to.

This ensures that if the categorization is applied more than once, it doesn’t
change the output.

#### *property* mapping *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

A 1-to-1 mapping appropriate for use with `pd.Series.map()`.

### pudl.transform.classes.categorize_strings(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [StringCategories](#pudl.transform.classes.StringCategories)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Impose a controlled vocabulary on a freeform string column.

Note that any value present in the data that is not mapped to one of the output
categories will be set to NA.

### pudl.transform.classes.categorize_strings_multicol

A multi-column version of the [`categorize_strings()`](#pudl.transform.classes.categorize_strings) function.

### *class* pudl.transform.classes.UnitConversion(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

A column-wise unit conversion which can also rename the column.

Allows simple linear conversions of the form y(x) = a\*x + b. Note that the default
values result in no alteration of the column.

* **Parameters:**
  * **multiplier** – A multiplicative coefficient; “a” in the equation above. Set to 1.0
    by default.
  * **adder** – An additive constant; “b” in the equation above. Set to 0.0 by default.
  * **from_unit** – A string that will be replaced in the input series name. If None or
    the empty string, the series is not renamed.
  * **to_unit** – The string from_unit is replaced with. If None or the empty string,
    the series is not renamed. Note that either both or neither of `from_unit`
    and `to_unit` can be left unset, but not just one of them.

#### multiplier *: [float](https://docs.python.org/3/library/functions.html#float)* *= 1.0*

#### adder *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.0*

#### from_unit *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### to_unit *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

#### both_or_neither_units_are_none()

Ensure that either both or neither of the units strings are None.

#### inverse() → [UnitConversion](#pudl.transform.classes.UnitConversion)

Construct a [`UnitConversion`](#pudl.transform.classes.UnitConversion) that is the inverse of self.

Allows a unit conversion to be undone. This is currently used in the context of
validating the combination of `UnitConversions` that are used in the
[`UnitCorrections`](#pudl.transform.classes.UnitCorrections) parameter model.

#### *property* pattern *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Regular expression based on from_unit for use with [`re.sub()`](https://docs.python.org/3/library/re.html#re.sub).

#### *property* repl *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Regex backreference to parentheticals, for use with [`re.sub()`](https://docs.python.org/3/library/re.html#re.sub).

### pudl.transform.classes.convert_units(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [UnitConversion](#pudl.transform.classes.UnitConversion)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Convert column units and rename the column to reflect the change.

### pudl.transform.classes.convert_units_multicol

A multi-column version of the [`convert_units()`](#pudl.transform.classes.convert_units) function.

### *class* pudl.transform.classes.ValidRange(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Column level specification of min and/or max values.

#### lower_bound *: [float](https://docs.python.org/3/library/functions.html#float)*

#### upper_bound *: [float](https://docs.python.org/3/library/functions.html#float)*

#### *classmethod* upper_bound_gte_lower_bound(upper_bound: [float](https://docs.python.org/3/library/functions.html#float), info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo))

Require upper bound to be greater than or equal to lower bound.

### pudl.transform.classes.nullify_outliers(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [ValidRange](#pudl.transform.classes.ValidRange)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Set any values outside the valid range to NA.

The column is coerced to be numeric.

### pudl.transform.classes.nullify_outliers_multicol

A multi-column version of the [`nullify_outliers()`](#pudl.transform.classes.nullify_outliers) function.

### *class* pudl.transform.classes.UnitCorrections(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Fix outlying values resulting from apparent unit errors.

Note that since the unit correction depends on other columns in the dataframe to
select a relevant subset of records, it is a table transform not a column transform,
and so needs to know what column it applies to internally.

#### data_col *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

The label of the column to be modified.

#### cat_col *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Label of a categorical column which will be used to select records to correct.

#### cat_val *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Categorical value to use to select records for correction.

#### valid_range *: [ValidRange](#pudl.transform.classes.ValidRange)*

The range of values expected to be found in `data_col`.

#### unit_conversions *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[UnitConversion](#pudl.transform.classes.UnitConversion)]*

A list of unit conversions to use to identify errors and correct them.

#### *classmethod* no_column_rename(params: [list](https://docs.python.org/3/library/stdtypes.html#list)[[UnitConversion](#pudl.transform.classes.UnitConversion)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[UnitConversion](#pudl.transform.classes.UnitConversion)]

Ensure that the unit conversions used in corrections don’t rename the column.

This constraint is imposed so that the same unit conversion definitions can be
re-used both for unit corrections and normal columnwise unit conversions.

#### distinct_domains()

Verify that all unit conversions map distinct domains to the valid range.

If the domains being mapped to the valid range overlap, then it is ambiguous
which unit conversion should be applied to the original value.

* For all unit conversions calculate the range of original values that result
  from the inverse of the specified unit conversion applied to the valid
  ranges of values.
* For all pairs of unit conversions verify that their original data ranges do
  not overlap with each other. We must also ensure that the original and
  converted ranges of each individual correction do not overlap. For example, if
  the valid range is from 1 to 10, and the unit conversion multiplies by 3, we’d
  be unable to distinguish a valid value of 6 from a value that should be
  corrected to be 2.

### pudl.transform.classes.correct_units(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [UnitCorrections](#pudl.transform.classes.UnitCorrections)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Correct outlying values based on inferred discrepancies in reported units.

In many cases we know that a particular column in the database should have a value
within a particular range (e.g. the heat content of a ton of coal is a well defined
physical quantity – it can be 15 mmBTU/ton or 22 mmBTU/ton, but it can’t be 1
mmBTU/ton or 100 mmBTU/ton).

Sometimes these fields are reported in the wrong units (e.g. kWh of electricity
generated rather than MWh) resulting in several recognizable populations of reported
values showing up at different ranges of value within the data. In cases where the
unit conversion and range of valid values are such that these populations do not
overlap, it’s possible to convert them to the canonical units fairly unambiguously.

This issue is especially common in the context of fuel attributes, because fuels are
reported in terms of many different units. Because fuels with different units are
often reported in the same column, and different fuels have different valid ranges
of values, it’s also necessary to be able to select only a subset of the data that
pertains to a particular fuel. This means filtering based on another column, so the
function needs to have access to the whole dataframe.

Data values which are not found in one of the expected ranges are set to NA.

### *class* pudl.transform.classes.InvalidRows(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Pameters that identify invalid rows to drop.

#### invalid_values *: Annotated[[set](https://docs.python.org/3/library/stdtypes.html#set)[Any], [Field](../../metadata/classes/index.md#pudl.metadata.classes.Field)(min_length=1)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

A list of values that should be considered invalid in the selected columns.

#### required_valid_cols *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

List of columns passed into `pd.filter()` as the `items` argument.

#### allowed_invalid_cols *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

List of columns *not* to search for valid values to preserve.

Used to construct an `items` argument for `pd.filter()`. This option is useful
when a table is wide, and specifying all `required_valid_cols` would be tedious.

#### like *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

A string to use as the `like` argument to `pd.filter()`

#### regex *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

A regular expression to use as the `regex` argument to `pd.filter()`.

#### one_filter_argument()

Validate that only one argument is specified for `pd.filter()`.

### pudl.transform.classes.drop_invalid_rows(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [InvalidRows](#pudl.transform.classes.InvalidRows)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop rows with only invalid values in all specified columns.

This method finds all rows in a dataframe that contain ONLY invalid data in ALL of
the columns that we are checking, and drops those rows, logging the % of all rows
that were dropped.

### *class* pudl.transform.classes.ReplaceWithNa(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Pameters that replace certain values with NA.

The categorize strings function replaces bad values, but it requires all the values
in the column to fall under a certain category. This function allows you to replace
certain specific values with NA without having to categorize the rest of the column.

#### replace_with_na *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

A list of values that should be replaced with NA.

### pudl.transform.classes.replace_with_na(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), params: [ReplaceWithNa](#pudl.transform.classes.ReplaceWithNa)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Replace specified values with NA.

### pudl.transform.classes.replace_with_na_multicol

A multi-column version of the [`nullify_outliers()`](#pudl.transform.classes.nullify_outliers) function.

### *class* pudl.transform.classes.SpotFixes(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

Parameters that replace certain values with a manually corrected value.

#### idx_cols *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The column(s) used to identify a record.

#### fix_cols *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The column(s) to be fixed.

#### expect_unique *: [bool](https://docs.python.org/3/library/functions.html#bool)*

Set to True if each fix should correspond to only one row.

#### spot_fixes *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str) | [int](https://docs.python.org/3/library/functions.html#int) | [float](https://docs.python.org/3/library/functions.html#float) | [bool](https://docs.python.org/3/library/functions.html#bool), Ellipsis]]*

A tuple containing the values of the idx_cols and fix_cols for each fix.

### pudl.transform.classes.spot_fix_values(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [SpotFixes](#pudl.transform.classes.SpotFixes)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Manually fix one-off singular missing values and typos across a DataFrame.

Use this function to correct typos, missing values that are easily manually
identified through manual investigation of records, consistent issues for a small
number of records (e.g. incorrectly entered capacity data for 2-3 plants).

From an instance of [`SpotFixes`](#pudl.transform.classes.SpotFixes), this function takes a list of sets of
manual fixes and applies them to the specified records in a given dataframe. Each
set of fixes contains a list of identifying columns, a list of columns to be fixed,
and the values to be updated. A ValueError will be returned if spot-fixed datatypes
do not match those of the inputted dataframe. For each set of fixes, the
expect_unique parameter allows users to specify whether each fix should be applied
only to one row.

* **Returns:**
  The same input DataFrame but with some spot fixes corrected.

### *class* pudl.transform.classes.TableTransformParams(/, \*\*data: Any)

Bases: [`TransformParams`](#pudl.transform.classes.TransformParams)

A collection of all the generic transformation parameters for a table.

This class is used to instantiate and contain all of the individual
[`TransformParams`](#pudl.transform.classes.TransformParams) objects that are associated with transforming a given
table. It can be instantiated using one of the table-level dictionaries of
parameters defined in the dataset-specific modules in [`pudl.transform.params`](../params/index.md#module-pudl.transform.params)

Data source-specific [`TableTransformParams`](#pudl.transform.classes.TableTransformParams) classes should be defined in
the data source-specific transform modules and inherit from this class. See e.g.
[`pudl.transform.ferc1.Ferc1TableTransformParams`](../ferc1/index.md#pudl.transform.ferc1.Ferc1TableTransformParams)

#### convert_units *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [UnitConversion](#pudl.transform.classes.UnitConversion)]*

#### categorize_strings *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [StringCategories](#pudl.transform.classes.StringCategories)]*

#### nullify_outliers *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ValidRange](#pudl.transform.classes.ValidRange)]*

#### normalize_strings *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [StringNormalization](#pudl.transform.classes.StringNormalization)]*

#### strip_non_numeric_values *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [StripNonNumericValues](#pudl.transform.classes.StripNonNumericValues)]*

#### replace_with_na *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ReplaceWithNa](#pudl.transform.classes.ReplaceWithNa)]*

#### correct_units *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[UnitCorrections](#pudl.transform.classes.UnitCorrections)]* *= []*

#### rename_columns *: [RenameColumns](#pudl.transform.classes.RenameColumns)*

#### drop_invalid_rows *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[InvalidRows](#pudl.transform.classes.InvalidRows)]* *= []*

#### spot_fix_values *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[SpotFixes](#pudl.transform.classes.SpotFixes)]* *= []*

#### *classmethod* from_dict(params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [TableTransformParams](#pudl.transform.classes.TableTransformParams)

Construct `TableTransformParams` from a dictionary of keyword arguments.

Typically these will be the table-level dictionaries defined in the dataset-
specific modules in the [`pudl.transform.params`](../params/index.md#module-pudl.transform.params) subpackage. See also the
[`TableTransformParams.from_id()`](#pudl.transform.classes.TableTransformParams.from_id) method.

#### *classmethod* from_id(table_id: [enum.Enum](https://docs.python.org/3/library/enum.html#enum.Enum)) → [TableTransformParams](#pudl.transform.classes.TableTransformParams)

A factory method that looks up transform parameters based on table_id.

This is a shortcut, which allows us to constitute the parameter models based on
the table they are associated with without having to pass in a potentially large
nested data structure, which gets messy in Dagster.

### pudl.transform.classes.cache_df(key: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'main') → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[Ellipsis, [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

A decorator for caching dataframes within an [`AbstractTableTransformer`](#pudl.transform.classes.AbstractTableTransformer).

It’s often useful during development or debugging to be able to track the evolution
of data as it passes through several transformation steps. Especially when some of
the steps are time consuming, it’s nice to still get a copy of the last known state
of the data when a transform raises an exception and fails.

This decorator lets you easily save a copy of the dataframe being returned by a
class method for later reference, before moving on to the next step. Each unique key
used within a given [`AbstractTableTransformer`](#pudl.transform.classes.AbstractTableTransformer) instance results in a new
dataframe being cached. Re-using the same key will overwrite previously cached
dataframes that were stored with that key.

Saving many intermediate steps can provide lots of detailed information, but will
use more memory. Updating the same cached dataframe as it successfully passes
through each step lets you access the last known state it had before an error
occurred.

This decorator requires that the decorated function return a single
`pd.DataFrame`, but it can take any type of inputs.

There’s a lot of nested functions in here. For a more thorough explanation, see:
[https://realpython.com/primer-on-python-decorators/#fancy-decorators](https://realpython.com/primer-on-python-decorators/#fancy-decorators)

* **Parameters:**
  **key** – The key that will be used to store and look up the cached dataframe in the
  internal `self._cached_dfs` dictionary.
* **Returns:**
  The decorated class method.

### *class* pudl.transform.classes.AbstractTableTransformer(params: [TableTransformParams](#pudl.transform.classes.TableTransformParams) | [None](https://docs.python.org/3/library/constants.html#None) = None, cache_dfs: [bool](https://docs.python.org/3/library/functions.html#bool) = False, clear_cached_dfs: [bool](https://docs.python.org/3/library/functions.html#bool) = True, \*\*kwargs)

Bases: [`abc.ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

An abstract base table transformer class.

This class provides methods for applying the general purpose transform functions to
dataframes. These methods should each log that they are running, and the
`table_id` of the table they’re beiing applied to. By default they should obtain
their parameters from the `params` which are stored in the class, but should allow
other parameters to be passed in.

The class also provides a template for coordinating the high level flow of data
through the transformations. The main coordinating function that’s used to run the
full transformation is [`AbstractTableTransformer.transform()`](#pudl.transform.classes.AbstractTableTransformer.transform), and the transform
is broken down into 3 distinct steps: start, main, and end. Those individual steps
need to be defined by child classes. Usually the start and end methods will handle
transformations that need to be applied uniformily across all the tables in a given
dataset, with the main step containing transformations that are specific to a
particular table.

In development it’s often useful to be able to review the state of the data at
various stages as it progresses through the transformation. The [`cache_df()`](#pudl.transform.classes.cache_df)
decorator defined above can be applied to individual transform methods or the
start, main, and end methods defined in the child classes, to allow intermediate
dataframes to be reviewed after the fact. Whether to cache dataframes and whether to
delete them upon successful completion of the transform is controlled by flags set
when the `TableTransformer` class is created.

Table-specific transform parameters need to be associated with the class. They can
either be passed in explicitly when the class is instantiated, or looked up based on
the `table_id` associated with the class. See [`TableTransformParams.from_id()`](#pudl.transform.classes.TableTransformParams.from_id)

The call signature of the [`AbstractTableTransformer.transform_start()`](#pudl.transform.classes.AbstractTableTransformer.transform_start) method
accepts any type of inputs by default, and returns a single `pd.DataFrame`.
Later transform steps are assumed to take a single dataframe as input, and return a
single dataframe. Since Python is lazy about enforcing types and interfaces you can
get away with other kinds of arguments when they’re sometimes necessary, but this
isn’t a good arrangement and we should figure out how to do it right. See the
[`pudl.transform.ferc1.SteamPlantsTableTransformer`](../ferc1/index.md#pudl.transform.ferc1.SteamPlantsTableTransformer) class for an example.

#### table_id *: [enum.Enum](https://docs.python.org/3/library/enum.html#enum.Enum)*

Name of the PUDL database table that this table transformer produces.

Must be defined in the database schema / metadata. This ID is used to instantiate
the appropriate [`TableTransformParams`](#pudl.transform.classes.TableTransformParams) object.

#### cache_dfs *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

Whether to cache copies of intermediate dataframes until transformation is done.

When True, the TableTransformer will save dataframes internally at each step of the
transform, so that they can be inspected easily if the transformation fails.

#### clear_cached_dfs *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

Determines whether cached dataframes are deleted at the end of the transform.

#### \_cached_dfs *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]*

Cached intermediate dataframes for use in development and debugging.

The dictionary keys are the strings passed to the [`cache_df()`](#pudl.transform.classes.cache_df) method decorator.

#### parameter_model

The `pydantic` model that is used to contain & instantiate parameters.

In child classes this should be replaced with the data source-specific
[`TableTransformParams`](#pudl.transform.classes.TableTransformParams) class, if it has been defined.

#### params *: [AbstractTableTransformer.parameter_model](#pudl.transform.classes.AbstractTableTransformer.parameter_model)*

The parameters that will be used to control the transformation functions.

This attribute is of type `parameter_model` which is defined above. This type
varies across datasets and is used to construct and validate the parameters based,
so it needs to be set separately in child classes. See
[`pudl.transform.ferc1.Ferc1AbstractTableTransformer`](../ferc1/index.md#pudl.transform.ferc1.Ferc1AbstractTableTransformer) for an example.

#### *abstractmethod* transform_start(\*args, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transformations applied to many tables within a dataset at the beginning.

This method should be implemented by the dataset-level abstract table
transformer class. It does not specify its inputs because different data sources
need different inputs. E.g. the FERC 1 transform needs 2 XBRL derived
dataframes, and one DBF derived dataframe, while (most) EIA tables just receive
and return a single dataframe.

This step is often used to organize initial transformations that are applied
uniformly across all the tables in a dataset.

At the end of this step, all the inputs should have been consolidated into a
single dataframe to return.

#### *abstractmethod* transform_main(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

The method used to do most of the table-specific transformations.

Typically the transformations grouped together into this method will be unique
to the table that is being transformed. Generally this method will take and
return a single dataframe, and that pattern is implemented in the
[`AbstractTableTransformer.transform()`](#pudl.transform.classes.AbstractTableTransformer.transform) method. In cases where transforms
take or return more than one dataframe, you will need to define a new transform
method within the child class. See `SteamPlantsTableTransformer`
as an example.

#### *abstractmethod* transform_end(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transformations applied to many tables within a dataset at the end.

This method should be implemented by the dataset-level abstract table
transformer class. It should do any standard cleanup that’s required after the
table-specific transformations have been applied. E.g. enforcing the table’s
database schema and dropping invalid records based on parameterized criteria.

#### transform(\*args, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Apply all specified transformations to the appropriate input dataframes.

#### rename_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [RenameColumns](#pudl.transform.classes.RenameColumns) | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Rename the whole collection of dataframe columns using input params.

Raise an error if there’s any mismatch between the columns in the dataframe, and
the columns that have been defined in the mapping for renaming.

#### normalize_strings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [bool](https://docs.python.org/3/library/functions.html#bool)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Method wrapper for string normalization.

#### strip_non_numeric_values(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [bool](https://docs.python.org/3/library/functions.html#bool)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Method wrapper for stripping non-numeric values.

#### categorize_strings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [StringCategories](#pudl.transform.classes.StringCategories)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Method wrapper for string categorization.

#### nullify_outliers(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ValidRange](#pudl.transform.classes.ValidRange)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Method wrapper for nullifying outlying values.

#### convert_units(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [UnitConversion](#pudl.transform.classes.UnitConversion)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Method wrapper for columnwise unit conversions.

#### correct_units(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [UnitCorrections](#pudl.transform.classes.UnitCorrections) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Apply all specified unit corrections to the table in order.

Note: this is a table transform, not a multi-column transform.

#### drop_invalid_rows(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [list](https://docs.python.org/3/library/stdtypes.html#list)[[InvalidRows](#pudl.transform.classes.InvalidRows)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop rows with only invalid values in all specified columns.

#### replace_with_na(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ReplaceWithNa](#pudl.transform.classes.ReplaceWithNa)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Replace specified values with NA.

#### spot_fix_values(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), params: [list](https://docs.python.org/3/library/stdtypes.html#list)[[SpotFixes](#pudl.transform.classes.SpotFixes)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Replace specified values with specified values.

#### enforce_schema(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop columns not in the DB schema and enforce specified types.
