"""Classes for defining & coordinating the transformation of tabular data sources.

We define our data transformations in four separate components:

  * The data being transformed (:class:`pd.DataFrame` or :class:`pd.Series`).
  * The functions & methods doing the transformations.
  * Non-data parameters that control the behavior of the transform functions & methods.
  * Classes that organize the functions & parameters that transform a given input table.

Separating out the transformation functions and the parameters that control them allows
us to re-use the same transforms in many different contexts without duplicating the
code.

Transform functions take data (either a Series or DataFrame) and a TransformParams
object as inputs, and return transformed data of the same type that they consumed
(Series or DataFrame). They operate on the data, and their particular behavior is
controled by the TransformParams. Like the TableTransformer classes discussed below,
they are organized into 3 separate levels of abstraction:

  * general-purpose: always available from the abstract base class.
  * dataset-specific: used repeatedly by a dataset, from an intermediate abstract class.
  * table-specific: used only once for a particular table, defined in a concrete class.

These functions are not generally meant to be used independent of a ``TableTransfomer``
class. They are wrapped by methods within the class definitions which handle logging
and intermediate dataframe caching.

  * Transform functions that operate on individual columns should implement the
    :class:`ColumnTransformFunc` :class:`Protocol`.
  * Transform functions that need to operate on whole tables should implement the
    :class:`TableTransformFunc` :class:`Protocol`.
  * To iteratively apply a :class:`ColumnTransformFunc` to several columns in a table,
    use :func:`multicol_transform_factory` to construct a
    :class:`MultiColumnTransformFunc`

Using a hierarchy of ``TableTransformer`` classes to organize the functions and
parameters allows us to apply a particular set of transformations uniformly across every
table that's part of a family of similar data. It also allows us to keep transform
functions that only apply to a particular collection of tables or an individual table
separated from other data that it should not be used with.

Currently there are 3 levels of abstraction in the TableTransformer classes:

  * The :class:`AbstractTableTransformer` abstract base class that defines methods
    useful across a wide range of data sources.
  * A dataset-specific abstract class that can define transforms which are consistently
    useful across many tables in the dataset (e.g. the
    :class:`pudl.transform.ferc1.Ferc1AbstractTableTransformer` class).
  * Table-specific concrete classes that inherit from both of the higher levels, and
    contain any bespoke transformations or parameters that only pertain to that table.
    (e.g. the :class:`pudl.transform.ferc1.SteamPlantsFerc1TableTransformer` class).

The :class:`TransformParams` classes are immutable :mod:`pydantic` models that store and
the parameters which are passed to the transform functions / methods described above.
These models are defined alongside the functions they're used with. General purpose
transforms have their parameter models defined in this module. Dataset-specific
transforms should have their parameters defined in the module that defines the
associated transform function. The :class:`MultiColumnTransformParams` models are
dictionaries keyed by column name, that must map to per-column parameters which are all
of the same type.

Specific :class:`TransformParams` classes are instantiated using dictionaries of values
defined in the per-dataset modules under :mod:`pudl.transform.params` e.g.
:mod:`pudl.transform.params.ferc1`.
"""
import enum
import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import wraps
from itertools import combinations
from typing import Any, Protocol

import numpy as np
import pandas as pd
from pydantic import BaseModel, conset, root_validator, validator

import pudl.logging_helpers
import pudl.transform.params.ferc1
from pudl.metadata.classes import Package

logger = pudl.logging_helpers.get_logger(__name__)


#####################################################################################
# Transform Parameter Models
#####################################################################################
class TransformParams(BaseModel):
    """An immutable base model for transformation parameters.

    ``TransformParams`` instances created without any arguments should have no effect
    when applied by their associated function.
    """

    class Config:
        """Prevent parameters from changing part way through."""

        allow_mutation = False
        extra = "forbid"


class MultiColumnTransformParams(TransformParams):
    """A dictionary of :class:`TransformParams` to apply to several columns in a table.

    These parameter dictionaries are dynamically generated for each multi-column
    transformation specified within a :class:`TableTransformParams` object, and passed
    in to the :class:`MultiColumnTransformFunc` callables which are constructed by
    :func:`multicol_transform_factory`

    The keys are column names, values must all be the same type of
    :class:`TransformParams` object. For examples, see e.g. the ``categorize_strings``
    or ``convert_units`` elements within
    :py:const:`pudl.transform.ferc1.TRANSFORM_PARAMS`.

    The dictionary structure is not explicitly stated in this class, because it's messy
    to use Pydantic for validation when the data to be validated isn't contained within
    a Pydantic model. When Pydantic v2 is available, it will be easy, and we'll do it:
    https://pydantic-docs.helpmanual.io/blog/pydantic-v2/#validation-without-a-model
    """

    @root_validator
    def single_param_type(cls, params):  # noqa: N805
        """Check that all TransformParams in the dictionary are of the same type."""
        param_types = {type(params[col]) for col in params}
        if len(param_types) > 1:
            raise ValueError(
                "Found multiple parameter types in multi-column transform params: "
                f"{param_types}"
            )
        return params


#####################################################################################
# Transform Protocol & General Function Definitions
# Factory functions and callback protocols are a little complex. For more on how
# Protocols and type annotations can be used to define function call
# signatures and return types see:
# https://realpython.com/python-type-checking/#callables
# https://mypy.readthedocs.io/en/stable/protocols.html#callback-protocols
#####################################################################################
class ColumnTransformFunc(Protocol):
    """Callback protocol defining a per-column transformation function."""

    def __call__(self, col: pd.Series, params: TransformParams) -> pd.Series:
        """Create a callable."""
        ...


class TableTransformFunc(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(self, df: pd.DataFrame, params: TransformParams) -> pd.DataFrame:
        """Create a callable."""
        ...


class MultiColumnTransformFunc(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(
        self, df: pd.DataFrame, params: MultiColumnTransformParams
    ) -> pd.DataFrame:
        """Create a callable."""
        ...


def multicol_transform_factory(
    col_func: ColumnTransformFunc,
    drop=True,
) -> MultiColumnTransformFunc:
    """Construct :class:`MultiColumnTransformFunc` from a :class:`ColumnTransformFunc`.

    This factory function saves us from having to iterate over dataframes in many
    separate places, applying the same transform functions with different parameters to
    multiple columns. Instead, we define a function that transforms a column given
    some parameters, and then easily apply that function to many columns using a
    dictionary of parameters (a :class:`MultiColumnTransformParams`). Uniform logging
    output is also integrated into the constructed function.

    Args:
        col_func: A single column transform function.

    Returns:
        A multi-column transform function.

    Examples:
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
    """

    class InnerMultiColumnTransformFunc(
        Callable[[pd.DataFrame, MultiColumnTransformParams], pd.DataFrame]
    ):
        __name__ = col_func.__name__ + "_multicol"

        def __call__(
            self, df: pd.DataFrame, params: MultiColumnTransformParams
        ) -> pd.DataFrame:
            drop_col: bool = drop
            for col_name in params:
                if col_name in df.columns:
                    logger.debug(f"Applying {col_func.__name__} to {col_name}")
                    new_col = col_func(col=df[col_name], params=params[col_name])
                    if drop_col:
                        df = df.drop(columns=col_name)
                    df = pd.concat([df, new_col], axis="columns")
                else:
                    logger.warning(
                        f"Expected column {col_name} not found in dataframe during "
                        f"application of {col_func.__name__}."
                    )
            return df

    return InnerMultiColumnTransformFunc()


################################################################################
# Rename Columns
################################################################################
class RenameColumns(TransformParams):
    """A dictionary for mapping old column names to new column names in a dataframe.

    This parameter model has no associated transform function since it is used with the
    :meth:`pd.DataFrame.rename` method. Because it renames all of the columns in a
    dataframe at once, it's a table transformation (though it could also have been
    implemented as a column transform).
    """

    columns: dict[str, str] = {}


################################################################################
# Normalize Strings
################################################################################
class StringNormalization(TransformParams):
    """Options to control string normalization.

    Most of what takes place in the string normalization is standardized and controlled
    by the :func:`normalize_strings` function since we need the normalizations of
    different columns to be comparable, but there are a couple of column-specific
    parameterizations that are useful, and they are encapsulated by this class.
    """

    remove_chars: str
    """A string of individual ASCII characters removed at the end of normalization."""

    nullable: bool = False
    """Whether the normalized string should be cast to :class:`pd.StringDtype`."""


def normalize_strings(col: pd.Series, params: StringNormalization) -> pd.Series:
    """Derive a canonical, simplified version of the strings in the column.

    Transformations include:

    * Convert to :class:`pd.StringDtype`.
    * Decompose composite unicode characters.
    * Translate to ASCII character equivalents if they exist.
    * Translate to lower case.
    * Strip leading and trailing whitespace.
    * Consolidate multiple internal whitespace characters into a single space.

    Args:
        col: series of strings to normalize.
        params: settings enumerating any particular characters to remove, and whether
            the resulting series should be a nullable string.
    """
    if params:
        col = (
            col.astype(pd.StringDtype())
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("ascii", errors="ignore")
            .str.lower()
            .str.replace(r"[" + params.remove_chars + r"]+", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        if not params.nullable:
            col = col.fillna("").astype(str)
    return col


normalize_strings_multicol = multicol_transform_factory(normalize_strings)
"""A multi-column version of the :func:`normalize_strings` function."""


################################################################################
# Enforce Snake Case
################################################################################
class EnforceSnakeCase(TransformParams):
    """Boolean parameter for :func:`enforce_snake_case`."""

    enforce_snake_case: bool


def enforce_snake_case(
    col: pd.Series, params: EnforceSnakeCase | None = None
) -> pd.Series:
    """Enforce snake_case for a column.

    Removes leading whitespaces, lower-cases, replaces spaces with underscore and
    removes remaining non alpha numeric snake case values.

    Args:
        col: a column of strings.
        params: an :class:`EnforceSnakeCase` parameter object. Default is None which
            will instantiate an instance of :class:`EnforceSnakeCase` where
            ``enforce_snake_case`` is ``True``, which will enforce snake case on the
            ``col``. If ``enforce_snake_case`` is ``False``, the column will be
            returned unaltered.
    """
    if params is None:
        params = EnforceSnakeCase(enforce_snake_case=True)
    if params.enforce_snake_case:
        col = (
            col.str.strip()
            .str.lower()
            .replace(
                to_replace={
                    r"\s{1,}": "_",
                    r"[^a-z\d_]": "",
                },
                regex=True,
            )
        )
    return col


enforce_snake_case_multicol = multicol_transform_factory(enforce_snake_case)


################################################################################
# Strip Non-Numeric Values
################################################################################
class StripNonNumericValues(TransformParams):
    """Boolean parameter for :func:`strip_non_numeric_values`.

    Stores a named boolean variable that is employed in
    :func:`strip_non_numeric_values` to determine whether of not the transform
    treatment should be applied. Pydantic 2.0 will allow validation of these simple
    variables without needing to define a model.
    """

    strip_non_numeric_values: bool


def strip_non_numeric_values(
    col: pd.Series, params: StripNonNumericValues | None = None
) -> pd.Series:
    """Strip a column of any non numeric values.

    Using the following options in :func:`pd.Series.extract` :

    * an optional ``+`` or ``-`` followed by at least one digit followed by an optional
      decimal place followed by any number of digits (including zero)
    * OR an optional ``+`` or ``-`` followed by a period followed by at least one digit

    Unless the found mathc is followed by a letter (this is done using a negative
    lookback).

    Note: This will not work with exponential values. If there are two possible matches
    of numeric values within a value, only the first match will be returned (ex:
    ``"FERC1 Licenses 1234 & 5678"`` will return ``"1234"``).
    """
    if params is None:
        params = StripNonNumericValues(strip_non_numeric_values=True)
    if params.strip_non_numeric_values:
        col = col.astype(str).str.extract(
            rf"(?P<{col.name}>(?<![a-z-A-Z])[-+]?\d+\.?\d*|[-+]?\.\d+)",  # name the series
            expand=False,
        )
    return col


strip_non_numeric_values_multicol = multicol_transform_factory(strip_non_numeric_values)


################################################################################
# Categorize Strings
################################################################################
class StringCategories(TransformParams):
    """Mappings to categorize the values in freeform string columns."""

    categories: dict[str, set[str]]
    """Mapping from a categorical string to the set of the values it should replace."""

    na_category: str = "na_category"
    """All strings mapped to this category will be set to NA at the end.

    The NA category is a special case because testing whether a value is NA is complex,
    given the many different values which can be used to represent NA. See
    :func:`categorize_strings` to see how it is used.
    """

    @validator("categories")
    def categories_are_disjoint(cls, v):
        """Ensure that each string to be categorized only appears in one category."""
        for cat1, cat2 in combinations(v, 2):
            intersection = set(v[cat1]).intersection(v[cat2])
            if intersection:
                raise ValueError(
                    f"String categories are not disjoint. {cat1} and {cat2} both "
                    f"contain these values: {intersection}"
                )
        return v

    @validator("categories")
    def categories_are_idempotent(cls, v):
        """Ensure that every category contains the string it will map to.

        This ensures that if the categorization is applied more than once, it doesn't
        change the output.
        """
        for cat in v:
            if cat not in v[cat]:
                logger.info(f"String category {cat} does not map to itself. Adding it.")
                v[cat] = v[cat].union({cat})
        return v

    @property
    def mapping(self) -> dict[str, str]:
        """A 1-to-1 mapping appropriate for use with :meth:`pd.Series.map`."""
        return {
            string: cat for cat in self.categories for string in self.categories[cat]
        }


def categorize_strings(col: pd.Series, params: StringCategories) -> pd.Series:
    """Impose a controlled vocabulary on a freeform string column.

    Note that any value present in the data that is not mapped to one of the output
    categories will be set to NA.
    """
    uncategorized_strings = set(col).difference(params.mapping)
    if uncategorized_strings:
        logger.warning(
            f"{col.name}: Found {len(uncategorized_strings)} uncategorized values: "
            f"{uncategorized_strings}"
        )
    col = col.map(params.mapping).astype(pd.StringDtype())
    col.loc[col == params.na_category] = pd.NA
    return col


categorize_strings_multicol = multicol_transform_factory(categorize_strings)
"""A multi-column version of the :func:`categorize_strings` function."""


################################################################################
# Convert Units
################################################################################
class UnitConversion(TransformParams):
    """A column-wise unit conversion which can also rename the column.

    Allows simple linear conversions of the form y(x) = a*x + b. Note that the default
    values result in no alteration of the column.

    Args:
        multiplier: A multiplicative coefficient; "a" in the equation above. Set to 1.0
            by default.
        adder: An additive constant; "b" in the equation above. Set to 0.0 by default.
        from_unit: A string that will be replaced in the input series name. If None or
            the empty string, the series is not renamed.
        to_unit: The string from_unit is replaced with. If None or the empty string,
            the series is not renamed. Note that either both or neither of ``from_unit``
            and ``to_unit`` can be left unset, but not just one of them.
    """

    multiplier: float = 1.0  # By default, multiply by 1 (no change)
    adder: float = 0.0  # By default, add 0 (no change)
    from_unit: str = ""  # If it's the empty string, no renaming will happen.
    to_unit: str = ""  # If it's the empty string, no renaming will happen.

    @root_validator
    def both_or_neither_units_are_none(cls, params):
        """Ensure that either both or neither of the units strings are None."""
        if (params["from_unit"] == "" and params["to_unit"] != "") or (
            params["from_unit"] != "" and params["to_unit"] == ""
        ):
            raise ValueError(
                "Either both or neither of from_unit and to_unit must be non-empty. "
                f"Got {params['from_unit']=} {params['to_unit']=}."
            )
        return params

    def inverse(self) -> "UnitConversion":
        """Construct a :class:`UnitConversion` that is the inverse of self.

        Allows a unit conversion to be undone. This is currently used in the context of
        validating the combination of :class:`UnitConversions` that are used in the
        :class:`UnitCorrections` parameter model.
        """
        return UnitConversion(
            multiplier=1.0 / self.multiplier,
            adder=-1.0 * (self.adder / self.multiplier),
            from_unit=self.to_unit,
            to_unit=self.from_unit,
        )

    @property
    def pattern(self) -> str:
        """Regular expression based on from_unit for use with :func:`re.sub`."""
        return r"^(.*)" + self.from_unit + r"(.*)$"

    @property
    def repl(self) -> str:
        """Regex backreference to parentheticals, for use with :func:`re.sub`."""
        return r"\1" + self.to_unit + r"\2"


def convert_units(col: pd.Series, params: UnitConversion) -> pd.Series:
    """Convert column units and rename the column to reflect the change."""
    if params.from_unit is not None:
        new_name = re.sub(pattern=params.pattern, repl=params.repl, string=col.name)
        # only apply the unit conversion if the column name matched the pattern
        if not re.match(pattern=params.pattern, string=col.name):
            logger.warning(
                f"{col.name} did not match the unit rename pattern. Check for typos "
                "and make sure you're applying the conversion to an appropriate column."
            )
    else:
        new_name = col.name
    if (col.name == new_name) & (params.from_unit != "") & (params.to_unit != ""):
        logger.debug(f"Old and new column names are identical: {col.name}.")
    col = (params.multiplier * col) + params.adder
    col.name = new_name
    return col


convert_units_multicol = multicol_transform_factory(convert_units)
"""A multi-column version of the :func:`convert_units` function."""


################################################################################
# Nullify outlying values not within the valid range
################################################################################
class ValidRange(TransformParams):
    """Column level specification of min and/or max values."""

    lower_bound: float = -np.inf
    upper_bound: float = np.inf

    @validator("upper_bound")
    def upper_bound_gte_lower_bound(cls, v, values):
        """Require upper bound to be greater than or equal to lower bound."""
        if values["lower_bound"] > v:
            raise ValueError("upper_bound must be greater than or equal to lower_bound")
        return v


def nullify_outliers(col: pd.Series, params: ValidRange) -> pd.Series:
    """Set any values outside the valid range to NA.

    The column is coerced to be numeric.
    """
    # Surprisingly, pd.to_numeric() did *not* return a copy of the series!
    col = col.copy()
    col = pd.to_numeric(col, errors="coerce")
    col[~col.between(params.lower_bound, params.upper_bound)] = np.nan
    return col


nullify_outliers_multicol = multicol_transform_factory(nullify_outliers)
"""A multi-column version of the :func:`nullify_outliers` function."""


################################################################################
# Correct units based on inferred data entry errors or implicit units.
################################################################################
class UnitCorrections(TransformParams):
    """Fix outlying values resulting from apparent unit errors.

    Note that since the unit correction depends on other columns in the dataframe to
    select a relevant subset of records, it is a table transform not a column transform,
    and so needs to know what column it applies to internally.
    """

    data_col: str
    """The label of the column to be modified."""

    cat_col: str
    """Label of a categorical column which will be used to select records to correct."""

    cat_val: str
    """Categorical value to use to select records for correction."""

    valid_range: ValidRange
    """The range of values expected to be found in ``data_col``."""

    unit_conversions: list[UnitConversion]
    """A list of unit conversions to use to identify errors and correct them."""

    @validator("unit_conversions")
    def no_column_rename(cls, params: list[UnitConversion]) -> list[UnitConversion]:
        """Ensure that the unit conversions used in corrections don't rename the column.

        This constraint is imposed so that the same unit conversion definitions can be
        re-used both for unit corrections and normal columnwise unit conversions.
        """
        new_conversions = []
        for uc in params:
            new_conversions.append(
                UnitConversion(multiplier=uc.multiplier, adder=uc.adder)
            )
        return new_conversions

    @root_validator
    def distinct_domains(cls, params):
        """Verify that all unit conversions map distinct domains to the valid range.

        If the domains being mapped to the valid range overlap, then it is ambiguous
        which unit conversion should be applied to the original value.

        * For all unit conversions calculate the range of original values that result
          from the inverse of the specified unit conversion applied to the valid
          ranges of values.
        * For all pairs of unit conversions verify that their original data ranges do
          not overlap with each other. We must also ensure that the original and
          converted ranges of each individual correction do not overlap. For example, if
          the valid range is from 1 to 10, and the unit conversion multiplies by 3, we'd
          be unable to distinguish a valid value of 6 from a value that should be
          corrected to be 2.
        """
        input_vals = pd.Series(
            [params["valid_range"].lower_bound, params["valid_range"].upper_bound],
            name="dude",
        )
        # We need to make sure that the unit conversion doesn't map the valid range
        # onto itself either, so add an additional conversion that does nothing:
        uc_combos = combinations(params["unit_conversions"] + [UnitConversion()], 2)
        for uc1, uc2 in uc_combos:
            out1 = convert_units(input_vals, uc1.inverse())
            out2 = convert_units(input_vals, uc2.inverse())
            if not ((min(out1) > max(out2)) or (max(out1) < min(out2))):
                raise ValueError(
                    "The following pair of unit corrections are incompatible due to "
                    "overlapping domains.\n"
                    f"{params['valid_range']=}\n"
                    f"{uc1=}\n"
                    f"{uc2=}\n"
                )
        return params


def correct_units(df: pd.DataFrame, params: UnitCorrections) -> pd.DataFrame:
    """Correct outlying values based on inferred discrepancies in reported units.

    In many cases we know that a particular column in the database should have a value
    within a particular range (e.g. the heat content of a ton of coal is a well defined
    physical quantity -- it can be 15 mmBTU/ton or 22 mmBTU/ton, but it can't be 1
    mmBTU/ton or 100 mmBTU/ton).

    Sometimes these fields are reported in the wrong units (e.g. kWh of electricity
    generated rather than MWh) resulting in several recognizable populations of reported
    values showing up at different ranges of value within the data. In cases where the
    unit conversion and range of valid values are such that these populations do not
    overlap, it's possible to convert them to the canonical units fairly unambiguously.

    This issue is especially common in the context of fuel attributes, because fuels are
    reported in terms of many different units. Because fuels with different units are
    often reported in the same column, and different fuels have different valid ranges
    of values, it's also necessary to be able to select only a subset of the data that
    pertains to a particular fuel. This means filtering based on another column, so the
    function needs to have access to the whole dataframe.

    Data values which are not found in one of the expected ranges are set to NA.
    """
    logger.info(
        f"Correcting units of {params.data_col} "
        f"where {params.cat_col}=={params.cat_val}."
    )
    # Select a subset of the input dataframe to work on. E.g. only the heat content
    # column for coal records:
    selected = df.loc[df[params.cat_col] == params.cat_val, params.data_col]
    not_selected = df[params.data_col].drop(index=selected.index)

    # Now, we only want to alter the subset of these values which, when transformed by
    # the unit conversion, lie in the range of valid values.
    for uc in params.unit_conversions:
        converted = convert_units(col=selected, params=uc)
        converted = nullify_outliers(col=converted, params=params.valid_range)
        selected = selected.where(converted.isna(), converted)

    # Nullify outliers that remain after the corrections have been applied.
    na_before = sum(selected.isna())
    selected = nullify_outliers(col=selected, params=params.valid_range)
    na_after = sum(selected.isna())
    total_nullified = na_after - na_before
    logger.info(
        f"{total_nullified}/{len(selected)} ({total_nullified/len(selected):.2%}) "
        "of records could not be corrected and were set to NA."
    )
    # Combine our cleaned up values with the other values we didn't select.
    df = df.copy()
    df[params.data_col] = pd.concat([selected, not_selected])
    return df


################################################################################
# Drop invalid rows
################################################################################
class InvalidRows(TransformParams):
    """Pameters that identify invalid rows to drop."""

    invalid_values: conset(Any, min_items=1) | None = None
    """A list of values that should be considered invalid in the selected columns."""

    required_valid_cols: list[str] | None = None
    """List of columns passed into :meth:`pd.filter` as the ``items`` argument."""

    allowed_invalid_cols: list[str] | None = None
    """List of columns *not* to search for valid values to preserve.

    Used to construct an ``items`` argument for :meth:`pd.filter`. This option is useful
    when a table is wide, and specifying all ``required_valid_cols`` would be tedious.
    """

    like: str | None = None
    """A string to use as the ``like`` argument to :meth:`pd.filter`"""

    regex: str | None = None
    """A regular expression to use as the ``regex`` argument to :meth:`pd.filter`."""

    @root_validator
    def one_filter_argument(cls, values):
        """Validate that only one argument is specified for :meth:`pd.filter`."""
        num_args = sum(
            int(bool(val))
            for val in [
                values["required_valid_cols"],
                values["allowed_invalid_cols"],
                values["like"],
                values["regex"],
            ]
        )
        if num_args > 1:
            raise AssertionError(
                "You must specify at most one argument to :meth:`pd.filter` and "
                f"{num_args} were found."
            )

        return values


def drop_invalid_rows(df: pd.DataFrame, params: InvalidRows) -> pd.DataFrame:
    """Drop rows with only invalid values in all specificed columns.

    This method finds all rows in a dataframe that contain ONLY invalid data in ALL of
    the columns that we are checking, and drops those rows, logging the % of all rows
    that were dropped.
    """
    # The default InvalidRows() instance has no effect:
    if (
        params.required_valid_cols is None
        and params.allowed_invalid_cols is None
        and params.like is None
        and params.regex is None
    ) or params.invalid_values is None:
        return df

    pre_drop_len = len(df)
    if params.required_valid_cols or params.allowed_invalid_cols:
        # check if the columns enumerated are actually in the df
        possible_cols = (
            params.required_valid_cols or [] + params.allowed_invalid_cols or []
        )
        missing_cols = [col for col in possible_cols if col not in df]
        if missing_cols and params.allowed_invalid_cols:
            logger.warning(
                "Columns used as drop_invalid_rows parameters do not appear in "
                f"dataframe: {missing_cols}"
            )
        if missing_cols and params.required_valid_cols:
            raise ValueError(
                "Some required valid columns in drop_invalid_rows are missing from "
                f"dataframe: {missing_cols}"
            )
        # set filter items using either required_valid_cols or allowed_invalid_cols
        items = params.required_valid_cols or [
            col for col in df if col not in params.allowed_invalid_cols
        ]

    # Filter to select the subset of COLUMNS we want to check for valid values:
    cols_to_check = df.filter(
        items=items, like=params.like, regex=params.regex, axis="columns"
    )
    # Create a boolean mask selecting the ROWS where NOT ALL of the columns we
    # care about are invalid (i.e. where ANY of the columns we care about contain a
    # valid value):
    mask = ~cols_to_check.isin(params.invalid_values).all(axis="columns")
    # Mask the input dataframe and make a copy to avoid returning a slice.
    df_out = df[mask].copy()
    logger.info(
        f"{1 - (len(df_out)/pre_drop_len):.1%} of records ({pre_drop_len-len(df_out)} "
        f"rows) contain only {params.invalid_values} values in required columns. "
        "Dropped these 💩💩💩 records."
    )
    return df_out


################################################################################
# Replace values with NA
################################################################################
class ReplaceWithNa(TransformParams):
    """Pameters that replace certain values with NA.

    The categorize strings function replaces bad values, but it requires all the values
    in the column to fall under a certain category. This function allows you to replace
    certain specific values with NA without having to categorize the rest of the column.
    """

    replace_with_na: list[str]
    """A list of values that should be replaced with NA."""


def replace_with_na(col: pd.Series, params: ReplaceWithNa) -> pd.Series:
    """Replace specified values with NA."""
    return col.replace(params.replace_with_na, pd.NA)


replace_with_na_multicol = multicol_transform_factory(replace_with_na)
"""A multi-column version of the :func:`nullify_outliers` function."""

################################################################################
# Spot fix missing values
################################################################################


class SpotFixes(TransformParams):
    """Parameters that replace certain values with a manually corrected value."""

    idx_cols: list[str]
    """The column(s) used to identify a record."""
    fix_cols: list[str]
    """The column(s) to be fixed."""
    expect_unique: bool
    """Set to True if each fix should correspond to only one row."""
    spot_fixes: list[tuple[str | int | float | bool, ...]]
    """A tuple containing the values of the idx_cols and fix_cols for each fix."""


def spot_fix_values(df: pd.DataFrame, params: SpotFixes) -> pd.DataFrame:
    """Manually fix one-off singular missing values and typos across a DataFrame.

    Use this function to correct typos, missing values that are easily manually
    identified through manual investigation of records, consistent issues for a small
    number of records (e.g. incorrectly entered capacity data for 2-3 plants).

    From an instance of :class:`SpotFixes`, this function takes a list of sets of
    manual fixes and applies them to the specified records in a given dataframe. Each
    set of fixes contains a list of identifying columns, a list of columns to be fixed,
    and the values to be updated. A ValueError will be returned if spot-fixed datatypes
    do not match those of the inputted dataframe. For each set of fixes, the
    expect_unique parameter allows users to specify whether each fix should be applied
    only to one row.

    Returns:
        The same input DataFrame but with some spot fixes corrected.
    """
    spot_fixes_df = pd.DataFrame(
        params.spot_fixes, columns=params.idx_cols + params.fix_cols
    )

    # Convert input datatypes to match corresponding df columns.
    for x in spot_fixes_df.columns:
        spot_fixes_df[x] = spot_fixes_df[x].astype(df[x].dtypes.name)

    spot_fixes_df = spot_fixes_df.set_index(params.idx_cols)
    df = df.set_index(params.idx_cols)

    if params.expect_unique is True and not df.index.is_unique:
        cols_list = ", ".join(params.idx_cols)
        raise ValueError(
            f"This spot fix expects a unique set of idx_col, but the idx_cols provided are not uniquely identifying: {cols_list}."
        )

    # Only keep spot fix values found in the dataframe index.
    spot_fixes_df = spot_fixes_df[spot_fixes_df.index.isin(df.index)]

    if not spot_fixes_df.empty:
        df.loc[spot_fixes_df.index, params.fix_cols] = spot_fixes_df

    df = df.reset_index()

    return df


################################################################################
# A parameter model collecting all the valid generic transform params:
################################################################################
class TableTransformParams(TransformParams):
    """A collection of all the generic transformation parameters for a table.

    This class is used to instantiate and contain all of the individual
    :class:`TransformParams` objects that are associated with transforming a given
    table. It can be instantiated using one of the table-level dictionaries of
    parameters defined in the dataset-specific modules in :mod:`pudl.transform.params`

    Data source-specific :class:`TableTransformParams` classes should be defined in
    the data source-specific transform modules and inherit from this class. See e.g.
    :class:`pudl.transform.ferc1.Ferc1TableTransformParams`
    """

    # MultiColumnTransformParams can be initilized to empty dictionaries, since they
    # all iterate over the columns they apply to. Empty means... do nothing.
    convert_units: dict[str, UnitConversion] = {}
    categorize_strings: dict[str, StringCategories] = {}
    nullify_outliers: dict[str, ValidRange] = {}
    normalize_strings: dict[str, StringNormalization] = {}
    strip_non_numeric_values: dict[str, StripNonNumericValues] = {}
    replace_with_na: dict[str, ReplaceWithNa] = {}

    # Transformations that apply to whole dataframes have to be treated individually,
    # with default initializations that result in no transformation taking place.

    # correct_units iterates over a list... so does nothing with an empty list.
    correct_units: list[UnitCorrections] = []
    # This instantiates an empty dictionary of column renames:
    rename_columns: RenameColumns = RenameColumns()
    # InvalidRows has a special case of all None parameters, where it does nothing:
    drop_invalid_rows: list[InvalidRows] = []
    # This instantiates an empty list. The function iterates over a list,
    # so does nothing.
    spot_fix_values: list[SpotFixes] = []

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> "TableTransformParams":
        """Construct ``TableTransformParams`` from a dictionary of keyword arguments.

        Typically these will be the table-level dictionaries defined in the dataset-
        specific modules in the :mod:`pudl.transform.params` subpackage. See also the
        :meth:`TableTransformParams.from_id` method.
        """
        return cls(**params)

    @classmethod
    def from_id(cls, table_id: enum.Enum) -> "TableTransformParams":
        """A factory method that looks up transform parameters based on table_id.

        This is a shortcut, which allows us to constitute the parameter models based on
        the table they are associated with without having to pass in a potentially large
        nested data structure, which gets messy in Dagster.
        """
        transform_params = {
            **pudl.transform.params.ferc1.TRANSFORM_PARAMS,
            # **pudl.transform.params.eia860.TRANSFORM_PARAMS,
            # **pudl.transform.params.eia923.TRANSFORM_PARAMS,
            # etc... as appropriate
        }
        return cls.from_dict(transform_params[table_id.value])


#####################################################################################
# Abstract Table Transformer classes
#####################################################################################
def cache_df(key: str = "main") -> Callable[..., pd.DataFrame]:
    """A decorator for caching dataframes within an :class:`AbstractTableTransformer`.

    It's often useful during development or debugging to be able to track the evolution
    of data as it passes through several transformation steps. Especially when some of
    the steps are time consuming, it's nice to still get a copy of the last known state
    of the data when a transform raises an exception and fails.

    This decorator lets you easily save a copy of the dataframe being returned by a
    class method for later reference, before moving on to the next step. Each unique key
    used within a given :class:`AbstractTableTransformer` instance results in a new
    dataframe being cached. Re-using the same key will overwrite previously cached
    dataframes that were stored with that key.

    Saving many intermediate steps can provide lots of detailed information, but will
    use more memory. Updating the same cached dataframe as it successfully passes
    through each step lets you access the last known state it had before an error
    occurred.

    This decorator requires that the decorated function return a single
    :class:`pd.DataFrame`, but it can take any type of inputs.

    There's a lot of nested functions in here. For a more thorough explanation, see:
    https://realpython.com/primer-on-python-decorators/#fancy-decorators

    Args:
        key: The key that will be used to store and look up the cached dataframe in the
            internal ``self._cached_dfs`` dictionary.

    Returns:
        The decorated class method.
    """

    def _decorator(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
        @wraps(func)
        def _wrapper(self: AbstractTableTransformer, *args, **kwargs) -> pd.DataFrame:
            df = func(self, *args, **kwargs)
            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    f"{self.table_id.value}: The cache_df decorator only works on "
                    "methods that return a pandas dataframe. "
                    f"The method {func.__name__} returned a {type(df)}."
                )
            if self.cache_dfs:
                logger.debug(
                    f"{self.table_id.value}: Caching df to {key=} "
                    f"in {func.__name__}()"
                )
                self._cached_dfs[key] = df.copy()
            return df

        return _wrapper

    return _decorator


class AbstractTableTransformer(ABC):
    """An abstract base table transformer class.

    This class provides methods for applying the general purpose transform funcitons to
    dataframes. These methods should each log that they are running, and the
    ``table_id`` of the table they're beiing applied to. By default they should obtain
    their parameters from the ``params`` which are stored in the class, but should allow
    other parameters to be passed in.

    The class also provides a template for coordinating the high level flow of data
    through the transformations. The main coordinating function that's used to run the
    full transformation is :meth:`AbstractTableTransformer.transform`, and the transform
    is broken down into 3 distinct steps: start, main, and end. Those individual steps
    need to be defined by child classes. Usually the start and end methods will handle
    transformations that need to be applied uniformily across all the tables in a given
    dataset, with the main step containing transformations that are specific to a
    particular table.

    In development it's often useful to be able to review the state of the data at
    various stages as it progresses through the transformation. The :func:`cache_df`
    decorator defined above can be applied to individual transform methods or the
    start, main, and end methods defined in the child classes, to allow intermediate
    dataframes to be reviewed after the fact. Whether to cache dataframes and whether to
    delete them upon successful completion of the transform is controlled by flags set
    when the ``TableTransformer`` class is created.

    Table-specific transform parameters need to be associated with the class. They can
    either be passed in explicitly when the class is instantiated, or looked up based on
    the ``table_id`` associated with the class. See :meth:`TableTransformParams.from_id`

    The call signature of the :meth:`AbstractTableTransformer.transform_start` method
    accepts any type of inputs by default, and returns a single :class:`pd.DataFrame`.
    Later transform steps are assumed to take a single dataframe as input, and return a
    single dataframe. Since Python is lazy about enforcing types and interfaces you can
    get away with other kinds of arguments when they're sometimes necessary, but this
    isn't a good arrangement and we should figure out how to do it right. See the
    :class:`pudl.transform.ferc1.PlantsSteamFerc1TableTransformer` class for an example.
    """

    table_id: enum.Enum
    """Name of the PUDL database table that this table transformer produces.

    Must be defined in the database schema / metadata. This ID is used to instantiate
    the appropriate :class:`TableTransformParams` object.
    """

    cache_dfs: bool = False
    """Whether to cache copies of intermediate dataframes until transformation is done.

    When True, the TableTransformer will save dataframes internally at each step of the
    transform, so that they can be inspected easily if the transformation fails.
    """

    clear_cached_dfs: bool = True
    """Determines whether cached dataframes are deleted at the end of the transform."""

    _cached_dfs: dict[str, pd.DataFrame] = {}
    """Cached intermediate dataframes for use in development and debugging.

    The dictionary keys are the strings passed to the :func:`cache_df` method decorator.
    """

    parameter_model = TableTransformParams
    """The :mod:`pydantic` model that is used to contain & instantiate parameters.

    In child classes this should be replaced with the data source-specific
    :class:`TableTransformParams` class, if it has been defined.
    """

    params: parameter_model
    """The parameters that will be used to control the transformation functions.

    This attribute is of type ``parameter_model`` which is defined above. This type
    varies across datasets and is used to construct and validate the parameters based,
    so it needs to be set separately in child classes. See
    :class:`pudl.transform.ferc1.Ferc1AbstractTableTransformer` for an example.
    """

    def __init__(
        self,
        params: TableTransformParams | None = None,
        cache_dfs: bool = False,
        clear_cached_dfs: bool = True,
        **kwargs,
    ) -> None:
        """Initialize the table transformer, setting caching flags."""
        super().__init__()
        # Allowing params to be passed in or looked up makes testing easier, since it
        # means the same transformer can be run with a variety of parameters and inputs.
        if params is None:
            self.params = self.parameter_model.from_id(self.table_id)
        else:
            self.params = params
        self.cache_dfs = cache_dfs
        self.clear_cached_dfs = clear_cached_dfs

    ################################################################################
    # Abstract methods that must be defined by subclasses
    @abstractmethod
    def transform_start(self, *args, **kwargs) -> pd.DataFrame:
        """Transformations applied to many tables within a dataset at the beginning.

        This method should be implemented by the dataset-level abstract table
        transformer class. It does not specify its inputs because different data sources
        need different inputs. E.g. the FERC 1 transform needs 2 XBRL derived
        dataframes, and one DBF derived dataframe, while (most) EIA tables just receive
        and return a single dataframe.

        This step is often used to organize initial transformations that are applied
        uniformly across all the tables in a dataset.

        At the end of this step, all the inputs should have been consolidated into a
        single dataframe to return.
        """
        ...

    @abstractmethod
    def transform_main(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """The method used to do most of the table-specific transformations.

        Typically the transformations grouped together into this method will be unique
        to the table that is being transformed. Generally this method will take and
        return a single dataframe, and that pattern is implemented in the
        :meth:`AbstractTableTransformer.transform` method. In cases where transforms
        take or return more than one dataframe, you will need to define a new transform
        method within the child class. See :class:`PlantsSteamFerc1TableTransformer`
        as an example.
        """
        ...

    @abstractmethod
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformations applied to many tables within a dataset at the end.

        This method should be implemented by the dataset-level abstract table
        transformer class. It should do any standard cleanup that's required after the
        table-specific transformations have been applied. E.g. enforcing the table's
        database schema and dropping invalid records based on parameterized criteria.
        """
        ...

    ################################################################################
    # Default method implementations which can be used or overridden by subclasses
    def transform(self, *args, **kwargs) -> pd.DataFrame:
        """Apply all specified transformations to the appropriate input dataframes."""
        df = (
            self.transform_start(*args, **kwargs)
            .pipe(self.transform_main)
            .pipe(self.transform_end)
        )
        if self.clear_cached_dfs:
            logger.debug(
                f"{self.table_id.value}: Clearing cached dfs: "
                f"{sorted(self._cached_dfs.keys())}"
            )
            self._cached_dfs.clear()
        return df

    def rename_columns(
        self, df: pd.DataFrame, params: RenameColumns | None = None, **kwargs
    ) -> pd.DataFrame:
        """Rename the whole collection of dataframe columns using input params.

        Log if there's any mismatch between the columns in the dataframe, and the
        columns that have been defined in the mapping for renaming.
        """
        if params is None:
            params = self.params.rename_columns
        logger.info(
            f"{self.table_id.value}: Attempting to rename {len(params.columns)} "
            "columns."
        )

        # If we are attempting to rename columns that do *not* appear in the dataframe,
        # warn the user.
        missing_cols = set(params.columns).difference(set(df.columns))
        if missing_cols:
            logger.warning(
                f"{self.table_id.value}: Attempting to rename columns which are not "
                "present in the dataframe.\n"
                f"Missing columns: {missing_cols}"
            )
        return df.rename(columns=params.columns)

    def normalize_strings(
        self,
        df: pd.DataFrame,
        params: dict[str, bool] | None = None,
    ) -> pd.DataFrame:
        """Method wrapper for string normalization."""
        if params is None:
            params = self.params.normalize_strings
        logger.info(f"{self.table_id.value}: Normalizing freeform string columns.")
        return normalize_strings_multicol(df, params)

    def strip_non_numeric_values(
        self,
        df: pd.DataFrame,
        params: dict[str, bool] | None = None,
    ) -> pd.DataFrame:
        """Method wrapper for stripping non-numeric values."""
        if params is None:
            params = self.params.strip_non_numeric_values
        logger.info(
            f"{self.table_id.value}: Stripping non-numeric values from {list(params.keys())}."
        )
        return strip_non_numeric_values_multicol(df, params)

    def categorize_strings(
        self,
        df: pd.DataFrame,
        params: dict[str, StringCategories] | None = None,
    ) -> pd.DataFrame:
        """Method wrapper for string categorization."""
        if params is None:
            params = self.params.categorize_strings
        logger.info(
            f"{self.table_id.value}: Categorizing string columns using a controlled "
            "vocabulary."
        )
        return categorize_strings_multicol(df, params)

    def nullify_outliers(
        self,
        df: pd.DataFrame,
        params: dict[str, ValidRange] | None = None,
    ) -> pd.DataFrame:
        """Method wrapper for nullifying outlying values."""
        if params is None:
            params = self.params.nullify_outliers
        logger.info(f"{self.table_id.value}: Nullifying outlying values.")
        return nullify_outliers_multicol(df, params)

    def convert_units(
        self,
        df: pd.DataFrame,
        params: dict[str, UnitConversion] | None = None,
    ) -> pd.DataFrame:
        """Method wrapper for columnwise unit conversions."""
        if params is None:
            params = self.params.convert_units
        logger.info(
            f"{self.table_id.value}: Converting units and renaming columns accordingly."
        )
        return convert_units_multicol(df, params)

    def correct_units(
        self,
        df: pd.DataFrame,
        params: UnitCorrections | None = None,
    ) -> pd.DataFrame:
        """Apply all specified unit corrections to the table in order.

        Note: this is a table transform, not a multi-column transform.
        """
        if params is None:
            params = self.params.correct_units
        logger.info(
            f"{self.table_id.value}: Correcting inferred non-standard column units."
        )
        for correction in params:
            df = correct_units(df, correction)
        return df

    def drop_invalid_rows(
        self, df: pd.DataFrame, params: list[InvalidRows] | None = None
    ) -> pd.DataFrame:
        """Drop rows with only invalid values in all specificed columns."""
        if params is None:
            params = self.params.drop_invalid_rows
        logger.info(f"{self.table_id.value}: Dropping remaining invalid rows.")
        for param in params:
            df = drop_invalid_rows(df, param)
        return df

    def replace_with_na(
        self,
        df: pd.DataFrame,
        params: dict[str, ReplaceWithNa] | None = None,
    ) -> pd.DataFrame:
        """Replace specified values with NA."""
        if params is None:
            params = self.params.replace_with_na
        logger.info(f"{self.table_id.value}: Replacing specified values with NA.")
        return replace_with_na_multicol(df, params)

    def spot_fix_values(
        self,
        df: pd.DataFrame,
        params: list[SpotFixes] | None = None,
    ) -> pd.DataFrame:
        """Replace specified values with specified values."""
        if params is None:
            params = self.params.spot_fix_values
        logger.info(f"{self.table_id.value}: Spot fixing missing values.")
        for param in params:
            df = spot_fix_values(df, param)
        return df

    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns not in the DB schema and enforce specified types."""
        logger.info(f"{self.table_id.value}: Enforcing database schema on dataframe.")
        resource = Package.from_resource_ids().get_resource(self.table_id.value)
        df = resource.enforce_schema(df)
        return df
