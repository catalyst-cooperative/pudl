"""Generic class definitions useful for transforming multiple data sources.

There are 3 kinds of classes defined in this module:

* Transform Parameters (Pydantic)
* Transform Functions (Protocols)
* Table Transformers (Abstract Base Class)

"""
import enum
import re
import typing
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import cached_property, wraps
from itertools import combinations
from typing import Protocol

import numpy as np
import pandas as pd
from pydantic import BaseModel, root_validator, validator

import pudl.transform.params.ferc1
from pudl.helpers import get_logger
from pudl.metadata.classes import Package

logger = get_logger(__name__)


#####################################################################################
# Transform Parameter Models
#####################################################################################
class TransformParams(BaseModel):
    """An immutable base model for transformation parameters."""

    class Config:
        """Prevent parameters from changing part way through."""

        allow_mutation = False


class MultiColumnTransformParams(TransformParams):
    """Transform params that apply to several columns in a table.

    The keys are column names, and the values must all be the same type of
    :class:`TransformParams` object, since MultiColumnTransformParams are used by
    :class:`MultiColumnTransformFn` callables.

    Individual subclasses are dynamically generated for each multi-column transformation
    specified within a :class:`TableTransformParams` object.

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


class RenameColumns(TransformParams):
    """A dictionary for mapping old column names to new column names in a dataframe."""

    columns: dict[str, str] = {}


class InvalidRows(TransformParams):
    """Pameters that identify invalid rows to drop."""

    invalid_values: list
    """A list of values that should be considered invalid in the selected columns."""

    required_valid_cols: list[str] | None = None
    """The list of columns passed into :meth:`pd.filter` as the ``items`` argument."""

    allowed_invalid_cols: list[str] | None = None
    """The list of *not* to search for valid values to preserve.

    Used to construct an ``items`` argument for :meth:`pd.filter`. This is useful if a
    table is wide and specifcying all ``required_valid_cols`` would be tedious.
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
        if num_args != 1:
            raise AssertionError(
                "You must specify one and only one argument to :meth:`pd.filter` and "
                f"{num_args} were found."
            )
        return values


class StringCategories(TransformParams):
    """Defines mappings to clean up manually categorized freeform strings.

    Each key in the dictionary is the categorical value that all the freeform strings
    listed in the associated value will be mapped to after categorization.

    """

    categories: dict[str, set[str]]
    na_category: str = "na_category"

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


class UnitConversion(TransformParams):
    """A column-wise unit conversion.

    The default values will result in no alteration of the column.
    """

    multiplier: float = 1.0  # By default, multiply by 1 (no change)
    adder: float = 0.0  # By default, add 0 (no change)
    pattern: typing.Pattern = r"^(.*)$"  # By default, match the whole column namme
    repl: str = r"\1"  # By default, replace the whole column name with itself.


class ValidRange(TransformParams):
    """Column level specification of min and/or max values."""

    lower_bound: float = -np.inf
    upper_bound: float = np.inf

    @validator("upper_bound")
    def upper_bound_gte_lower_bound(cls, v, values, **kwargs):
        """Require upper bound to be greater than or equal to lower bound."""
        if values["lower_bound"] > v:
            raise ValueError("upper_bound must be greater than or equal to lower_bound")
        return v


class UnitCorrections(TransformParams):
    """Fix outlying values resulting from unit errors by muliplying by a constant.

    Note that since the unit correction depends on other columns in the dataframe to
    select a relevant subset of records, it is a table transform not a column transform,
    and so needs to know what column it applies to internally.

    """

    col: str
    query: str
    valid_range: ValidRange
    unit_conversions: list[UnitConversion]

    @validator("unit_conversions")
    def no_column_rename(cls, v: list[UnitConversion]):
        """Require that all unit conversions result in no column renaming.

        This constraint is imposed so that the same unit conversion definitions
        can be re-used both for unit corrections and columnwise unit conversions.
        """
        new_conversions = []
        for conv in v:
            new_conversions.append(
                UnitConversion(multiplier=conv.multiplier, adder=conv.adder)
            )
        return new_conversions


class TableTransformParams(TransformParams):
    """All the generic transformation parameters for a table.

    Data source specific TableTransformParams can be defined by models that inherit
    from this one in the data source specific transform modules.

    """

    rename_columns: RenameColumns = {}
    convert_units: dict[str, UnitConversion] = {}
    categorize_strings: dict[str, StringCategories] = {}
    nullify_outliers: dict[str, ValidRange] = {}
    normalize_strings: dict[str, bool] = {}
    correct_units: list[UnitCorrections] = []
    drop_invalid_rows: InvalidRows = {}

    @classmethod
    def from_id(cls, table_id: enum.Enum) -> "TableTransformParams":
        """A factory method that looks up tr ansform parameters based on table_id."""
        transform_params = {
            **pudl.transform.params.ferc1.TRANSFORM_PARAMS,
            # **pudl.transform.params.eia860.TRANSFORM_PARAMS,
            # **pudl.transform.params.eia923.TRANSFORM_PARAMS,
            # etc... as appropriate
        }
        return cls(**transform_params[table_id.value])


#####################################################################################
# Transform Protocol & General Function Definitions
#####################################################################################
class ColumnTransformFn(Protocol):
    """Callback protocol defining a per-column transformation function."""

    def __call__(self, col: pd.Series, params: TransformParams) -> pd.Series:
        """Create a callable."""
        ...


class TableTransformFn(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(self, df: pd.DataFrame, params: TransformParams) -> pd.DataFrame:
        """Create a callable."""
        ...


class MultiColumnTransformFn(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(
        self, df: pd.DataFrame, params: MultiColumnTransformParams
    ) -> pd.DataFrame:
        """Create a callable."""
        ...


def multicol_transform_fn_factory(
    col_fn: ColumnTransformFn,
    drop=True,
) -> MultiColumnTransformFn:
    """A factory for creating a multi-column transform function."""

    class InnerMultiColumnTransformFn(
        Callable[[pd.DataFrame, MultiColumnTransformParams], pd.DataFrame]
    ):
        __name__ = col_fn.__name__ + "_multicol"

        def __call__(
            self, df: pd.DataFrame, params: MultiColumnTransformParams
        ) -> pd.DataFrame:
            drop_col: bool = drop
            for col_name in params:
                if col_name in df.columns:
                    logger.debug(f"Applying {col_fn.__name__} to {col_name}")
                    new_col = col_fn(col=df[col_name], params=params[col_name])
                    if drop_col:
                        df = df.drop(columns=col_name)
                    df = pd.concat([df, new_col], axis="columns")
                else:
                    logger.warning(
                        f"Expected column {col_name} not found in dataframe during "
                        f"application of {col_fn.__name__}."
                    )
            return df

    return InnerMultiColumnTransformFn()


def normalize_strings(col: pd.Series, params: bool) -> pd.Series:
    """Derive a canonical version of the strings in the column.

    Transformations include:

    * Conversion to Pandas nullable String data type.
    * Removal of some non-printable characters.
    * Unicode composite character decomposition.
    * Translation to lower case.
    * Stripping of leading and trailing whitespace.
    * Compression of multiple consecutive whitespace characters to a single space.

    """
    return (
        col.astype(pd.StringDtype())
        .str.replace(r"[\x00-\x1f\x7f-\x9f]", "", regex=True)
        .str.normalize("NFKD")
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


normalize_strings_multicol = multicol_transform_fn_factory(normalize_strings)


def categorize_strings(col: pd.Series, params: StringCategories) -> pd.Series:
    """Impose a controlled vocabulary on freeform string column."""
    uncategorized_strings = set(col).difference(params.mapping)
    if uncategorized_strings:
        logger.warning(
            f"{col.name}: Found {len(uncategorized_strings)} uncategorized values: "
            f"{uncategorized_strings}"
        )
    col = col.map(params.mapping).astype(pd.StringDtype())
    col.loc[col == params.na_category] = pd.NA
    return col


categorize_strings_multicol = multicol_transform_fn_factory(categorize_strings)


def nullify_outliers(col: pd.Series, params: ValidRange) -> pd.Series:
    """Set any values outside the valid range to NA."""
    col = pd.to_numeric(col, errors="coerce")
    col[~col.between(params.lower_bound, params.upper_bound)] = np.nan
    return col


nullify_outliers_multicol = multicol_transform_fn_factory(nullify_outliers)


def convert_units(col: pd.Series, params: UnitConversion) -> pd.Series:
    """Convert the units of and appropriately rename a column."""
    new_name = re.sub(pattern=params.pattern, repl=params.repl, string=col.name)
    # only apply the unit conversion if the column name matched the pattern
    if not re.match(pattern=params.pattern, string=col.name):
        logger.warning(
            f"{col.name} did not match the unit rename pattern. Check for typos "
            "and make sure you're applying the conversion to an appropriate column."
        )
    if col.name == new_name:
        logger.debug(f"Old and new column names are identical: {col.name}.")
    col = (params.multiplier * col) + params.adder
    col.name = new_name
    return col


convert_units_multicol = multicol_transform_fn_factory(convert_units)


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

    for the values, and a list of factors by which we expect to see some of the data
    multiplied due to unit errors.  Data found in these "ghost" distributions are
    multiplied by the appropriate factor to bring them into the expected range.

    Data values which are not found in one of the acceptable multiplicative ranges are
    set to NA.

    """
    logger.info(f"Correcting units of {params.col} where {params.query}.")
    # Select a subset of the input dataframe to work on. E.g. only the heat content
    # column for coal records:
    selected = df.loc[df.query(params.query).index, params.col]
    not_selected = df[params.col].drop(index=selected.index)

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
    df[params.col] = pd.concat([selected, not_selected])
    return df


#####################################################################################
# Abstract Table Transformer classes
#####################################################################################
def cache_df(key: str = "main") -> Callable:
    """A decorator for managing intermediate dataframe caching."""

    def _decorator(func) -> Callable:
        @wraps(func)
        def _wrapper(self: AbstractTableTransformer, *args, **kwargs) -> pd.DataFrame:
            df = func(self, *args, **kwargs)
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

    Only methods that are generally useful across data sources should be defined here.
    Make sure to decorate any methods that must be defined by child classes with
    @abstractmethod.

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
    """Cached intermediate dataframes for use in development and debugging."""

    def __init__(self, cache_dfs: bool = False, clear_cached_dfs: bool = True) -> None:
        """Initialize the table transformer, setting caching flags."""
        super().__init__()
        self.cache_dfs = cache_dfs
        self.clear_cached_dfs = clear_cached_dfs

    @cached_property
    def params(self) -> TableTransformParams:
        """Obtain table transform parameters based on the table ID."""
        return TableTransformParams.from_id(table_id=self.table_id)

    ################################################################################
    # Abstract methods that must be defined by subclasses
    @abstractmethod
    def start_transform(self, **kwargs) -> pd.DataFrame:
        """Transformations applied to many tables within a dataset at the beginning.

        This method should be implemented by the dataset-level abstract table
        transformer class. It does not specify its inputs because different data sources
        need different inputs. E.g. the FERC 1 transform needs 2 XBRL derived
        dataframes, and one DBF derived dataframe, while (most) EIA tables just receive
        and return a single dataframe.

        At the end of this step, all the inputs should have been consolidated into a
        single dataframe to return.

        """
        ...

    @abstractmethod
    def main_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """The workhorse method doing most of the table-specific transformations."""
        ...

    @abstractmethod
    def finish_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformations applied to many tables within a dataset at the end.

        This method should be implemented by the dataset-level abstract table
        transformer class. It should do any standard cleanup that's required after the
        table-specific transformations have been applied. E.g. enforcing the table's
        database schema and dropping invalid records based on parameterized criteria.

        """

    ################################################################################
    # Default method implementations which can be used or overridden by subclasses
    def transform(self, *args, **kwargs) -> pd.DataFrame:
        """Apply all specified transformations to the appropriate input dataframes."""
        df = (
            self.start_transform(*args, **kwargs)
            .pipe(self.main_transform)
            .pipe(self.finish_transform)
        )
        if self.clear_cached_dfs:
            logger.debug(
                f"{self.table_id.value}: Clearing cached dfs: "
                f"{sorted(self._cached_dfs.keys())}"
            )
            self._cached_dfs.clear()
        return df

    def rename_columns(
        self,
        df: pd.DataFrame,
        params: RenameColumns,
    ) -> pd.DataFrame:
        """Rename the whole collection of dataframe columns using input params.

        Log if there's any mismatch between the columns in the dataframe, and the
        columns that have been defined in the mapping for renaming.

        """
        logger.info(f"{self.table_id.value}: Renaming {len(params.columns)} columns.")
        df_col_set = set(df.columns)
        param_col_set = set(params.columns)
        if df_col_set != param_col_set:
            unshared_values = df_col_set.symmetric_difference(param_col_set)
            logger.warning(
                f"{self.table_id.value}: Discrepancy between dataframe columns and "
                "rename dictionary keys. \n"
                f"Unshared values: {unshared_values}"
            )
        return df.rename(columns=params.columns)

    def drop_invalid_rows(self, df: pd.DataFrame, params: InvalidRows) -> pd.DataFrame:
        """Drop rows with only invalid values in all specificed columns.

        This method finds all rows in a dataframe that contain ONLY invalid data in ALL
        of the columns that we are checking, and drops those rows, logging the % of all
        rows that were dropped.

        """
        pre_drop_len = len(df)
        # set filter items using either required_valid_cols or allowed_invalid_cols
        if params.required_valid_cols or params.allowed_invalid_cols:
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
            f"{self.table_id.value}: {1 - (len(df_out)/pre_drop_len):.0%} of records"
            f" contain only {params.invalid_values} values in required columns. "
            "Dropping these 💩💩💩 records."
        )
        return df_out

    def normalize_strings_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, bool],
    ) -> pd.DataFrame:
        """Method wrapper for string normalization."""
        logger.info(f"{self.table_id.value}: Normalizing freeform string columns.")
        return normalize_strings_multicol(df, params)

    def categorize_strings_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, StringCategories],
    ) -> pd.DataFrame:
        """Method wrapper for string categorization."""
        logger.info(
            f"{self.table_id.value}: Categorizing string columns using a controlled "
            "vocabulary."
        )
        return categorize_strings_multicol(df, params)

    def nullify_outliers_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, ValidRange],
    ) -> pd.DataFrame:
        """Method wrapper for nullifying outlying values."""
        logger.info(f"{self.table_id.value}: Nullifying outlying values.")
        return nullify_outliers_multicol(df, params)

    def convert_units_multicol(
        self,
        df: pd.DataFrame,
        params: dict[str, UnitConversion],
    ) -> pd.DataFrame:
        """Method wrapper for columnwise unit conversions."""
        logger.info(
            f"{self.table_id.value}: Converting units and renaming columns accordingly."
        )
        return convert_units_multicol(df, params)

    def correct_units(
        self,
        df: pd.DataFrame,
        params: UnitCorrections,
    ) -> pd.DataFrame:
        """Apply all specified unit corrections to the table in order.

        Note: this is a table transform, not a multi-column transform.
        """
        logger.info(
            f"{self.table_id.value}: Correcting inferred non-standard column units."
        )
        for correction in params:
            df = correct_units(df, correction)
        return df

    def enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop columns not in the DB schema and enforce specified types."""
        logger.info(f"{self.table_id.value}: Enforcing database schema on dataframe.")
        resource = Package.from_resource_ids().get_resource(self.table_id.value)
        expected_cols = pd.Index(resource.get_field_names())
        missing_cols = list(expected_cols.difference(df.columns))
        if missing_cols:
            raise ValueError(
                f"{self.table_id.value}: Missing columns found when enforcing table "
                f"schema: {missing_cols}"
            )
        return resource.format_df(df)
