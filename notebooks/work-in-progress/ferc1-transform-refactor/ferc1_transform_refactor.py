"""A temporary module for exploring a refactor of our data transformations.

There are 4 main components to model of table transformations: transform functions,
transform parameters, the data to be transformed, and table transformer classes that are
used to organize dataset and table-specific collections of transformer functions
hierarchically.

1. transform_functions(): take data (either a Series or DataFrame) and a
   TransformParams object as inputs, and return transformed data of the same type that
   they consumed (Series or DataFrame). They operate on the data, and their particular
   behavior is controled by the TransformParams.

   * Some transform_functions() operate on individual columns. They take a Series and
     and a TransformParam and return a Series.

   * Some transform_functions() are intrinsically table-oriented since they depend on
     more than one column of input data. They take a DataFrame and a TransformParam and
     return a DataFrame.

   * Column-wise transforms can be automatically turned into table-wise transforms using
     a factory function.

   * transform_functions() of potentially general utility will be defined as globally
     accessible functions at the module level.

2. TransformParams: Immutable Pydantic models that store and validate the parameters
   required to perform a variety of different column or table level transformations.

   * Some TransformParams only apply to a column level transform. Others only apply to a
     table level transform.

   * Multiple column-level TransformParams of the same type can be turned into
     table-level TransformParams simply by creating a dictionary that maps the column
     names to their individual column-level TransformParams.

3. The data to be transformed is tabular, and will be passed around as dataframes. If
   possible, we'll only operate on whole dataframes, with individual column-level
   transformations being packaged inside of table-level transformations as described
   above.

4. TableTransformers are classes specific to cleaning a particular table (e.g.
   plants_steam_ferc1). These may inherit from a dataset-specific abstract base class
   that defines any transformation methods that are only relevant to that dataset, but
   which are generally applicable to many tables within the dataset (e.g.  merging the
   instant and duration tables from the FERC 1 XBRL data.). These inherited methods
   can be overridden when necessary (potentially still making use of the inherited
   method) if a particular table needs special treatment.

"""

import logging
import re
import sys
import typing
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)


################################################################################
# Test dataframes
################################################################################
TEST_INPUT_DF: pd.DataFrame = pd.DataFrame(
    {
        "netgen": [1_000, 2_000, 3_000],
        "fuel_btu_per_unit": [1.7e7, 5.8e6, 1e5],
        "units": ["tons", "barrels", "mcf"],
        "fuel_type": ["dinosaur", "astroglide", "unicorn farts"],
    }
)

EXPECTED_OUTPUT_DF: pd.DataFrame = pd.DataFrame(
    {
        "net_generation_mwh": [1, 2, 3],
        "fuel_mmbtu_per_unit": [17.0, 5.8, 0.1],
        "fuel_units": ["tons", "barrels", "mcf"],
        "fuel_type": ["coal", "oil", "gas"],
    }
)

################################################################################
# Constants that parameterize the transformations to be performed.
################################################################################
KWH_TO_MWH = dict(
    multiplier=1e-3,
    pattern=r"(.*)_kwh$",
    repl=r"\1_mwh",
)
"""Parameters for converting column units from kWh to MWh."""

BTU_TO_MMBTU = dict(
    multiplier=1e-6,
    pattern=r"(.*)_btu(.*)$",
    repl=r"\1_mmbtu\2",
)
"""Parameters for converting column units from BTU to MMBTU."""

FUEL_TYPES = {
    "coal": {"black", "xenoforming", "dinosaur", "goo"},
    "oil": {"olive", "sunflower", "fish", "astroglide"},
    "gas": {"elon musk", "unicorn farts", "cow farts"},
}

TRANSFORM_PARAMS = {
    "test_table_id": {
        "rename_columns": {
            "netgen": "net_generation_kwh",
            "heat_content": "fuel_btu_per_unit",
            "units": "fuel_units",
            "fuel_type": "fuel_type",
        },
        "unit_conversions": {
            "net_generation_kwh": KWH_TO_MWH,
            "fuel_btu_per_unit": BTU_TO_MMBTU,
        },
        "string_categories": {
            "fuel_type": FUEL_TYPES,
        },
    }
}


################################################################################
# Pydanic Transformation Parameter Models
################################################################################
class TransformParams(BaseModel):
    """An immutable base model for transformation parameters."""

    class Config:
        """Prevent parameters from changing part way through."""

        allow_mutation = False


class MultiColumnTransformParams(TransformParams):
    """Transform params that apply to several columns in a table."""

    __root__: dict[str, TransformParams]

    def __iter__(self):
        """Enable iteration over stored column transform parameters."""
        return iter(self.__root__)

    def __getitem__(self, item):
        """Enable dictionary style access."""
        return self.__root__[item]

    def __getattr__(self, item):
        """Enable dotted attribute access."""
        return self.__root__[item]


class RenameCols(TransformParams):
    """Simple table transform params for renaming columns."""

    __root__: dict[str, str] = {}

    def __iter__(self):
        """Enable iteration over column rename dictionary."""
        return iter(self.__root__)

    def __getitem__(self, item):
        """Enable dictionary style access."""
        return self.__root__[item]

    def __getattr__(self, item):
        """Enable dotted attribute access."""
        return self.__root__[item]


class StringCategories(TransformParams):
    """Defines mappings to clean up manually categorized freeform strings.

    Each key in a stringmap is a cleaned output category, and each value is the set of
    all strings which should be replaced with associated clean output category.

    """

    __root__: dict[str, set[str]]

    def __iter__(self):
        """Enable iteration over string categories."""
        return iter(self.__root__)

    def __getitem__(self, item):
        """Enable dictionary style access."""
        return self.__root__[item]

    def __getattr__(self, item):
        """Enable dotted attribute access."""
        return self.__root__[item]

    @property
    def mapping(self) -> dict[str, str]:
        """A 1-to-1 mapping appropriate for use with :meth:`pd.Series.map`."""
        return {string: cat for cat in self for string in self[cat]}


class UnitConversion(TransformParams):
    """A column-wise unit conversion."""

    multiplier: float = 1.0
    adder: float = 0.0
    pattern: typing.Pattern
    repl: str


class MultiColumnUnitConversion(MultiColumnTransformParams):
    """Transformation paramaters for converting the units of multiple columns."""

    __root__: dict[str, UnitConversion]


class MultiColumnStringCategories(MultiColumnTransformParams):
    """Transformation parameters for categorizing strings in multiple columns."""

    __root__: dict[str, StringCategories]


class TableTransformParams(TransformParams):
    """All defined transformation parameters for a table."""

    rename_columns: RenameCols = {}
    unit_conversions: MultiColumnUnitConversion = {}
    string_categories: MultiColumnStringCategories = {}

    @classmethod
    def from_id(cls, table_id: str) -> "TableTransformParams":
        """A factory method that looks up transform parameters based on table_id."""
        return cls(**DatasetTransformParams()[table_id])


class DatasetTransformParams(TransformParams):
    """All defined transformation parameters pertaining to a dataset, (e.g. ferc1).

    Dataset transform parameters are keyed by table name.
    """

    __root__: dict[str, TableTransformParams] = TRANSFORM_PARAMS

    def __iter__(self):
        """Enable iteration over stored table transform parameters."""
        return iter(self.__root__)

    def __getitem__(self, item):
        """Enable dictionary style access."""
        return self.__root__[item]

    def __getattr__(self, item):
        """Enable dotted attribute access."""
        return self.__root__[item]


################################################################################
# Column, MultiColumn, and Table Transform Functions
################################################################################
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


def multicol_transform_fn_factory(
    col_fn: ColumnTransformFn,
    drop=False,
) -> TableTransformFn:
    """A factory for creating a table transformation from a column transformation."""

    def tab_fn(df: pd.DataFrame, params: MultiColumnTransformParams) -> pd.DataFrame:
        drop_col: bool = drop
        for col_name in params:
            if col_name in df.columns:
                new_col = col_fn(col=df[col_name], params=params[col_name])
                df = pd.concat([df, new_col], axis="columns")
                if drop_col:
                    if col_name == new_col.name:
                        raise ValueError(
                            f"Attempting to drop the column {col_name} that we just "
                            "created!"
                        )
                    df = df.drop(columns=col_name)
            else:
                logger.info(
                    f"Expected column {col_name} not found in dataframe during table "
                    "transform."
                )
        return df

    return tab_fn


def convert_units_col(col: pd.Series, params: UnitConversion) -> pd.Series:
    """Convert the units of and appropriately rename a column."""
    new_name = re.sub(pattern=params.pattern, repl=params.repl, string=col.name)
    # only apply the unit conversion if the column name matched the pattern
    if new_name == col.name:
        logger.warning(
            f"{col.name} did not match the unit rename pattern. Check for typos "
            "and make sure you're applying the conversion to an appropriate column."
        )
    col = (params.multiplier * col) + params.adder
    col.name = new_name
    return col


convert_units = multicol_transform_fn_factory(convert_units_col, drop=True)


def categorize_strings_col(col: pd.Series, params: StringCategories) -> pd.Series:
    """Impose a controlled vocabulary on freeform string column."""
    return col.map(params.mapping)


categorize_strings = multicol_transform_fn_factory(categorize_strings_col)


################################################################################
# TableTransformer classes
################################################################################
class AbstractTableTransformer(ABC):
    """An exmaple abstract base transformer class."""

    table_id: str
    params: TableTransformParams

    def __init__(self, table_id: str):
        """Initialize the table transformer class."""
        self.table_id = table_id
        self.params = TableTransformParams.from_id(table_id=table_id)

    ################################################################################
    # Abstract methods that must be defined by subclasses
    @abstractmethod
    def apply(self, dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Apply all specified transformations to the appropriate input dataframes."""
        ...

    ################################################################################
    # Default method implementations which can be used or overridden by subclasses
    def rename_columns(self, df: pd.DataFrame, params: RenameCols) -> pd.DataFrame:
        """Rename the whole collection of dataframe columns using input params.

        Log if there's any mismatch between the columns in the dataframe, and the
        columns that have been defined in the mapping for renaming.

        """
        return df.rename(columns=params.__root__)


class TestTableTransformer(AbstractTableTransformer):
    """A concrete table transformer class for testing."""

    def apply(self, dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Apply all the Test Transforms."""
        out_df = (
            dfs[self.table_id].pipe(self.rename_columns, self.params.rename_columns)
            # .pipe(categorize_strings, self.params.string_categories)
            # .pipe(convert_units, self.params.unit_conversions)
            # .pipe(self.bespoke_transform)
        )
        return {self.table_id: out_df}

    def bespoke_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """A transformation that's specific to this table."""
        return df


################################################################################
# Coordinating function that runs the show.
################################################################################
def main() -> int:
    """Stand-in for the coordinationg transform() function so we can test."""
    tttfr = TestTableTransformer("test_table_id")
    actual = tttfr.apply({"test_table_id": TEST_INPUT_DF})["test_table_id"]

    pd.testing.assert_frame_equal(actual, EXPECTED_OUTPUT_DF)

    return 0


if __name__ == "__main__":
    sys.exit(main())

# TODO:
# [ ] Test DatasetTransformParams actually instantiates nested models
# [ ] Function factory that takes a table name and provides a function with access
#     to the right TableTransformParams and TableTransformer for use with Dagster.
#     transform_all() function
# [ ] Flesh out table iterations in main() to stand in for coordinating transform()
#     function
