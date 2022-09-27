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

   * Column transforms can be automatically turned into table (multi-column) transforms
     using a factory function.

   * transform_functions() of potentially general utility will be defined as globally
     accessible functions at the module level. I'm not sure whether this pattern can
     be applied directly to table-specific colum/multi-column transforms that are
     defined as methods inside of a table transformer class.

2. TransformParams: Immutable Pydantic models that store and validate the parameters
   required to perform a variety of different column or table level transformations.

   * Some TransformParams only apply to a column level transform. Others only apply to a
     table level transform.

   * Multiple column-level TransformParams of the same type can be turned into
     table-level TransformParams simply by creating a dictionary that maps the column
     names to their individual column-level TransformParams.

   * TableTransformParams contain all of the TransformParams that apply to a particular
     table.

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
   method) if a particular table needs special treatment. Each of the table-specific
   subclasses will know what table it is associated with, and can look up the
   TableTransformParams that are associated with it.
"""
import logging
import re
import sys
import typing
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Protocol

import pandas as pd
from pydantic import BaseModel, root_validator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

################################################################################
# Test dataframes
################################################################################
TEST_INPUT_DF: pd.DataFrame = pd.DataFrame(
    {
        "netgen": [1000.0, 2000.0, 3000.0],
        "fuel_btu_per_unit": [1.7e7, 5.8e6, 1e5],
        "units": ["tons", "barrels", "mcf"],
        "fuel_type": ["dinosaur", "astroglide", "unicorn farts"],
    }
)

EXPECTED_OUTPUT_DFS: dict[str, pd.DataFrame] = dict(
    test_table_one=pd.DataFrame(
        {
            "net_generation_mwh": [1.0, 2.0, 3.0],
            "fuel_mmbtu_per_unit": [17.0, 5.8, 0.1],
            "fuel_units": ["tons", "barrels", "mcf"],
            "fuel_type": ["💩", "oil", "gas"],
        }
    ),
    test_table_two=pd.DataFrame(
        {
            "net_generation_mwh": [1.0, 2.0, 3.0],
            "fuel_mmbtu_per_unit": [17.0, 5.8, 0.1],
            "fuel_units": ["tons", "barrels", "mcf"],
            "fuel_type": ["👿", "oil", "gas"],
        }
    ),
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
    "categories": {
        "coal": {"black", "xenoforming", "dinosaur", "goo"},
        "oil": {"olive", "sunflower", "fish", "astroglide"},
        "gas": {"elon musk", "unicorn farts", "cow farts"},
    }
}

TRANSFORM_PARAMS = {
    "test_table_one": {
        "rename_columns": {
            "columns": {
                "netgen": "net_generation_kwh",
                "heat_content": "fuel_btu_per_unit",
                "units": "fuel_units",
                "fuel_type": "fuel_type",
            }
        },
        "convert_units": {
            "net_generation_kwh": KWH_TO_MWH,
            "fuel_btu_per_unit": BTU_TO_MMBTU,
        },
        "categorize_strings": {
            "fuel_type": FUEL_TYPES,
        },
    },
    "test_table_two": {
        "rename_columns": {
            "columns": {
                "netgen": "net_generation_kwh",
                "heat_content": "fuel_btu_per_unit",
                "units": "fuel_units",
                "fuel_type": "fuel_type",
            }
        },
        "convert_units": {
            "net_generation_kwh": KWH_TO_MWH,
            "fuel_btu_per_unit": BTU_TO_MMBTU,
        },
        "categorize_strings": {
            "fuel_type": FUEL_TYPES,
        },
    },
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


class StringCategories(TransformParams):
    """Defines mappings to clean up manually categorized freeform strings.

    Each key in a stringmap is a cleaned output category, and each value is the set of
    all strings which should be replaced with associated clean output category.
    """

    categories: dict[str, set[str]]

    @property
    def mapping(self) -> dict[str, str]:
        """A 1-to-1 mapping appropriate for use with :meth:`pd.Series.map`."""
        return {
            string: cat for cat in self.categories for string in self.categories[cat]
        }


class UnitConversion(TransformParams):
    """A column-wise unit conversion."""

    multiplier: float = 1.0
    adder: float = 0.0
    pattern: typing.Pattern
    repl: str


class TableTransformParams(TransformParams):
    """All defined transformation parameters for a table."""

    class Config:
        """Only allow the known table transform params."""

        extra = "forbid"

    rename_columns: RenameColumns = {}
    convert_units: dict[str, UnitConversion] = {}
    categorize_strings: dict[str, StringCategories] = {}

    @classmethod
    def from_id(cls, table_id: str) -> "TableTransformParams":
        """A factory method that looks up transform parameters based on table_id."""
        return cls(**TRANSFORM_PARAMS[table_id])


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


class MultiColumnTransformFn(Protocol):
    """Callback protocol defining a per-table transformation function."""

    def __call__(
        self, df: pd.DataFrame, params: MultiColumnTransformParams
    ) -> pd.DataFrame:
        """Create a callable."""
        ...


def multicol_transform_fn_factory(
    col_fn: ColumnTransformFn,
    drop=False,
) -> MultiColumnTransformFn:
    """A factory for creating a table transformation from a column transformation."""

    def tab_fn(df: pd.DataFrame, params: MultiColumnTransformParams) -> pd.DataFrame:
        drop_col: bool = drop
        for col_name in params:
            if col_name in df.columns:
                new_col = col_fn(col=df[col_name], params=params[col_name])
                if drop_col:
                    df = df.drop(columns=col_name)
                df = pd.concat([df, new_col], axis="columns")
            else:
                logger.warning(
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


categorize_strings = multicol_transform_fn_factory(categorize_strings_col, drop=True)


################################################################################
# TableTransformer classes
################################################################################
class AbstractTableTransformer(ABC):
    """An exmaple abstract base transformer class."""

    table_id: str

    @property
    def params(self) -> TableTransformParams:
        """Obtain table transform parameters based on the table ID."""
        return TableTransformParams.from_id(table_id=self.table_id)

    ################################################################################
    # Abstract methods that must be defined by subclasses
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all specified transformations to the appropriate input dataframes."""
        ...

    ################################################################################
    # Default method implementations which can be used or overridden by subclasses
    def rename_columns(self, df: pd.DataFrame, params: RenameColumns) -> pd.DataFrame:
        """Rename the whole collection of dataframe columns using input params.

        Log if there's any mismatch between the columns in the dataframe, and the
        columns that have been defined in the mapping for renaming.
        """
        return df.rename(columns=params.columns)


class TestTableTransformerOne(AbstractTableTransformer):
    """A concrete table transformer class for testing."""

    table_id: str = "test_table_one"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all the Test Transforms."""
        return (
            df.pipe(self.rename_columns, self.params.rename_columns)
            .pipe(categorize_strings, self.params.categorize_strings)
            .pipe(convert_units, self.params.convert_units)
            .pipe(self.bespoke_transform)
        )

    def bespoke_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """A transformation that's specific to this table."""
        return df.replace(to_replace="coal", value="💩")


class TestTableTransformerTwo(AbstractTableTransformer):
    """A concrete table transformer class for testing."""

    table_id: str = "test_table_two"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all the Test Transforms."""
        return (
            df.pipe(self.rename_columns, self.params.rename_columns)
            .pipe(categorize_strings, self.params.categorize_strings)
            .pipe(convert_units, self.params.convert_units)
            .pipe(self.bespoke_transform)
        )

    def bespoke_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """A transformation that's specific to this table."""
        return df.replace(to_replace="coal", value="👿")


def table_transformer_fn_factory(
    table_id: str,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """A factory to create table transform functions we can wrap in ops..."""
    table_transformers = {
        "test_table_one": TestTableTransformerOne(),
        "test_table_two": TestTableTransformerTwo(),
    }

    def fn(df: pd.DataFrame) -> pd.DataFrame:
        return table_transformers[table_id].transform(df)

    return fn


################################################################################
# Coordinating function that runs the show.
################################################################################
def main() -> int:
    """Stand-in for the coordinationg transform() function so we can test."""
    raw_dfs = {
        "test_table_one": TEST_INPUT_DF,
        "test_table_two": TEST_INPUT_DF,
    }
    transformed_dfs = {}
    for table_id in raw_dfs:
        df = raw_dfs[table_id]
        transform = table_transformer_fn_factory(table_id)
        transformed_dfs[table_id] = transform(df)

        pd.testing.assert_frame_equal(
            transformed_dfs[table_id].sort_index(axis="columns"),
            EXPECTED_OUTPUT_DFS[table_id].sort_index(axis="columns"),
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
