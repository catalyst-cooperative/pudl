"""Unit tests for TableTransformer and associated software tooling.

* Test the individual transform functions on static series / dataframes
* Test a dummy TableTransformer class that stitches together all of the
  general transform functions together with the class structure.
* Test that the TransformParams models succeed / fail / behave as expected.

"""

import enum
import random
from contextlib import nullcontext as does_not_raise
from string import ascii_letters

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from pydantic import ValidationError

from pudl.transform.classes import (
    AbstractTableTransformer,
    StringCategories,
    UnitConversion,
    UnitCorrections,
    cache_df,
    categorize_strings,
    convert_units,
    normalize_strings,
)
from pudl.transform.params.ferc1 import BTU_TO_MMBTU, KWH_TO_MWH, VALID_PLANT_YEARS

ANIMAL_CATS: dict[str, set[str]] = {
    "categories": {
        "cat": {
            "cat",
            "gato",
            "neko",
            "lion",
            "ta66y",
            "puma",
            "sphinx",
        },
        "dog": {
            "dog",
            "wolf",
            "dire wolf",
            "coyote",
            "dingo",
        },
        "na_category": {
            "na_category",
            "",
            "hyena",
            "scorpion",
            "cuttlefish",
            "bryozoan",
            "brittle star",
            "nudibranct",
            "bearded dragon",
        },
    },
}

STRING_PARAMS = {
    "test_table": {
        # "rename_columns": RENAME_CATS,
        "normalize_strings": {
            "col2": True,
        },
        "categorize_strings": {
            "col2": ANIMAL_CATS,
        },
        "nullify_outliers": {"col3": VALID_PLANT_YEARS},
        "correct_units": {},
        "drop_invalid_rows": {},
    }
}

STRING_DATA: pd.DataFrame = pd.DataFrame(
    columns=["raw", "norm", "cat"],
    data=[
        ("gato", "gato", "cat"),
        ("NEKO", "neko", "cat"),
        ("cAt", "cat", "cat"),
        (" lion ", "lion", "cat"),
        ("\tlion\t", "lion", "cat"),
        ("pUmA", "puma", "cat"),
        ("wolf", "wolf", "dog"),
        ("dire\t      \twolf  ", "dire wolf", "dog"),
        ("hyena", "hyena", pd.NA),
        ("ta66y", "ta66y", "cat"),
        ("", "", pd.NA),
        (pd.NA, "", pd.NA),
        (" ", "", pd.NA),
        (" \t ", "", pd.NA),
        ("\t\t", "", pd.NA),
    ],
).astype(
    {
        "norm": str,
        "cat": pd.StringDtype(),
    }
)


#####################################################################################
# TransformParams parameter model unit tests
#####################################################################################
@pytest.mark.parametrize(
    "unit_corrections,expectation",
    [
        pytest.param(
            dict(
                col="column",
                query="query string",
                valid_range={"lower_bound": 1.0, "upper_bound": 999},
                unit_conversions=[BTU_TO_MMBTU, KWH_TO_MWH],
            ),
            does_not_raise(),
            id="good_unit_corrections",
        ),
        pytest.param(
            dict(
                col="column",
                query="query string",
                valid_range={"lower_bound": 1.0, "upper_bound": 1000},
                unit_conversions=[BTU_TO_MMBTU, KWH_TO_MWH],
            ),
            pytest.raises(ValidationError),
            id="bad_unit_corrections",
        ),
    ],
)
def test_unit_corrections_distinct_domains(unit_corrections, expectation):
    """Make sure we are able to identify distinct vs. overlapping domains."""
    with expectation:
        _ = UnitCorrections(**unit_corrections)


#####################################################################################
# Series transformation unit tests
# These transform functions take and return single columns.
#####################################################################################
@pytest.mark.parametrize("series,expected", [(STRING_DATA.raw, STRING_DATA.norm)])
def test_normalize_strings(series: pd.Series, expected: pd.Series) -> None:
    """Test our string normalization function in isolation."""
    normalized = normalize_strings(series)
    assert_series_equal(normalized, expected, check_names=False)


@pytest.mark.parametrize(
    "series,expected,params", [(STRING_DATA.norm, STRING_DATA.cat, ANIMAL_CATS)]
)
def test_categorize_strings(series: pd.Series, expected: pd.Series, params) -> None:
    """Test string categorization function in isolation."""
    categorized = categorize_strings(series, params=StringCategories(**params))
    assert_series_equal(categorized, expected, check_names=False)


# @pytest.mark.parametrize("series,expected,params", [()])
def test_nullifiy_outliers():
    """Test outlier nullification function in isolation."""
    ...


# @pytest.mark.parametrize("series,expected,params", [()])
def test_convert_units():
    """Test unit conversion function in isolation.

    * Check column names are as expected.
    * Check that converted values seem correct.
    """
    ...


def test_convert_units_round_trip():
    """Generate random unit conversions and check that we can invert them."""
    for _ in range(0, 10):
        from_unit = "".join(
            random.choice(ascii_letters) for _ in range(10)  # nosec: B311
        )
        to_unit = "".join(
            random.choice(ascii_letters) for _ in range(10)  # nosec: B311
        )
        uc = UnitConversion(
            multiplier=np.random.uniform(-10, 10),
            adder=np.random.uniform(-10, 10),
            from_unit=from_unit,
            to_unit=to_unit,
        )

        dude = pd.Series((np.random.uniform(-10, 10, 1000)), name="dude")
        dude.name = "dude_" + from_unit
        wtf = convert_units(convert_units(dude, uc), uc.inverse())
        pd.testing.assert_series_equal(dude, wtf)


#####################################################################################
# Table transformation unit tests
# These transform functions operate on whole dataframes, not just single columns.
#####################################################################################
# @pytest.mark.parametrize("series,expected,params", [()])
def test_rename_columns():
    """Test column rename function in isolation."""
    ...


# @pytest.mark.parametrize("series,expected,params", [()])
def test_correct_units():
    """Test unit connection function in isolation.

    * Test that the order in which the conversions are applied doesn't matter.
    """
    ...


# @pytest.mark.parametrize("series,expected,params", [()])
def test_drop_invalid_rows():
    """Test our ability to select and drop invalid rows."""
    ...


#####################################################################################
# TableTransformer class unit tests.
#####################################################################################
@enum.unique
class TestTableId(enum.Enum):
    """Dummy test table IDs."""

    TEST_TABLE = "test_table"


class TestTableTransformer(AbstractTableTransformer):
    """A concrete TableTransformer for testing purposes."""

    table_id: enum.Enum = TestTableId.TEST_TABLE

    @cache_df(key="start")
    def transform_start(self, df: pd.DataFrame) -> pd.DataFrame:
        """Start the transform."""
        df = self.rename_columns(df).pipe(self.convert_units)
        return df

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """The main body of the transform."""
        df = (
            self.normalize_strings(df)
            .pipe(self.categorize_strings)
            .pipe(self.nullify_outliers)
            .pipe(self.correct_units)
        )
        return df

    @cache_df(key="end")
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finish up the transform."""
        df = self.drop_invalid_rows(df).convert_dtypes(convert_floating=False)
        return df


@pytest.mark.parametrize(
    "df,params,expected",
    [
        # pytest.param(STRING_DATA, STRING_PARAMS, STRING_EXPECTED, id="string_cleaning"),
        # pytest.param(UNITS_DF, UNITS_PARAMS, UNITS_EXPECTED, id="unit_conversion"),
    ],
)
def test_transform(df, params, expected):
    """Test the use of general transforms as part of a TableTransfomer class."""
    transformer = TestTableTransformer(
        params=params, cache_dfs=True, clear_cached_dfs=False
    )
    actual = transformer.transform(df)
    # assert_frame_equal(transformer._cached_dfs["start"], expected_start)
    # assert_frame_equal(transformer._cached_dfs["main"], expected_main)
    assert_frame_equal(transformer._cached_dfs["end"], expected)
    assert_frame_equal(actual, expected)
