"""Unit tests for TableTransformer and associated software tooling.

* Test the individual transform functions on static series / dataframes
* Test a dummy TableTransformer class that stitches together all of the
  general transform functions together with the class structure.
* Test that the TransformParams models succeed / fail / behave as expected.
"""

import enum
import random
from contextlib import nullcontext as does_not_raise
from datetime import date
from string import ascii_letters
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from pydantic import ValidationError

import pudl.logging
from pudl.transform.classes import (
    AbstractTableTransformer,
    InvalidRows,
    StringCategories,
    StringNormalization,
    TableTransformParams,
    TransformParams,
    UnitConversion,
    UnitCorrections,
    ValidRange,
    cache_df,
    categorize_strings,
    convert_units,
    correct_units,
    drop_invalid_rows,
    multicol_transform_factory,
    normalize_strings,
    nullify_outliers,
)
from pudl.transform.params.ferc1 import (
    BTU_TO_MMBTU,
    FERC1_STRING_NORM,
    FUEL_COST_PER_MMBTU_CORRECTIONS,
    FUEL_MMBTU_PER_UNIT_CORRECTIONS,
    KW_TO_MW,
    KWH_TO_MWH,
    PERCF_TO_PERMCF,
    PERGALLON_TO_PERBARREL,
    PERPOUND_TO_PERSHORTTON,
    VALID_COAL_MMBTU_PER_TON,
    VALID_GAS_MMBTU_PER_MCF,
    VALID_OIL_MMBTU_PER_BBL,
    VALID_PLANT_YEARS,
)

logger = pudl.logging.get_logger(__name__)

# Unit conversions that are only used in testing
PERTHERM_TO_PERMCF = dict(
    multiplier=10.37,
    from_unit="_per_therm",
    to_unit="_per_mcf",
)
PERGRAM_TO_PERSHORTTON = dict(
    multiplier=907185,
    from_unit="_per_kg",
    to_unit="_per_ton",
)
PERTON_TO_PERBARREL = dict(
    multiplier=(1.0 / 7.46),
    from_unit="_per_ton",
    to_unit="_per_bbl",
)
PERKGAL_TO_PERBARREL = dict(
    multiplier=42000.0,
    from_unit="_per_kgal",
    to_unit="_per_bbl",
)
PERFUCKTON_TO_PERSHORTTON = dict(
    multiplier=1.0e-9,
    adder=123.4e-9,
    from_unit="_per_fuckton",
    to_unit="_per_ton",
)
BUTTLOAD_TO_MMBUTTLOAD = dict(
    multiplier=1.0e9,
    adder=1.234e9,
    from_unit="_butt",
    to_unit="_mmbutt",
)

# Unit corrections only used in testing
COAL_MMBTU_PER_UNIT_CORRECTIONS = {
    "data_col": "fuel_mmbtu_per_unit",
    "cat_col": "fuel_type_code_pudl",
    "cat_val": "coal",
    "valid_range": VALID_COAL_MMBTU_PER_TON,
    "unit_conversions": [
        PERPOUND_TO_PERSHORTTON,
        BTU_TO_MMBTU,
        PERGRAM_TO_PERSHORTTON,
        PERFUCKTON_TO_PERSHORTTON,
    ],
}
GAS_MMBTU_PER_UNIT_CORRECTIONS = {
    "data_col": "fuel_mmbtu_per_unit",
    "cat_col": "fuel_type_code_pudl",
    "cat_val": "gas",
    "valid_range": VALID_GAS_MMBTU_PER_MCF,
    "unit_conversions": [
        PERCF_TO_PERMCF,
        BTU_TO_MMBTU,
        BUTTLOAD_TO_MMBUTTLOAD,
    ],
}
OIL_MMBTU_PER_UNIT_CORRECTIONS = {
    "data_col": "fuel_mmbtu_per_unit",
    "cat_col": "fuel_type_code_pudl",
    "cat_val": "oil",
    "valid_range": VALID_OIL_MMBTU_PER_BBL,
    "unit_conversions": [
        PERGALLON_TO_PERBARREL,
        PERTON_TO_PERBARREL,
        PERKGAL_TO_PERBARREL,
        BTU_TO_MMBTU,
    ],
}

TEST_FUEL_UNIT_CORRECTIONS = [
    COAL_MMBTU_PER_UNIT_CORRECTIONS,
    GAS_MMBTU_PER_UNIT_CORRECTIONS,
    OIL_MMBTU_PER_UNIT_CORRECTIONS,
]

BAD_TEST_FUEL_UNIT_CORRECTIONS = [
    *FUEL_MMBTU_PER_UNIT_CORRECTIONS,
    *FUEL_COST_PER_MMBTU_CORRECTIONS,
]

VALID_CAPACITY_MW = {
    "lower_bound": 0.0,
    "upper_bound": 4000.0,
}

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
        "rename_columns": {
            "columns": {
                "raw": "sushi",
                "norm": "orthogonal",
                "cat": "kitteh",
            },
        },
        "normalize_strings": {
            "sushi": FERC1_STRING_NORM,
        },
        "categorize_strings": {
            "sushi": ANIMAL_CATS,
        },
    }
}

NUMERIC_PARAMS = {
    "test_table": {
        "rename_columns": {
            "columns": {
                "capacity_mw": "capacity_mw_expected",
                "net_generation_mwh": "net_generation_mwh_expected",
            },
        },
        "nullify_outliers": {
            "valid_capacity_mw": VALID_CAPACITY_MW,
            "valid_year": VALID_PLANT_YEARS,
        },
        "convert_units": {
            "capacity_kw": KW_TO_MW,
            "net_generation_kwh": KWH_TO_MWH,
        },
        "drop_invalid_rows": {
            "invalid_values": [0, pd.NA, np.nan],
            "required_valid_cols": [
                "valid_year",
                "valid_capacity_mw",
                "net_generation_mwh",
            ],
        },
    }
}

STRING_DATA: pd.DataFrame = pd.DataFrame(
    columns=["raw", "norm", "cat"],
    data=[
        ("gato", "gato", "cat"),
        ("NEKO", "neko", "cat"),
        ("猫", "", pd.NA),
        ("ねこ", "", pd.NA),
        ("ネコ", "", pd.NA),
        ("\ua000", "", pd.NA),
        ("cAt", "cat", "cat"),
        (" lion ", "lion", "cat"),
        ("\tlion\t", "lion", "cat"),
        ("püma", "puma", "cat"),
        ("wolf?", "wolf", "dog"),
        ("dire\t      \twolf  ", "dire wolf", "dog"),
        ("hyeña", "hyena", pd.NA),
        ("ta66y", "ta66y", "cat"),
        ("pu$$y", "puy", pd.NA),
        ("", "", pd.NA),
        (pd.NA, "", pd.NA),
        (" ", "", pd.NA),
        (" \t ", "", pd.NA),
        ("\t\t", "", pd.NA),
        ("\x02steam (1)", "steam (1)", pd.NA),
    ],
).astype(
    {
        "norm": str,
        "cat": pd.StringDtype(),
    }
)

NUMERIC_DATA: pd.DataFrame = pd.DataFrame(
    columns=[
        "id",
        "year",
        "valid_year",
        "capacity_kw",
        "capacity_mw",
        "valid_capacity_mw",
        "net_generation_kwh",
        "net_generation_mwh",
    ],
    data=[
        (1, 1776, pd.NA, 1000.0, 1.0, 1.0, 1e6, 1000.0),
        (2, 1876, 1876, 2000.0, 2.0, 2.0, 2e6, 2000.0),
        (3, 1976, 1976, 3000.0, 3.0, 3.0, 3e6, 3000.0),
        (4, 2076, pd.NA, 4000.0, 4.0, 4.0, 4e6, 4000.0),
        (5, pd.NA, pd.NA, 5000.0, 5.0, 5.0, 5e6, 5000.0),
        (6, 2000, 2000, 6e6, 6000.0, np.nan, 6e6, 6000.0),
        (7, pd.NA, pd.NA, np.nan, np.nan, np.nan, 0.0, 0.0),
        (8, -52, pd.NA, -3000.0, -3.0, np.nan, 3e6, 3000.0),
        (9, 1850, 1850, 0.0, 0.0, 0.0, 3e6, 3000.0),
        (10, date.today().year, date.today().year, 4e6, 4000.0, 4000.0, 3e6, 3000.0),
    ],
).astype(
    {
        "id": int,
        "year": pd.Int64Dtype(),
        "valid_year": pd.Int64Dtype(),
        "capacity_kw": float,
        "capacity_mw": float,
        "valid_capacity_mw": float,
        "net_generation_kwh": float,
        "net_generation_mwh": float,
    }
)


#####################################################################################
# Infrastructure function tests.
#####################################################################################
class FillValue(TransformParams):
    """The simplest possible parameter."""

    fill_value: float | int


def fill_values(col: pd.Series, params: FillValue):
    """A very simple column transformer for testing multicolumn transform factory.

    Fills NA values with a number.
    """
    col = col.copy().fillna(params.fill_value)
    col.name = col.name + "_filled"
    return col


MULTICOL_PARAMS = {
    "a": {"fill_value": 42},
    "b": {"fill_value": 17},
    "c": {"fill_value": 0},
}

MULTICOL_TEST_DF = pd.DataFrame(
    columns=["a", "b", "c"],
    data=[
        (0.0, 1, 2),
        (np.nan, pd.NA, None),
    ],
)
MULTICOL_DROP_EXPECTED = pd.DataFrame(
    columns=["a_filled", "b_filled", "c_filled"],
    data=[
        (0.0, 1.0, 2.0),
        (42.0, 17.0, 0.0),
    ],
)
MULTICOL_NODROP_EXPECTED = pd.DataFrame(
    columns=["a", "b", "c", "a_filled", "b_filled", "c_filled"],
    data=[
        (0.0, 1, 2.0, 0.0, 1.0, 2.0),
        (np.nan, pd.NA, None, 42.0, 17.0, 0.0),
    ],
)


@pytest.mark.parametrize(
    "func,drop,df,expected,param_model,param_dict",
    [
        pytest.param(
            fill_values,
            True,
            MULTICOL_TEST_DF,
            MULTICOL_DROP_EXPECTED,
            FillValue,
            MULTICOL_PARAMS,
            id="multicol_transform_with_drop",
        ),
        pytest.param(
            fill_values,
            False,
            MULTICOL_TEST_DF,
            MULTICOL_NODROP_EXPECTED,
            FillValue,
            MULTICOL_PARAMS,
            id="multicol_transform_no_drop",
        ),
    ],
)
def test_multicol_transform_factory(func, drop, df, expected, param_model, param_dict):
    """Make sure our transform factory works."""
    func_multicol = multicol_transform_factory(func, drop=drop)
    assert func_multicol.__name__ == func.__name__ + "_multicol"  # nosec: B101
    params = {col: param_model(**param_dict[col]) for col in param_dict}
    actual = func_multicol(df, params=params)
    assert_frame_equal(actual, expected)


#####################################################################################
# TransformParams parameter model unit tests
#####################################################################################
@pytest.mark.parametrize(
    "unit_corrections,expectation",
    [
        pytest.param(
            dict(
                data_col="column",
                cat_col="categories",
                cat_val="cat",
                valid_range={"lower_bound": 1.0, "upper_bound": 999},
                unit_conversions=[BTU_TO_MMBTU, KWH_TO_MWH],
            ),
            does_not_raise(),
            id="distinct_unit_corrections",
        ),
        pytest.param(
            dict(
                data_col="column",
                cat_col="categories",
                cat_val="cat",
                valid_range={"lower_bound": 1.0, "upper_bound": 1000},
                unit_conversions=[BTU_TO_MMBTU, KWH_TO_MWH],
            ),
            pytest.raises(ValidationError),
            id="overlapping_unit_corrections",
        ),
        pytest.param(
            dict(
                data_col="column",
                cat_col="fuel_type",
                cat_val="gas",
                valid_range={"lower_bound": 0.3, "upper_bound": 3.3},
                unit_conversions=[PERTHERM_TO_PERMCF],
            ),
            pytest.raises(ValidationError),
            id="self_overlapping_unit_correction",
        ),
        pytest.param(
            COAL_MMBTU_PER_UNIT_CORRECTIONS,
            does_not_raise(),
            id="fake_coal_mmbtu_per_unit_corrections",
        ),
        pytest.param(
            GAS_MMBTU_PER_UNIT_CORRECTIONS,
            does_not_raise(),
            id="fake_gas_mmbtu_per_unit_corrections",
        ),
        pytest.param(
            OIL_MMBTU_PER_UNIT_CORRECTIONS,
            does_not_raise(),
            id="fake_oil_mmbtu_per_unit_corrections",
        ),
    ],
)
def test_unit_corrections_distinct_domains(unit_corrections, expectation):
    """Make sure we are able to identify distinct vs.

    overlapping domains.
    """
    with expectation:
        _ = UnitCorrections(**unit_corrections)


@pytest.mark.parametrize(
    "invalid_rows,expectation",
    [
        pytest.param(
            dict(invalid_values=[0, pd.NA], required_valid_cols=["a", "b"]),
            does_not_raise(),
            id="good_required_valid_cols",
        ),
        pytest.param(
            dict(invalid_values=[0, pd.NA], allowed_invalid_cols=["a", "b"]),
            does_not_raise(),
            id="good_allowed_invalid_cols",
        ),
        pytest.param(
            dict(
                invalid_values=[0, pd.NA],
                allowed_invalid_cols=["a", "b"],
                required_valid_cols=["a", "b"],
            ),
            pytest.raises(ValidationError),
            id="too_many_filters",
        ),
        pytest.param(
            dict(
                invalid_values=[0, pd.NA],
                like="dude",
                regex="wtf",
            ),
            pytest.raises(ValidationError),
            id="too_many_filters",
        ),
        pytest.param(
            dict(
                invalid_values=[0, pd.NA],
                required_valid_cols=["a", "b"],
                like="omg",
            ),
            pytest.raises(ValidationError),
            id="too_many_filters",
        ),
        pytest.param(
            dict(invalid_values=[0, pd.NA]),
            does_not_raise(),
            id="no_filters",
        ),
        pytest.param(
            dict(invalid_values=[], allowed_invalid_cols=["a", "b"]),
            pytest.raises(ValidationError),
            id="no_invalid_values_specified",
        ),
    ],
)
def test_invalid_row_validation(invalid_rows, expectation):
    """Make sure we catch invalid arguments to the InvalidRows model."""
    with expectation:
        _ = InvalidRows(**invalid_rows)


#####################################################################################
# Series transformation unit tests
# These transform functions take and return single columns.
#####################################################################################
@pytest.mark.parametrize(
    "series,expected,params", [(STRING_DATA.raw, STRING_DATA.norm, FERC1_STRING_NORM)]
)
def test_normalize_strings(series: pd.Series, expected: pd.Series, params) -> None:
    """Test our string normalization function in isolation."""
    normalized = normalize_strings(series, StringNormalization(**params))
    assert_series_equal(normalized, expected, check_names=False)


@pytest.mark.parametrize(
    "series,expected,params", [(STRING_DATA.norm, STRING_DATA.cat, ANIMAL_CATS)]
)
def test_categorize_strings(series: pd.Series, expected: pd.Series, params) -> None:
    """Test string categorization function in isolation."""
    categorized = categorize_strings(series, params=StringCategories(**params))
    assert_series_equal(categorized, expected, check_names=False)


@pytest.mark.parametrize(
    "series,expected,params",
    [
        pytest.param(
            NUMERIC_DATA.year,
            NUMERIC_DATA.valid_year,
            VALID_PLANT_YEARS,
            id="valid_plant_years",
        ),
        pytest.param(
            NUMERIC_DATA.capacity_mw,
            NUMERIC_DATA.valid_capacity_mw,
            VALID_CAPACITY_MW,
            id="valid_capacity_mw",
        ),
    ],
)
def test_nullify_outliers(series, expected, params):
    """Test outlier nullification function in isolation."""
    valid = nullify_outliers(series, params=ValidRange(**params))
    assert_series_equal(valid, expected, check_names=False)


@pytest.mark.parametrize(
    "series,expected,params",
    [
        pytest.param(
            NUMERIC_DATA.capacity_kw,
            NUMERIC_DATA.capacity_mw,
            KW_TO_MW,
            id="kw_to_mw",
        ),
        pytest.param(
            NUMERIC_DATA.net_generation_kwh,
            NUMERIC_DATA.net_generation_mwh,
            KWH_TO_MWH,
            id="kwh_to_mwh",
        ),
    ],
)
def test_convert_units(series, expected, params):
    """Test unit conversion function in isolation.

    * Check column names and converted values are as we expect.
    * Check that the inverse of the unit conversion gets us back to the original.
      (this will let us know if the column name substitution has a collision...)
    """
    uc = UnitConversion(**params)

    converted = convert_units(series, params=uc)
    assert_series_equal(converted, expected)

    converted_back = convert_units(converted, params=uc.inverse())
    assert_series_equal(converted_back, series)


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


def unit_corrections_are_homogeneous(corrections: list[UnitCorrections]) -> tuple():
    """Check that all unit corrections apply to same data and category columns.

    Assuming the list of unit corrections are homogeneous, return the names of the
    data colum, category column, and the categories that appear as a tuple.

    This is a helper function for test_correct_units().

    Args:
        corrections: The list of unit correction parameters to be used in generating
            test data.

    Returns:
         (data_col, cat_col, categories)
    """
    cat_cols = {uc.cat_col for uc in corrections}
    assert len(cat_cols) == 1  # nosec: B101
    cat_col = list(cat_cols)[0]

    data_cols = {uc.data_col for uc in corrections}
    assert len(data_cols) == 1  # nosec: B101
    data_col = list(data_cols)[0]

    categories = list({uc.cat_val for uc in corrections})

    return data_col, cat_col, categories


def make_unit_correction_test_data(
    corrections: list[UnitCorrections],
    nrows: int = 10_000,
) -> pd.DataFrame:
    """Create synthetic data for testing unit corrections.

    This is a helper function fo test_correct_units().

    Args:
        corrections: The list of unit correction parameters to be used in generating
            test data.
        nrows: Number of rows of synthetic data to generate.

    Returns:
        A dataframe that represents a perfectly dataset, as would be expected to result
        from correcting units in dirty data where only the provided unit corrections
        were responsible for discrepancies in the original data.
    """
    # Verify that all unit corrections are referring to the same data column and
    # categorical columns
    data_col, cat_col, categories = unit_corrections_are_homogeneous(corrections)

    df = pd.DataFrame(index=range(0, nrows))
    df[cat_col] = np.random.choice(categories, size=nrows)
    df[data_col] = np.nan

    # Assign data values
    for uc in corrections:
        mask = df[uc.cat_col] == uc.cat_val
        size = sum(mask)
        # Pick values from within the appropriate valid range
        df.loc[mask, data_col] = np.random.uniform(
            low=uc.valid_range.lower_bound,
            high=uc.valid_range.upper_bound,
            size=size,
        )

    return df


def scramble_units(
    df: pd.DataFrame,
    corrections: list[UnitCorrections],
) -> pd.DataFrame:
    """Scramble the units of a clean dataframe.

    This is a helper function for test_correct_units().

    Args:
        df: A clean dataframe with uniform units, as produced by
            make_unit_correction_test_data() above.
        corrections: The list of unit correction parameters that were used to generate
            the test data.

    Returns:
        A dataframe whose data has been scrambled randomly using the inverse of the
        unit conversions contained in corrections.
    """
    # Verify that all unit corrections are referring to the same
    # data column and categorical columns
    data_col, cat_col, categories = unit_corrections_are_homogeneous(corrections)
    df = df.copy()

    for corr in corrections:
        # Select only those records that this correction applies to:
        masked_df = df.loc[df[corr.cat_col] == corr.cat_val]
        for conv in corr.unit_conversions:
            # Select a subset of the applicable records to scramble using this
            # particular unit conversion:
            sampled_df = masked_df.sample(frac=1.0 / (len(corr.unit_conversions) + 2))
            # Use the inverse unit conversion to scramble the units:
            df.loc[sampled_df.index, "scrambled"] = convert_units(
                df.loc[sampled_df.index, corr.data_col], params=conv.inverse()
            )

    # Fill in any values that weren't scrambled with the original data
    df["scrambled"] = df["scrambled"].fillna(df[data_col])
    df[data_col + "_orig"] = df[data_col].copy()
    df[data_col] = df["scrambled"].copy()
    # At this point there should be no null values in the dataframe:
    assert df.notna().all(axis="columns").all()  # nosec: B101
    return df


@pytest.mark.parametrize(
    "corrections,expectation",
    [
        pytest.param(
            TEST_FUEL_UNIT_CORRECTIONS,
            does_not_raise(),
            id="fake_units",
        ),
        pytest.param(
            FUEL_MMBTU_PER_UNIT_CORRECTIONS,
            does_not_raise(),
            id="fuel_mmbtu_per_unit",
        ),
        pytest.param(
            FUEL_COST_PER_MMBTU_CORRECTIONS,
            does_not_raise(),
            id="fuel_cost_per_mmbtu",
        ),
        pytest.param(
            BAD_TEST_FUEL_UNIT_CORRECTIONS,
            pytest.raises(AssertionError),
            id="inconsistent_unit_corrections",
        ),
    ],
)
def test_correct_units(corrections, expectation):
    """Test our ability to correct units in isolation.

    First we construct a dataframe that has coherent units in a data column These units
    are different in different rows, but the units are consistent within groups that can
    be identified by another column. E.g. they vary by fuel type.

    Use the inverse of the unit conversions defined in the unit corrections to scramble
    the data, and then apply the unit conversions to unscramble it, and verify that we
    get the same input data back out.
    """
    with expectation:
        unit_corrections = [UnitCorrections(**uc) for uc in corrections]

        clean_df = make_unit_correction_test_data(
            corrections=unit_corrections,
            nrows=10_000,
        )

        scrambled_df = scramble_units(clean_df, unit_corrections)
        data_col, *_ = unit_corrections_are_homogeneous(unit_corrections)

        corrected_df = scrambled_df.copy()

        for corr in unit_corrections:
            corrected_df = correct_units(corrected_df, corr)

        assert_series_equal(
            corrected_df[data_col + "_orig"],
            corrected_df[data_col],
            check_names=False,
        )


@pytest.mark.parametrize(
    "df,expected,params",
    [
        pytest.param(
            NUMERIC_DATA,
            NUMERIC_DATA.loc[NUMERIC_DATA.id.isin([1, 2, 3, 4, 5, 6, 8, 9, 10])],
            dict(
                invalid_values=[0, pd.NA, np.nan],
                required_valid_cols=[
                    "valid_year",
                    "valid_capacity_mw",
                    "net_generation_mwh",
                ],
            ),
            id="required_valid_cols",
        ),
        pytest.param(
            NUMERIC_DATA,
            NUMERIC_DATA.loc[NUMERIC_DATA.id.isin([1, 2, 3, 4, 5, 6, 8, 9, 10])],
            dict(
                invalid_values=[0, pd.NA, np.nan],
                allowed_invalid_cols=[
                    "id",
                    "year",
                    "capacity_kw",
                    "capacity_mw",
                    "net_generation_kwh",
                ],
            ),
            id="allowed_invalid_cols",
        ),
    ],
)
def test_drop_invalid_rows(df, expected, params):
    """Test our ability to select and drop invalid rows."""
    invalid_row = InvalidRows(**params)
    actual = drop_invalid_rows(df, params=invalid_row)
    assert_frame_equal(actual, expected)


#####################################################################################
# TableTransformer class unit tests.
#####################################################################################
@enum.unique
class TableId(enum.Enum):
    """Dummy test table IDs."""

    TEST_TABLE = "test_table"


class TableTransformer(AbstractTableTransformer):
    """A concrete TableTransformer for testing purposes."""

    table_id: enum.Enum = TableId.TEST_TABLE

    @cache_df(key="start")
    def transform_start(self, df: pd.DataFrame) -> pd.DataFrame:
        """Start the transform."""
        df = self.rename_columns(df).pipe(self.convert_units)
        if self.cache_dfs:
            df["stage"] = "start"
        return df

    @cache_df(key="main")
    def transform_main(self, df: pd.DataFrame) -> pd.DataFrame:
        """The main body of the transform."""
        df = (
            self.normalize_strings(df)
            .pipe(self.categorize_strings)
            .pipe(self.nullify_outliers)
        )
        if self.cache_dfs:
            df["stage"] = "main"
        return df

    @cache_df(key="end")
    def transform_end(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finish up the transform."""
        df = self.drop_invalid_rows(df).convert_dtypes(convert_floating=False)
        if self.cache_dfs:
            df["stage"] = "end"
        return df


def test_transform(mocker):
    """Test the use of general transforms as part of a TableTransfomer class.

    This is trying to test the mechanics of the TableTransformer class, and not the
    individual transforms, since they are exercised in more specific testes above.

    However... the mocking doesn't seem to be working. The original functions are still
    getting called. Is that because they're getting called from within class methods?
    Or because they aren't normal "functions" and rather are callables that were
    constructed by the multicol_transform_factory function??
    """
    df = STRING_DATA
    params = STRING_PARAMS

    convert_units_mock: MagicMock = mocker.MagicMock(
        name="convert_units", return_value=df
    )
    normalize_strings_mock: MagicMock = mocker.MagicMock(
        name="normalize_strings", return_value=df
    )
    categorize_strings_mock: MagicMock = mocker.MagicMock(
        name="categorize_strings", return_value=df
    )
    nullify_outliers_mock: MagicMock = mocker.MagicMock(
        name="nullify_outliers", return_value=df
    )
    drop_invalid_rows_mock: MagicMock = mocker.MagicMock(
        name="drop_invalid_rows", return_value=df
    )

    mocker.patch(
        "pudl.transform.classes.convert_units_multicol",
        new=convert_units_mock,
    )
    mocker.patch(
        "pudl.transform.classes.normalize_strings_multicol",
        new=normalize_strings_mock,
    )
    mocker.patch(
        "pudl.transform.classes.categorize_strings_multicol",
        new=categorize_strings_mock,
    )
    mocker.patch(
        "pudl.transform.classes.nullify_outliers_multicol",
        new=nullify_outliers_mock,
    )
    mocker.patch(
        "pudl.transform.classes.drop_invalid_rows",
        new=drop_invalid_rows_mock,
    )

    params = TableTransformParams.from_dict(params["test_table"])
    transformer = TableTransformer(params=params)
    _ = transformer.transform(df)

    # Mock can't compare dataframes since it uses the == operator, and for dataframes
    # that returns a dataframe of bools, so we have to do it explicitly:
    convert_units_mock.assert_called_once()
    # assert_frame_equal(df, convert_units_mock.call_args.args[0])
    assert isinstance(convert_units_mock.call_args.args[0], pd.DataFrame)  # nosec: B101
    assert params.convert_units == convert_units_mock.call_args.args[1]  # nosec: B101

    normalize_strings_mock.assert_called_once()
    # assert_frame_equal(df, normalize_strings_mock.call_args.args[0])
    assert isinstance(  # nosec: B101
        normalize_strings_mock.call_args.args[0], pd.DataFrame
    )
    assert (  # nosec: B101
        params.normalize_strings == normalize_strings_mock.call_args.args[1]
    )

    categorize_strings_mock.assert_called_once()
    # assert_frame_equal(df, categorize_strings_mock.call_args.args[0])
    assert isinstance(  # nosec: B101
        categorize_strings_mock.call_args.args[0], pd.DataFrame
    )
    assert (  # nosec: B101
        params.categorize_strings == categorize_strings_mock.call_args.args[1]
    )

    nullify_outliers_mock.assert_called_once()
    # assert_frame_equal(df, nullify_outliers_mock.call_args.args[0])
    assert isinstance(  # nosec: B101
        nullify_outliers_mock.call_args.args[0], pd.DataFrame
    )
    assert (  # nosec: B101
        params.nullify_outliers == nullify_outliers_mock.call_args.args[1]
    )

    drop_invalid_rows_mock.assert_called_once()
    assert_frame_equal(df, drop_invalid_rows_mock.call_args.args[0])
    assert (  # nosec: B101
        params.drop_invalid_rows == drop_invalid_rows_mock.call_args.args[1]
    )

    caching_transformer = TableTransformer(
        params=params,
        cache_dfs=True,
        clear_cached_dfs=False,
    )
    actual = caching_transformer.transform(df)
    # The dataframe shouldn't change at all after the last stage is cached:
    assert_frame_equal(actual, caching_transformer._cached_dfs["end"])
    # Check that the dataframes were cached as expected after each stage:
    for stage in ["start", "main", "end"]:
        assert (  # nosec B101
            (caching_transformer._cached_dfs[stage]["stage"] == stage).all()
        ).all()
