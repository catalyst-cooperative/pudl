"""Unit tests for clean up of FERC Form 1."""

import logging

import numpy as np
import pandas as pd
import pytest

from pudl.analysis.clean_up_ferc1 import (associate_notes_with_values_sg,
                                          extract_ferc_license_sg,
                                          improve_plant_type_sg,
                                          label_row_type_sg,
                                          remove_bad_rows_sg,
                                          remove_header_note_rows_sg)
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)


TEST_DF = pd.DataFrame({
    "utility_id_ferc1": 1,
    "plant_name_ferc1": "test_plant",
    "construction_year": [np.nan],
    "net_generation_mwh": [np.nan],
    "total_cost_of_plant": [np.nan],
    "capex_per_mw": [np.nan],
    "opex_total": [np.nan],
    "opex_fuel": [np.nan],
    "opex_maintenance": [np.nan],
    "fuel_cost_per_mmbtu": [np.nan],
    "ferc_license_id": [np.nan],
    "plant_type": [np.nan],
    "test": [np.nan]
})

LABEL_ROWS_EXPECTED = pd.DataFrame({
    "utility_id_ferc1": [1, 1, 1, 1, 1, 1, 1, 1],
    "report_year": [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
    "plant_name_ferc1": ["steam", "plant1", "note1", "note2", "hydro", "plant2", "total", "note"],
    "row_type": ["header", np.nan, "note", "note", "header", np.nan, "total", "note"],
    "construction_year": [np.nan, 1999, np.nan, np.nan, np.nan, 1996, np.nan, np.nan],
    "net_generation_mwh": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "total_cost_of_plant": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "capex_per_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "opex_total": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "opex_fuel": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "opex_maintenance": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "fuel_cost_per_mmbtu": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "capacity_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
    "plant_type": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
}).pipe(apply_pudl_dtypes, group='ferc1')


@pytest.mark.parametrize(
    "df_update", [
        (pd.DataFrame({'test': [0]})),
        pytest.param(pd.DataFrame({'opex_fuel': [0]}), marks=pytest.mark.xfail),
        (pd.DataFrame({'opex_fuel': [0], "plant_name_ferc1": ["-------"]})),
        pytest.param(pd.DataFrame(
            {"opex_fuel": [0], "plant_name_ferc1": ["--"]}), marks=pytest.mark.xfail),
        (pd.DataFrame({'opex_fuel': [0], 'plant_name_ferc1': ['']})),
        (pd.DataFrame({'opex_fuel': [0], 'plant_name_ferc1': ['none']})),
        (pd.DataFrame({'opex_fuel': [0], 'plant_name_ferc1': ['na']})),
        (pd.DataFrame({'opex_fuel': [0], 'plant_name_ferc1': ['n/a']})),
        (pd.DataFrame({'opex_fuel': [0], 'plant_name_ferc1': ['not applicable']})),
    ]
)
def test_remove_bad_rows_sg(df_update):
    """Test remove_bad_rows_sg function.

    There are three types of test here:
    - Test a row is removed if all values in clean_up_ferc1.NAN_COLS for a given utility
      are NA.
    - Test that all rows with more than two dashes in the plant name get removed.
    - Test that all rows with a NA-ish plant_name_ferc1 value get removed.

    """
    remove_bad_rows_input = TEST_DF.copy()
    remove_bad_rows_input.update(df_update)
    assert remove_bad_rows_sg(
        remove_bad_rows_input).empty is True, "Test DataFrame should be empty"


def test_label_row_type_sg():
    """Test label_row_type_sg function.

    This test is a little more complicated. There are four types of row lables: header,
    note, total, and NA. Note rows are based on "clumps" of rows deemed to be possible
    headers. Because these steps are interrelated, it makes more sense to compare input
    and output dataframes rather than parameterize. It also makes sense to feed this
    test function a DataFrame with more than one row so that it can test the clumping.

    """
    label_rows_input = pd.DataFrame({
        "utility_id_ferc1": [1, 1, 1, 1, 1, 1, 1, 1],
        "report_year": [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
        "plant_name_ferc1": ["steam", "plant1", "note1", "note2", "hydro", "plant2", "total", "note"],
        "construction_year": [np.nan, 1999, np.nan, np.nan, np.nan, 1996, np.nan, np.nan],
        "net_generation_mwh": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "total_cost_of_plant": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "capex_per_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_total": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_fuel": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_maintenance": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "fuel_cost_per_mmbtu": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "capacity_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "plant_type": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
    })

    label_rows_actual = (
        label_row_type_sg(label_rows_input)
        .pipe(apply_pudl_dtypes, group='ferc1')
    )

    pd.testing.assert_frame_equal(LABEL_ROWS_EXPECTED, label_rows_actual)


def test_improve_plant_type_sg_1():
    """Test improve_plant_type_sg apsect that moves headers to plant_type_2.

    This test looks to see that the plant_type_2 column has been added with the accurate
    technology types. It uses the expected output from the label_row_type function as
    an input.

    """
    improve_plant_type_input = LABEL_ROWS_EXPECTED.copy()

    improve_plant_type_actual = (
        improve_plant_type_sg(improve_plant_type_input)
        .pipe(apply_pudl_dtypes, group='ferc1')
    )

    improve_plant_type_expected = pd.DataFrame({
        "utility_id_ferc1": [1, 1, 1, 1, 1, 1, 1, 1],
        "report_year": [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
        "plant_name_ferc1": ["steam", "plant1", "note1", "note2", "hydro", "plant2", "total", "note"],
        "row_type": ["header", np.nan, "note", "note", "header", np.nan, "total", "note"],
        "construction_year": [np.nan, 1999, np.nan, np.nan, np.nan, 1996, np.nan, np.nan],
        "net_generation_mwh": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "total_cost_of_plant": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "capex_per_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_total": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_fuel": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "opex_maintenance": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "fuel_cost_per_mmbtu": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "capacity_mw": [np.nan, 1000, np.nan, np.nan, np.nan, 1000, 2000, np.nan],
        "plant_type": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        "plant_type_2": ["steam_heat", "steam_heat", np.nan, np.nan, "hydro", "hydro", "hydro", np.nan]
    }).pipe(apply_pudl_dtypes, group='ferc1')

    pd.testing.assert_frame_equal(
        improve_plant_type_actual, improve_plant_type_expected)


@pytest.mark.parametrize(
    "sg_df", [
        (pd.DataFrame({"plant_name_ferc1": ["hydro 1"],
                       "plant_type_2": [np.nan],
                       "plant_type": [np.nan],
                       "row_type": [np.nan],
                       "report_year": [2000],
                       "utility_id_ferc1": [1]})),
        pytest.param(pd.DataFrame({"plant_name_ferc1": ["hydro 1"],
                                   "plant_type_2": [np.nan],
                                   "plant_type": [np.nan],
                                   "row_type": ["note"],
                                   "report_year": [2000],
                                   "utility_id_ferc1": [1]}), marks=pytest.mark.xfail),
        pytest.param(pd.DataFrame({"plant_name_ferc1": ["plant 1"],
                                   "plant_type_2": [np.nan],
                                   "plant_type": [np.nan],
                                   "row_type": [np.nan],
                                   "report_year": [2000],
                                   "utility_id_ferc1": [1]}), marks=pytest.mark.xfail),
    ]
)
def test_improve_plant_type_sg_2(sg_df):
    """Test the improve_plant_type_sg aspect that labels misc hydro records."""
    assert improve_plant_type_sg(sg_df)['plant_type_2'].item() == "hydro"


@pytest.mark.parametrize(
    "df_update", [
        (pd.DataFrame({"plant_name_ferc1": ["no. 8888"]})),
        (pd.DataFrame({"plant_name_ferc1": ["ferc 8888"]})),
        (pd.DataFrame({"plant_name_ferc1": ["8888"]})),
        pytest.param(pd.DataFrame(
            {"plant_name_ferc1": ["ferc"]}), marks=pytest.mark.xfail),
        pytest.param(pd.DataFrame(
            {"plant_name_ferc1": ["ferc page 8888"]}), marks=pytest.mark.xfail),
        pytest.param(pd.DataFrame(
            {"plant_name_ferc1": ["ferc 2001"]}), marks=pytest.mark.xfail),
        pytest.param(pd.DataFrame(
            {"plant_name_ferc1": ["ferc page 8888"],
             "plant_type": ["wind"]}), marks=pytest.mark.xfail),
    ]
)
def test_extract_ferc_license_sg(df_update):
    """Test extract_ferc_license function."""
    extract_ferc_license_input = TEST_DF.copy()
    extract_ferc_license_input.update(df_update)

    extract_ferc_license_actual = extract_ferc_license_sg(extract_ferc_license_input)

    assert "ferc_license_id" in extract_ferc_license_actual.columns
    assert "ferc_license_manual" in extract_ferc_license_actual.columns
    assert extract_ferc_license_actual.ferc_license_id.item() == 8888


def test_associate_notes_with_values_sg():
    """Test associate_notes_with_values_sg function."""
    associate_notes_with_values_input = pd.DataFrame({
        "report_year": [2000, 2000, 2000, 2000],
        "utility_id_ferc1": [1, 1, 1, 1],
        "plant_name_ferc1": ["plant1 (a)", "plant2 (2)", "(a) #1111", "(2) #8888"],
        "row_type": [np.nan, np.nan, "note", "note"],
        "ferc_license_id": [np.nan, np.nan, 1111, 8888]
    })

    associate_notes_with_values_actual = associate_notes_with_values_sg(
        associate_notes_with_values_input
    ).pipe(apply_pudl_dtypes, group='ferc1')

    associate_notes_with_values_expected = pd.DataFrame({
        "report_year": [2000, 2000, 2000, 2000],
        "utility_id_ferc1": [1, 1, 1, 1],
        "plant_name_ferc1": ["plant1 (a)", "plant2 (2)", "(a) #1111", "(2) #8888"],
        "row_type": [np.nan, np.nan, "note", "note"],
        "ferc_license_id": [1111, 8888, 1111, 8888],
        "notes": ["(a) #1111", "(2) #8888", np.nan, np.nan]
    }).pipe(apply_pudl_dtypes, group='ferc1')

    pd.testing.assert_frame_equal(
        associate_notes_with_values_actual, associate_notes_with_values_expected)


def test_remove_header_note_rows_sg():
    """Test remove_header_note_rows_sg function."""
    remove_header_note_rows_input = LABEL_ROWS_EXPECTED.copy()
    remove_header_note_rows_actual = (
        remove_header_note_rows_sg(remove_header_note_rows_input))

    assert ~remove_header_note_rows_actual["row_type"].str.contains('header|note').any()
