"""Unit tests for the :mod:`pudl.analysis.ramp_rates` module."""
from typing import Dict, Sequence

import pandas as pd
import pytest
from pandas.testing import assert_series_equal  # , assert_frame_equal

import pudl.analysis.ramp_rates as rr


def df_from_product(inputs: Dict[str, Sequence], as_index=True) -> pd.DataFrame:
    """Make a dataframe from cartesian product of input sequences.

    Args:
        inputs (Dict[str, Sequence]): dataframe column names mapped
            to their unique values.
        as_index (bool): whether to set the product as the index

    Return:
        df (pd.DataFrame): cartesian product dataframe
    """
    names, iterables = zip(*inputs.items())
    df = pd.MultiIndex.from_product(
        iterables, names=names).to_frame(index=as_index)

    return df


@pytest.fixture()
def dummy_cems():
    """
    EPA CEMS.

    unit_id_epa     operating_datetime_utc  plant_id  unitid  gross_load_mw
              0  2019-12-31 22:00:00+00:00        10       a              1
              0  2019-12-31 23:00:00+00:00        10       a              0
              0  2020-01-01 00:00:00+00:00        10       a              0
              0  2020-01-01 01:00:00+00:00        10       a              1
              1  2019-12-31 22:00:00+00:00        11       a              0
              1  2019-12-31 23:00:00+00:00        11       a              1
              1  2020-01-01 00:00:00+00:00        11       a              1
              1  2020-01-01 01:00:00+00:00        11       a              0
              2  2019-12-31 22:00:00+00:00        11       b              1
              2  2019-12-31 23:00:00+00:00        11       b              1
              2  2020-01-01 00:00:00+00:00        11       b              0
              2  2020-01-01 01:00:00+00:00        11       b              0
    """
    inputs = dict(
        unit_id_epa=[0, 1, 2],
        operating_datetime_utc=pd.date_range(
            start="2019-12-31 22:00",
            end="2020-01-01 01:00",
            freq="h",
            tz='UTC'
        ),
    )
    cems = df_from_product(inputs, as_index=False)
    # add composite keys
    cems['plant_id'] = cems['unit_id_epa'].map({0: 10, 1: 11, 2: 11})
    cems['unitid'] = cems['unit_id_epa'].map({0: 'a', 1: 'a', 2: 'b'})
    # add values
    lst = [
        1, 0, 0, 1,  # start, end with 1
        0, 1, 1, 0,  # start, end with 0
        1, 1, 0, 0  # mix
    ]
    cems['gross_load_mw'] = lst

    cems = cems.set_index(["unit_id_epa", "operating_datetime_utc"], drop=False)
    return cems


@pytest.fixture()
def dummy_crosswalk():
    """EPA Crosswalk."""
    raise NotImplementedError


def test__sorted_groupby_diff(dummy_cems):
    """Test equivalence to groupby."""
    actual = rr._sorted_groupby_diff(
        dummy_cems['gross_load_mw'], dummy_cems['unit_id_epa'])
    expected = dummy_cems.groupby(level='unit_id_epa')[
        'gross_load_mw'].transform(lambda x: x.diff())
    assert_series_equal(actual, expected)


def test_add_startup_shutdown_timestamps(dummy_cems):
    """Test startup and shutdown timestamps are correct and that intermediate columns were dropped."""
    original_cols = list(dummy_cems.columns)
    startup_indicators = [
        False, False, False, True,
        False, True, False, False,
        False, False, False, False,
    ]
    shutdown_indicators = [
        False, True, False, False,
        False, False, False, True,
        False, False, True, False,
    ]
    expected_startups = dummy_cems['operating_datetime_utc'].where(
        startup_indicators, pd.NaT).rename('startups')
    expected_shutdowns = dummy_cems['operating_datetime_utc'].where(
        shutdown_indicators, pd.NaT).rename('shutdowns')

    rr.add_startup_shutdown_timestamps(dummy_cems)
    actual_startups = dummy_cems['startups']
    actual_shutdowns = dummy_cems['shutdowns']

    assert_series_equal(actual_shutdowns, expected_shutdowns)
    assert_series_equal(actual_startups, expected_startups)

    # check for intermediate columns
    final_cols = set(dummy_cems.columns)
    assert set(original_cols + ['startups', 'shutdowns']) == final_cols
