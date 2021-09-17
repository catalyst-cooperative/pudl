"""Unit tests for the :mod:`pudl.analysis.ramp_rates` module."""
from typing import Dict, Sequence

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

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


class TestRampRatePipeline:
    """Test the full ramp rates analysis pipeline.

    Due to the size of the CEMS dataset, these functions operate in place to minimize copying.
    The downside of this approach is that it produces a long dependency chain of side effects.

    To break this chain for testing, I define the expected outputs in separate methods
    so that the next test can recreate the values as inputs.
    """

    def expected_add_startup_shutdown_timestamps(self, cems: pd.DataFrame) -> pd.DataFrame:
        """Make expected values."""
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
        expected_startups = cems['operating_datetime_utc'].where(
            startup_indicators, pd.NaT).rename('startups')
        expected_shutdowns = cems['operating_datetime_utc'].where(
            shutdown_indicators, pd.NaT).rename('shutdowns')
        return cems.assign(startups=expected_startups, shutdowns=expected_shutdowns)

    def test_add_startup_shutdown_timestamps(self, dummy_cems):
        """Test startup and shutdown timestamps are correct and that intermediate columns were dropped."""
        actual = dummy_cems
        expected = self.expected_add_startup_shutdown_timestamps(dummy_cems)

        rr.add_startup_shutdown_timestamps(actual)
        assert_frame_equal(actual, expected)

    def expected__fill_startups_shutdowns(self, cems: pd.DataFrame) -> pd.DataFrame:
        """Make expected values."""
        previous_startups = [
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2020-01-01 01:00:00+00:00"),  # end unit 1
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-31 23:00:00+00:00"),
            pd.Timestamp("2019-12-31 23:00:00+00:00"),
            pd.Timestamp("2019-12-31 23:00:00+00:00"),  # end unit 2
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
            pd.Timestamp("2019-12-30 22:00:00+00:00"),
        ]
        next_shutdowns = [
            pd.Timestamp("2019-12-31 23:00:00+00:00"),
            pd.Timestamp("2019-12-31 23:00:00+00:00"),
            pd.Timestamp("2020-01-02 01:00:00+00:00"),
            pd.Timestamp("2020-01-02 01:00:00+00:00"),  # end unit 1
            pd.Timestamp("2020-01-01 01:00:00+00:00"),
            pd.Timestamp("2020-01-01 01:00:00+00:00"),
            pd.Timestamp("2020-01-01 01:00:00+00:00"),
            pd.Timestamp("2020-01-01 01:00:00+00:00"),  # end unit 2
            pd.Timestamp("2020-01-01 00:00:00+00:00"),
            pd.Timestamp("2020-01-01 00:00:00+00:00"),
            pd.Timestamp("2020-01-01 00:00:00+00:00"),
            pd.Timestamp("2020-01-02 01:00:00+00:00"),
        ]
        return cems.assign(startups=previous_startups, shutdowns=next_shutdowns)

    def test__fill_startups_shutdowns(self, dummy_cems):
        """Test that startup/shutdown timestamp columns are converted to previous/next startup/shutdown timestamps."""
        actual = self.expected_add_startup_shutdown_timestamps(dummy_cems)
        expected = self.expected__fill_startups_shutdowns(dummy_cems)

        rr._fill_startups_shutdowns(actual)
        assert_frame_equal(actual, expected)

    def expected__distance_from_downtime(self, cems: pd.DataFrame) -> pd.DataFrame:
        """Make expected values."""
        hours_from_startup = [
            24, 25, 26, 0,
            24, 0, 1, 2,
            24, 25, 26, 27,
        ]
        hours_to_shutdown = [
            1, 0, 25, 24,
            3, 2, 1, 0,
            2, 1, 0, 24,
        ]
        expected = (
            cems
            .assign(hours_from_startup=hours_from_startup, hours_to_shutdown=hours_to_shutdown)
            .astype(dict(hours_from_startup=np.float32, hours_to_shutdown=np.float32))
        )
        return expected

    def test__distance_from_downtime(self, dummy_cems):
        """Compute the distance, in hours, from current timestamp to previous/next startup/shutdown timestamps."""
        actual = self.expected__fill_startups_shutdowns(dummy_cems)
        expected = self.expected__distance_from_downtime(dummy_cems)

        rr._distance_from_downtime(actual)
        assert_frame_equal(actual, expected)
