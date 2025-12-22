"""Tests for timeseries anomalies detection and imputation."""

import geopandas as gpd
import numpy as np
import pandas as pd
import polars as pl
import pytest

from pudl.analysis.state_demand import (
    lookup_state,
    out_ferc714__hourly_estimated_state_demand,
)

AK_FIPS = {"name": "Alaska", "code": "AK", "fips": "02"}


@pytest.mark.parametrize(
    "state,expected",
    [
        ("Alaska", AK_FIPS),
        ("alaska", AK_FIPS),
        ("ALASKA", AK_FIPS),
        ("AK", AK_FIPS),
        ("ak", AK_FIPS),
        (2, AK_FIPS),
        ("02", AK_FIPS),
        ("2", AK_FIPS),
        (2.0, AK_FIPS),
        (np.int64(2), AK_FIPS),
        pytest.param(99, {}, marks=pytest.mark.xfail),
        pytest.param("Oaxaca", {}, marks=pytest.mark.xfail),
        pytest.param("MX", {}, marks=pytest.mark.xfail),
        pytest.param(np.nan, {}, marks=pytest.mark.xfail),
        pytest.param(pd.NA, {}, marks=pytest.mark.xfail),
        pytest.param("", {}, marks=pytest.mark.xfail),
        pytest.param(None, {}, marks=pytest.mark.xfail),
    ],
)
def test_lookup_state(state: str | int, expected: dict[str, str | int]) -> None:
    """Check that various kinds of state lookups work."""
    assert lookup_state(state) == expected


@pytest.fixture
def out_ferc714__hourly_estimated_state_demand_args():
    return {
        # two respondents with total demand 2 and average 1
        "out_ferc714__hourly_planning_area_demand": pl.DataFrame(
            {
                "respondent_id_ferc714": [1, 2],
                "demand_imputed_pudl_mwh": [1, 1],
                # datetime doesn't matter; using 1900 for its obvious placeholder vibes
                "datetime_utc": pd.to_datetime(["1900-01-01T12:00:00"] * 2),
            }
        ).lazy(),
        # toy county, but the first two chars have to line up with a state listed in POLITICAL_SUBDIVISIONS
        # so let's go with Alabama
        "out_censusdp1tract__counties": gpd.GeoDataFrame(
            {
                "county_id_fips": ["01000"],
                "dp0010001": [1],
            }
        ),
        # assign both respondents to the same county
        "out_ferc714__respondents_with_fips": pd.DataFrame(
            {
                "respondent_id_ferc714": [1, 2],
                "county_id_fips": ["01000"] * 2,
                "report_date": pd.to_datetime(["1900-01-01"] * 2),
            }
        ),
    }


def mock_out_ferc714__hourly_estimated_state_demand_context(
    mocker, mean_overlaps: bool
):
    """Generate a mock context suitable for use in out_ferc714__hourly_estimated_state_demand."""
    # this is as gross as it is because Dagster messes with the context between when you
    # call out_ferc714__hourly_estimated_state_demand and when
    # out_ferc714__hourly_estimated_state_demand actually executes.
    # the thing out_ferc714__hourly_estimated_state_demand actually receives is
    # context.bind(), so we make a thing such that
    # context.bind().op_config["mean_overlaps"] has the value we want.
    return mocker.Mock(
        bind=mocker.Mock(
            return_value=mocker.Mock(op_config={"mean_overlaps": mean_overlaps})
        )
    )


def test_sales(mocker, out_ferc714__hourly_estimated_state_demand_args):
    pd.testing.assert_frame_equal(
        out_ferc714__hourly_estimated_state_demand(
            # we need this context because we're calling the asset def directly instead of using dg.materialize().
            # arbitrarily we choose to run with mean_overlaps off, since that's the default.
            context=mock_out_ferc714__hourly_estimated_state_demand_context(
                mocker, mean_overlaps=False
            ),
            # pass in non-None core_eia861__yearly_sales
            core_eia861__yearly_sales=pd.DataFrame(
                {
                    "state": ["AL"],
                    "report_date": pd.to_datetime(["1900-01-01"]),
                    "sales_mwh": [10.0],
                }
            ),
            **out_ferc714__hourly_estimated_state_demand_args,
        )
        .collect()
        .to_pandas(),
        pd.DataFrame(
            {
                "state_id_fips": ["01"],
                "datetime_utc": pd.to_datetime(["1900-01-01T12:00:00"]),
                "demand_mwh": [2.0],
                # this column must be present when core_eia861__yearly_sales is non-None
                "scaled_demand_mwh": [10.0],
            }
        ),
    )


@pytest.mark.parametrize(
    "mean_overlaps,expected",
    [
        (
            True,
            pd.DataFrame(
                {
                    "state_id_fips": ["01"],
                    "datetime_utc": pd.to_datetime(["1900-01-01T12:00:00"]),
                    "demand_mwh": [1.0],  # use the average
                }
            ),
        ),
        (
            False,
            pd.DataFrame(
                {
                    "state_id_fips": ["01"],
                    "datetime_utc": pd.to_datetime(["1900-01-01T12:00:00"]),
                    "demand_mwh": [2.0],  # use the sum
                }
            ),
        ),
    ],
)
def test_overlaps(
    mocker, out_ferc714__hourly_estimated_state_demand_args, mean_overlaps, expected
):
    """Make sure we average county overlaps when mean_overlaps is true, and sum them when mean_overlaps is false."""

    pd.testing.assert_frame_equal(
        expected,
        out_ferc714__hourly_estimated_state_demand(
            context=mock_out_ferc714__hourly_estimated_state_demand_context(
                mocker, mean_overlaps=mean_overlaps
            ),
            **out_ferc714__hourly_estimated_state_demand_args,
        )
        .collect()
        .to_pandas(),
    )
