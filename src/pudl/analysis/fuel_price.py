"""Methods for estimating redacted EIA-923 fuel price information."""
import logging
from collections import OrderedDict
from typing import Literal, TypedDict

import numpy as np
import pandas as pd

from pudl.metadata.enums import STATE_TO_CENSUS_REGION

logger = logging.getLogger(__name__)


class FuelPriceAgg(TypedDict):
    """A data structure for storing fuel price aggregation arguments."""

    agg_cols: list[str]
    fuel_group_eiaepm: Literal[
        "all",
        "coal",
        "natural_gas",
        "other_gas",
        "petroleum",
        "petroleum_coke",
    ]


FUEL_PRICE_AGGS: OrderedDict[str, FuelPriceAgg] = OrderedDict(
    {
        # The most precise estimator we have right now
        "state_esc_month": {
            "agg_cols": ["state", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Good for coal, since price varies much more with location than time
        "state_esc_year": {
            "agg_cols": ["state", "energy_source_code", "report_year"],
            "fuel_group_eiaepm": "coal",
        },
        # Good for oil products, because prices are consistent geographically
        "region_esc_month": {
            "agg_cols": ["census_region", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "petroleum",
        },
        # Less fuel specificity, but still precise date and location
        "state_fgc_month": {
            "agg_cols": ["state", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Less location and fuel specificity
        "region_fgc_month": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "region_fgc_year": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
        "national_esc_month": {
            "agg_cols": ["energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_month": {
            "agg_cols": ["fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_year": {
            "agg_cols": ["fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
    }
)
"""Fuel price aggregations ordered by precedence for filling missing values.

Precendece was largely determined by which aggregations resulted in the best
reproduction of reported fuel prices, as measured by the interquartile range of the
normalized difference between the estimate and the reported value:

(estimated fuel price - reported fuel price) / reported_fuel_price
"""


def weighted_median(df: pd.DataFrame, data: str, weights: str, dropna=True) -> float:
    """Calculate the median of the data column, weighted by the weights column.

    Suitable for use with df.groupby().apply().

    Args:
        df: DataFrame containing both the data whose weighted median we want to
            calculate, and the weights to use.
        data: Label of the column containing the data.
        weights: Label of the column containing the weights.
        dropna: If True, ignore rows where either data or weights are NA. If False, any
            NA value in either data or weights means the weighted median is also NA.

    Returns:
        A single weighted median value, or NA.
    """
    if dropna:
        df = df.dropna(subset=[data, weights])
    if df.empty | df[[data, weights]].isna().any(axis=None):
        return np.nan
    s_data, s_weights = map(np.array, zip(*sorted(zip(df[data], df[weights]))))
    midpoint = 0.5 * sum(s_weights)
    if any(df[weights] > midpoint):
        w_median = df.loc[df[weights].idxmax(), data]
    else:
        cs_weights = np.cumsum(s_weights)
        idx = np.where(cs_weights <= midpoint)[0][-1]
        if cs_weights[idx] == midpoint:
            w_median = np.mean(s_data[idx : idx + 2])
        else:
            w_median = s_data[idx + 1]
    return w_median


def weighted_modified_zscore(
    df: pd.DataFrame,
    data: str,
    weights: str,
    dropna: bool = True,
) -> pd.Series:
    """Calculate the modified z-score using a weighted median.

    Args:
        df: DataFrame containing the data whose weighted modified z-score we want to
            calculate, and the weights to use when calculating median values.
        data: Label of the column containing the data.
        weights: Label of the column containing the weights.
        dropna: Whether to drop NA values when calculating medians. Passed through
            to :func:`weighted_median`

    Returns:
        Series with the same index as the input DataFrame,
    """
    wm = weighted_median(df, data=data, weights=weights, dropna=dropna)
    delta = (df[data] - wm).abs()
    return (0.6745 * delta) / delta.median()


def aggregate_price_median(
    frc: pd.DataFrame,
    aggs: OrderedDict[FuelPriceAgg] | None = None,
    agg_mod_zscore: list[str] | None = None,
    max_mod_zscore: float = 5.0,
    debug: bool = False,
) -> pd.DataFrame:
    """Fill in missing fuel prices with median values using various aggregations.

    Aggregations are done across space (state, census region, country), time (month or
    year), and fuel groups (coal, petroleum, natural gas).

    Args:
        frc: a Fuel Receipts and Costs dataframe from EIA 923.
        aggs: Ordered sequence of fuel price aggregations to apply.
        mod_zscore_agg: Columns to group by when identifying fuel, location, or time
            period specific outlying fuel prices.
        max_mod_zscore: The modified z-score beyond which a fuel price will be
            considered an outlier, get removed, and be filled in.
        debug: If True, retain intermediate columns used in the calculation.

    Returns:
        A Fuel Receipts and Costs table that includes fuel price estimates for all
        missing records and replaced outliers.
    """
    if aggs is None:
        aggs = FUEL_PRICE_AGGS
    if agg_mod_zscore is None:
        agg_mod_zscore = ["report_year", "fuel_group_eiaepm"]

    logger.info("Filling in missing fuel prices using aggregated median values.")

    frc = frc.assign(
        report_year=lambda x: x.report_date.dt.year,
        census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
        fuel_cost_per_mmbtu=lambda x: x.fuel_cost_per_mmbtu.replace(0.0, np.nan),
        filled_by=np.where(frc.fuel_cost_per_mmbtu.notna(), "original", pd.NA),
        fuel_mmbtu_total=lambda x: x.fuel_received_units * x.fuel_mmbtu_per_unit,
    )

    # Identify outlying fuel prices using modified z-score and set them to NA
    mod_zscore = frc.groupby(agg_mod_zscore).apply(
        weighted_modified_zscore, data="fuel_cost_per_mmbtu", weights="fuel_mmbtu_total"
    )
    mod_zscore.index = mod_zscore.index.droplevel(level=agg_mod_zscore)
    frc["mod_zscore"] = mod_zscore
    frc["outlier"] = np.where(frc["mod_zscore"] > max_mod_zscore, True, False)
    frc.loc[frc["outlier"], "fuel_cost_per_mmbtu"] = np.nan

    for agg in aggs:
        agg_cols = aggs[agg]["agg_cols"]
        fgc = aggs[agg]["fuel_group_eiaepm"]
        wm = frc.groupby(agg_cols).apply(
            weighted_median, data="fuel_cost_per_mmbtu", weights="fuel_mmbtu_total"
        )
        wm.name = agg
        frc = frc.merge(
            wm.to_frame().reset_index(), how="left", on=agg_cols, validate="many_to_one"
        )
        frc[agg + "_err"] = (
            frc[agg] - frc.fuel_cost_per_mmbtu
        ) / frc.fuel_cost_per_mmbtu
        mask = (
            # Only apply estimates to fuel prices that are still missing
            (frc.fuel_cost_per_mmbtu.isna())
            # Using records where the current aggregation has a value
            & (frc[agg].notna())
            # Selectively apply to a single fuel group, if specified:
            & (True if fgc == "all" else frc.fuel_group_eiaepm == fgc)
        )
        # Label that record with the aggregation used to fill it:
        frc.loc[mask, "filled_by"] = agg
        # Finally, fill in the value:
        frc.loc[mask, "fuel_cost_per_mmbtu"] = frc.loc[mask, agg]
        logger.info(
            f"Filled in {sum(mask)} missing fuel prices with {agg} "
            f"aggregation for fuel group {fgc}."
        )
    # Unless debugging, remove the columns used to fill missing fuel prices
    if not debug:
        cols_to_drop = list(aggs)
        cols_to_drop += list(c + "_err" for c in cols_to_drop)
        cols_to_drop += ["report_year", "census_region"]
        frc = frc.drop(columns=cols_to_drop)

    return frc
