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


def aggregate_price_median(
    frc: pd.DataFrame,
    aggs: OrderedDict[FuelPriceAgg] | None = None,
    debug: bool = False,
) -> pd.DataFrame:
    """Fill in missing fuel prices with median values using various aggregations.

    Aggregations are done across space (state, census region, country), time (month or
    year), and fuel groups (coal, petroleum, natural gas).

    Args:
        frc: a Fuel Receipts and Costs dataframe from EIA 923.
        aggs: Ordered sequence of fuel price aggregations to apply.
        debug: If True, retain intermediate columns used in the calculation for later
            inspection.

    Returns:
        A Fuel Receipts and Costs table that includes fuel price estimates for all
        missing records.
    """
    if aggs is None:
        aggs = FUEL_PRICE_AGGS
    logger.info("Filling in missing fuel prices using aggregated median values.")

    frc = frc.assign(
        report_year=lambda x: x.report_date.dt.year,
        census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
        fuel_cost_per_mmbtu=lambda x: x.fuel_cost_per_mmbtu.replace(0.0, np.nan),
        filled_by=np.where(frc.fuel_cost_per_mmbtu.notna(), "original", pd.NA),
    )

    for agg in aggs:
        agg_cols = aggs[agg]["agg_cols"]
        fgc = aggs[agg]["fuel_group_eiaepm"]
        frc[agg] = frc.groupby(agg_cols)["fuel_cost_per_mmbtu"].transform(
            "median"
        )  # could switch to weighted median to avoid right-skew
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
