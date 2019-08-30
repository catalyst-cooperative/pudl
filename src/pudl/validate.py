"""
PUDL data validation functions and test case specifications.

What defines a data validation?
  * What data are we checking?
    * What table or output does it come from?
    * What selection criteria do we apply to that table or output?
  * What are we checking it against?
    * Itself (helps validate that the tests themselves are working)
    * A processed version of itself (aggregation or derived values)
    * A hard-coded external standard (e.g. heat rates, fuel heat content)

"""
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def weighted_quantile(data, weights, quantile):
    """
    Calculate the weighted quantile of a Series or DataFrame column.

    This function allows us to take two columns from a :mod:`pandas.DataFrame`
    one of which contains an observed value (data) like heat content per unit
    of fuel, and the other of which (weights) contains a quantity like quantity
    of fuel delivered which should be used to scale the importance of the
    observed value in an overall distribution, and calculate the values that
    the scaled distribution will have at various quantiles.

    Args:
        data (:mod:`pandas.Series`): A series containing numeric data.
        weights (pandas.series): Weights to use in scaling the data. Must have
            the same length as data.
        quantile (float): A number between 0 and 1, representing the quantile
            at which we want to find the value of the weighted data.

    Returns:
        float: the value in the weighted data corresponding to the given
        quantile. If there are no values in the data, return :mod:`numpy.na`.

    """
    if ((quantile < 0) or (quantile > 1)):
        raise ValueError(
            f"quantile must have a value between 0 and 1.")
    if len(data) != len(weights):
        raise ValueError("data and weights must have the same length")
    df = (
        pd.DataFrame({"data": data, "weights": weights}).
        sort_values(by="data").
        dropna()
    )
    Sn = df.weights.cumsum()  # noqa: N806
    # This conditional is necessary because sometimes new columns get
    # added to the EIA data, and so they won't show up in prior years.
    if len(Sn) > 0:
        Pn = (Sn - 0.5 * df.weights) / Sn.iloc[-1]  # noqa: N806
        return np.interp(quantile, Pn, df.data)

    return np.nan


def historical_distribution(df, data_col, wt_col, quantile):
    """Calculate a historical distribution of weighted values of a column.

    In order to know what a "reasonable" value of a particular column is in the
    pudl data, we can use this function to see what the value in that column
    has been in each of the years of data we have on hand, and a given
    quantile. This population of values can then be used to set boundaries on
    acceptable data distributions in the aggregated and processed data.

    Args:
        df (pandas.DataFrame): a dataframe containing historical data, with a
            column named either ``report_date`` or ``report_year``.
        data_col (string): Label of the column containing the data of interest.
        wt_col (string): Label of the column containing the weights to be
            used in scaling the data.

    Returns:
        list: The weighted quantiles of data, for each of the years found in
        the historical data of df.

    """
    if "report_year" not in df.columns:
        df["report_year"] = pd.to_datetime(df.report_date).dt.year
    report_years = df.report_year.unique()
    dist = []
    for year in report_years:
        dist = dist + [weighted_quantile(df[df.report_year == year][data_col],
                                         df[df.report_year == year][wt_col],
                                         quantile)]
    # these values can be NaN, if there were no values in that column for some
    # years in the data:
    return [d for d in dist if not np.isnan(d)]


def vs_bounds(df, data_col, weight_col, query="", title="",
              low_q=False, low_bound=False, hi_q=False, hi_bound=False):
    """Test a distribution against an upper bound, lower bound, or both."""
    # These assignments allow 0.0 to be used as a bound...
    low_bool = False
    hi_bool = False
    if low_bound is not False:
        low_bool = True
    if hi_bound is not False:
        hi_bool = True
    if bool(low_q) ^ low_bool:
        raise ValueError(
            f"You must supply both a lower quantile and lower bound, "
            f"or neither. Got: low_q={low_q}, low_bound={low_bound}."
        )
    if bool(hi_q) ^ hi_bool:
        raise ValueError(
            f"You must supply both a lower quantile and lower bound, "
            f"or neither. Got: low_q={hi_q}, low_bound={hi_bound}."
        )

    if query != "":
        df = df.copy().query(query)
    if title != "":
        logger.info(title)
    if low_q and low_bool:
        low_test = weighted_quantile(df[data_col], df[weight_col], low_q)
        logger.info(f"{data_col} ({low_q:.0%}): "
                    f"{low_test:.6} >= {low_bound:.6}")
        if low_test < low_bound:
            raise ValueError(
                f"{low_q:.0%} quantile ({low_test}) "
                f"is below lower bound ({low_bound})."
            )
    if hi_q and hi_bool:
        hi_test = weighted_quantile(df[data_col], df[weight_col], hi_q)
        logger.info(f"{data_col} ({hi_q:.0%}): {hi_test:.6} <= {hi_bound:.6}")
        if weighted_quantile(df[data_col], df[weight_col], hi_q) > hi_bound:
            raise ValueError(
                f"{hi_q:.0%} quantile ({hi_test}) "
                f"is above upper bound ({hi_bound})."
            )


def vs_self(df, data_col, weight_col, query="", title="",
            low_q=0.05, mid_q=0.5, hi_q=0.95):
    """
    Test a distribution against its own historical range.

    This is a special case of the :mod:`pudl.validate.vs_historical` function,
    in which both the ``orig_df`` and ``test_df`` are the same. Mostly it
    helps ensure that the test itself is valid for the given distribution.

    """
    vs_historical(df, df, data_col, weight_col, query=query,
                  low_q=low_q, mid_q=mid_q, hi_q=hi_q,
                  title=title)


def vs_historical(orig_df, test_df, data_col, weight_col, query="",
                  low_q=0.05, mid_q=0.5, hi_q=0.95,
                  title=""):
    """Validate aggregated distributions against original data."""
    if query != "":
        orig_df = orig_df.copy().query(query)
        test_df = test_df.copy().query(query)
    if title != "":
        logger.info(title)
    if low_q:
        low_range = historical_distribution(
            orig_df, data_col, weight_col, low_q)
        low_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], low_q)
        logger.info(
            f"{data_col} ({low_q:.0%}): {low_test:.6} >= {min(low_range):.6}")
        if low_test < min(low_range):
            raise ValueError(f"{low_test} below lower limit {min(low_range)}.")

    if mid_q:
        mid_range = historical_distribution(
            orig_df, data_col, weight_col, mid_q)
        mid_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], mid_q)
        logger.info(
            f"{data_col} ({mid_q:.0%}): {min(mid_range):.6} <= {mid_test:.6} "
            f"<= {max(mid_range):.6}")
        if mid_test < min(mid_range):
            raise ValueError(f"{mid_test} below lower limit {min(mid_range)}.")
        if mid_test > max(mid_range):
            raise ValueError(f"{mid_test} above upper limit {max(mid_range)}.")

    if hi_q:
        hi_range = historical_distribution(
            orig_df, data_col, weight_col, hi_q)
        hi_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], hi_q)
        logger.info(
            f"{data_col} ({hi_q:.0%}): {hi_test:.6} <= {max(hi_range):.6}.")
        if hi_test > max(hi_range):
            raise ValueError(
                f"{hi_test} above upper limit {max(hi_range)}")

###############################################################################
###############################################################################
# Data Validation Test Cases:
# These need to be accessible both by to PyTest, and to the validation
# nnotebooks, so they are stored here where they can be imported from anywhere.
###############################################################################
###############################################################################


###############################################################################
# EIA923 Fuel Receipts and Costs validation against fixed values
###############################################################################
frc_eia923_coal_heat_content = [
    {
        "title": "Bituminous coal heat content (middle)",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.50,
        "low_bound": 20.5,
        "hi_q": 0.50,
        "hi_bound": 26.5,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Bituminous coal heat content (tails)",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "low_bound": 17.0,
        "hi_q": 0.95,
        "hi_bound": 30.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Sub-bituminous coal heat content (middle)",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.50,
        "low_bound": 16.5,
        "hi_q": 0.50,
        "hi_bound": 18.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Sub-bituminous coal heat content (tails)",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "low_bound": 15.0,
        "hi_q": 0.95,
        "hi_bound": 20.5,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite heat content (middle)",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.50,
        "low_bound": 12.0,
        "hi_q": 0.50,
        "hi_bound": 14.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite heat content (tails)",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 15.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "All coal heat content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 10.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Valid coal (bituminous, sub-bituminous, and lignite) heat content values.

Based on IEA coal grade definitions:
https://www.iea.org/statistics/resources/balancedefinitions/
"""

frc_eia923_oil_heat_content = [
    {
        "title": "Diesel Fuel Oil heat content (tails)",
        "query": "energy_source_code=='DFO'",
        "low_q": 0.05,
        "low_bound": 5.5,
        "hi_q": 0.95,
        "hi_bound": 6.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Diesel Fuel Oil heat content (middle)",
        "query": "energy_source_code=='DFO'",
        "low_q": 0.50,
        "low_bound": 5.75,
        "hi_q": 0.50,
        "hi_bound": 5.85,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "All petroleum heat content (tails)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 5.5,
        "hi_q": 0.95,
        "hi_bound": 6.5,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Valid petroleum based fuel heat content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""

frc_eia923_gas_heat_content = [
    {
        "title": "Natural Gas heat content (middle)",
        "query": "fuel_type_code_pudl=='gas'",
        "hi_q": 0.50,
        "hi_bound": 1.036,
        "low_q": 0.50,
        "low_bound": 1.018,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {  # This may fail because of bad data at 0.1 mmbtu/unit
        "title": "Natural Gas heat content (tails)",
        "query": "fuel_type_code_pudl=='gas'",
        "hi_q": 0.99,
        "hi_bound": 1.15,
        "low_q": 0.01,
        "low_bound": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Valid natural gas heat content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs. May
fail because of a population of bad data around 0.1 mmbtu/unit. This appears
to be an off-by-10x error, possibly due to reporting error in units used.
"""

frc_eia923_coal_ash_content = [
    {
        "title": "Bituminous coal ash content (middle)",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.50,
        "low_bound": 6.0,
        "hi_q": 0.50,
        "hi_bound": 15.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Sub-bituminous coal ash content (middle)",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.50,
        "low_bound": 4.5,
        "hi_q": 0.50,
        "hi_bound": 7.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite ash content (middle)",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.50,
        "low_bound": 7.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "All coal ash content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 4.0,
        "hi_q": 0.50,
        "hi_bound": 20.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
]
"""Valid coal ash content (%). Based on historical reporting in EIA 923."""

frc_eia923_coal_sulfur_content = [
    {
        "title": "Coal sulfur content (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "hi_q": 0.95,
        "hi_bound": 4.0,
        "low_q": 0.05,
        "low_bound": 0.15,
        "data_col": "sulfur_content_pct",
        "weight_col": "fuel_qty_units",
    },
]
"""
Valid petroleum based coal sulfur content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""

frc_eia923_coal_mercury_content = [
    {  # Based on USGS FS095-01 https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
        "title": "Coal mercury content (upper tail)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 1.0,
        "data_col": "mercury_content_ppm",
        "weight_col": "fuel_qty_units",
    },
    {  # Based on USGS FS095-01 https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
        "title": "Coal mercury content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 0.04,
        "hi_q": 0.50,
        "hi_bound": 0.19,
        "data_col": "mercury_content_ppm",
        "weight_col": "fuel_qty_units",
    },
]
"""
Valid coal mercury content limits.

Based on USGS FS095-01: https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
Upper tail may fail because of a population of extremely high mercury content
coal (9.0ppm) which is likely a reporting error.
"""

frc_eia923_coal_moisture_content = [
    {
        "title": "Bituminous coal moisture content (middle)",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.50,
        "low_bound": 5.0,
        "hi_q": 0.50,
        "hi_bound": 16.5,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Sub-bituminous coal moisture content (middle)",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.50,
        "low_bound": 15.0,
        "hi_q": 0.50,
        "hi_bound": 32.5,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite moisture content (middle)",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.50,
        "low_bound": 25.0,
        "hi_q": 0.50,
        "hi_bound": 45.0,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "All coal moisture content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 5.0,
        "hi_q": 0.50,
        "hi_bound": 40.0,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
]
"""Valid coal moisture content, based on historical EIA 923 reporting."""

###############################################################################
# Validate frc_eia923 data against its historical self:
###############################################################################
frc_eia923_self = [
    {
        "title": "Bituminous coal ash content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.25,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Subbituminous coal ash content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite coal ash content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Bituminous coal heat content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.07,
        "mid_q": 0.5,
        "hi_q": 0.98,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Subbituminous coal heat content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.90,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite heat content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Diesel Fuel Oil heat content",
        "query": "energy_source_code=='DFO'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Bituminous coal moisture content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Subbituminous coal moisture content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite moisture content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 1.0,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
]
"""EIA923 fuel receipts & costs data validation against itself."""

###############################################################################
# EIA 923 Fuel Receipts & Costs validations against aggregated historical data.
###############################################################################
frc_eia923_agg = [
    {
        "title": "Coal ash content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.2,
        "mid_q": 0.7,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {  # Only 1 year of data, mostly zero, a few big outliers. Not useful
        "title": "Coal chlorine content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "hi_q": False,
        "data_col": "chlorine_content_ppm",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Coal fuel costs",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {  # Coal sulfur content is one-sided. Needs an absolute test.
        "title": "Coal sulfur content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "hi_q": False,
        "data_col": "sulfur_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {  # Weird little population of ~5% at 1/10th correct heat content
        "title": "Gas heat content",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {  # Gas fuel costs are *extremely* variable.
        "title": "Gas fuel costs",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": False,
        "mid_q": 0.50,
        "hi_q": False,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Petroleum fuel cost",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": False,
        "mid_q": 0.50,
        "hi_q": False,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Petroleum heat content",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""EIA923 fuel receipts & costs data validation against aggregated data."""
