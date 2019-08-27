"""Data validation tools."""
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


def vs_historical(orig_df, test_df, query, data_col, weight_col,
                  low_q=0.05, mid_q=0.5, high_q=0.95,
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
        logger.info(f"{data_col}: {low_test:.6} >= {min(low_range):.6}")
        assert low_test >= min(low_range)

    if mid_q:
        mid_range = historical_distribution(
            orig_df, data_col, weight_col, mid_q)
        mid_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], mid_q)
        logger.info(
            f"{data_col}: {min(mid_range):.6} <= {mid_test:.6} <= {max(mid_range):.6}")
        assert mid_test >= min(mid_range)
        assert mid_test <= max(mid_range)

    if high_q:
        high_range = historical_distribution(
            orig_df, data_col, weight_col, high_q)
        high_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], high_q)
        logger.info(f"{data_col}: {high_test:.6} <= {max(high_range):.6}")
        assert high_test <= max(high_range)


abs_test_args = [
    {
        "title": "Bituminous coal ash content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.25,
        "high_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Subbituminous coal ash content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "high_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite coal ash content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "high_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Bituminous coal heat content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.07,
        "mid_q": 0.5,
        "high_q": 0.98,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Subbituminous coal heat content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "high_q": 0.90,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Lignite coal heat content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.5,
        "high_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]


agg_test_args = [
    {
        "title": "Coal ash content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.2,
        "mid_q": 0.7,
        "high_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {  # Only 1 year of data, mostly zero, a few big outliers. Not useful
        "title": "Coal chlorine content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "high_q": False,
        "data_col": "chlorine_content_ppm",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Coal fuel costs",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "high_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Coal heat content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.4,
        "high_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Coal mercury content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "high_q": 0.95,
        "data_col": "mercury_content_ppm",
        "weight_col": "fuel_qty_units",
    },
    {  # Only 1-2 years of data. Proxy for BIT/SUB/LIG. Not useful.
        "title": "Coal moisture content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "high_q": False,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {  # Coal sulfur content is one-sided. Needs an absolute test.
        "title": "Coal sulfur content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "high_q": False,
        "data_col": "sulfur_content_pct",
        "weight_col": "fuel_qty_units",
    },
    {  # Weird little population of ~5% at 1/10th correct heat content
        "title": "Gas heat content",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "high_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {  # Gas fuel costs are *extremely* variable.
        "title": "Gas fuel costs",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": False,
        "mid_q": 0.50,
        "high_q": False,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Petroleum fuel cost",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": False,
        "mid_q": 0.50,
        "high_q": False,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Petroleum heat content",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "high_q": 0.95,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
