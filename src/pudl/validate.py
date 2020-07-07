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
import warnings

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

logger = logging.getLogger(__name__)


def no_null_cols(df, cols="all", df_name=""):
    """Check that a dataframe has no all-NaN columns.

    Occasionally in the concatenation / merging of dataframes we get a label
    wrong, and it results in a fully NaN column... which should probably never
    actually happen. This is a quick verification.

    Args:
        df (pandas.DataFrame): DataFrame to check for null columns.
        cols (iterable or "all"): The labels of columns to check for
            all-null values. If "all" check all columns.
        df_name (str): Name of the dataframe, to aid in debugging/logging.

    Returns:
        pandas.DataFrame: The same DataFrame as was passed in, for use in
            DataFrame.pipe().

    Raises:
        ValueError: If any completely NaN / Null valued columns are found.

    """
    if cols == "all":
        cols = df.columns

    for c in cols:
        if df[c].isna().all():
            raise ValueError(f"Null column: {c} found in dataframe {df_name}")

    return df


def check_max_rows(df, expected_rows=np.inf, margin=0.05, df_name=""):
    """Validate that a dataframe has less than a maximum number of rows."""
    len_df = len(df)
    max_rows = expected_rows * (1 + margin)
    if len_df > max_rows:
        raise ValueError(
            f"Too many records ({len_df}>{max_rows}) in dataframe {df_name}")
    logger.info(f"{df_name}: expected {expected_rows} rows, "
                f"found {len_df} rows.")

    return df


def check_min_rows(df, expected_rows=0, margin=0.05, df_name=""):
    """Validate that a dataframe has a certain minimum number of rows."""
    len_df = len(df)
    min_rows = expected_rows / (1 + margin)
    if len_df < min_rows:
        raise ValueError(
            f"Too few records ({len_df}<{min_rows}) in dataframe {df_name}")
    logger.info(f"{df_name}: expected {expected_rows} rows, "
                f"found {len_df} rows.")

    return df


def check_unique_rows(df, subset=None, df_name=""):
    """Test whether dataframe has unique records within a subset of columns.

    Args:
        df (pandas.DataFrame): DataFrame to check for duplicate records.
        subset (iterable or None): Columns to consider in checking for dupes.
        df_name (str): Name of the dataframe, to aid in debugging/logging.

    Returns:
        pandas.DataFrame: The same DataFrame as was passed in, for use in
            DataFrame.pipe().

    Raises:
        ValueError:  If there are duplicate records in the subset of selected
            columns.

    """
    n_dupes = len(df[df.duplicated(subset=subset)])
    if n_dupes != 0:
        raise ValueError(
            f"Found {n_dupes} dupes of in dataframe {df_name}")

    return df


def weighted_quantile(data, weights, quantile):
    """
    Calculate the weighted quantile of a Series or DataFrame column.

    This function allows us to take two columns from a
    :class:`pandas.DataFrame` one of which contains an observed value (data)
    like heat content per unit of fuel, and the other of which (weights)
    contains a quantity like quantity of fuel delivered which should be used to
    scale the importance of the observed value in an overall distribution, and
    calculate the values that the scaled distribution will have at various
    quantiles.

    Args:
        data (pandas.Series): A series containing numeric data.
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
            "quantile must have a value between 0 and 1.")
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


def historical_distribution(df, data_col, weight_col, quantile):
    """Calculate a historical distribution of weighted values of a column.

    In order to know what a "reasonable" value of a particular column is in the
    pudl data, we can use this function to see what the value in that column
    has been in each of the years of data we have on hand, and a given
    quantile. This population of values can then be used to set boundaries on
    acceptable data distributions in the aggregated and processed data.

    Args:
        df (pandas.DataFrame): a dataframe containing historical data, with a
            column named either ``report_date`` or ``report_year``.
        data_col (str): Label of the column containing the data of interest.
        weight_col (str): Label of the column containing the weights to be
            used in scaling the data.

    Returns:
        list: The weighted quantiles of data, for each of the years found in
        the historical data of df.

    """
    if "report_year" not in df.columns:
        df["report_year"] = pd.to_datetime(df.report_date).dt.year
    if weight_col is None or weight_col == "":
        df["ones"] = 1.0
        weight_col = "ones"
    report_years = df.report_year.unique()
    dist = []
    for year in report_years:
        dist = dist + [
            weighted_quantile(df[df.report_year == year][data_col],
                              df[df.report_year == year][weight_col],
                              quantile)
        ]
    # these values can be NaN, if there were no values in that column for some
    # years in the data:
    return [d for d in dist if not np.isnan(d)]


def vs_bounds(df, data_col, weight_col, query="", title="",
              low_q=False, low_bound=False, hi_q=False, hi_bound=False):
    """Test a distribution against an upper bound, lower bound, or both."""
    # These assignments allow 0.0 to be used as a bound...
    low_bool = low_bound is not False
    hi_bool = hi_bound is not False

    if bool(low_q) ^ low_bool and low_q != 0:
        raise ValueError(
            f"You must supply both a lower quantile and lower bound, "
            f"or neither. Got: low_q={low_q}, low_bound={low_bound} "
            f"for validation entitled {title}"
        )
    if bool(hi_q) ^ hi_bool:
        raise ValueError(
            f"You must supply both a lower quantile and lower bound, "
            f"or neither. Got: low_q={hi_q}, low_bound={hi_bound} "
            f"for validation entitled {title}"
        )

    if query != "":
        df = df.copy().query(query)
    if title != "":
        logger.info(title)
    if weight_col is None or weight_col == "":
        df["ones"] = 1.0
        weight_col = "ones"
    if low_q >= 0 and low_bool:
        low_test = weighted_quantile(df[data_col], df[weight_col], low_q)
        logger.info(f"{data_col} ({low_q:.0%}): "
                    f"{low_test:.6} >= {low_bound:.6}")
        if low_test < low_bound:
            raise ValueError(
                f"{low_q:.0%} quantile ({low_test}) "
                f"is below lower bound ({low_bound}) "
                f"in validation entitled {title}"
            )
    if hi_q <= 1 and hi_bool:
        hi_test = weighted_quantile(df[data_col], df[weight_col], hi_q)
        logger.info(f"{data_col} ({hi_q:.0%}): {hi_test:.6} <= {hi_bound:.6}")
        if weighted_quantile(df[data_col], df[weight_col], hi_q) > hi_bound:
            raise ValueError(
                f"{hi_q:.0%} quantile ({hi_test}) "
                f"is above upper bound ({hi_bound}) "
                f"in validation entitled {title}"
            )


def vs_self(df, data_col, weight_col, query="", title="",
            low_q=0.05, mid_q=0.5, hi_q=0.95):
    """
    Test a distribution against its own historical range.

    This is a special case of the :func:`pudl.validate.vs_historical` function,
    in which both the ``orig_df`` and ``test_df`` are the same. Mostly it
    helps ensure that the test itself is valid for the given distribution.

    """
    if weight_col is None or weight_col == "":
        df["ones"] = 1.0
        weight_col = "ones"
    vs_historical(df, df, data_col, weight_col, query=query,
                  low_q=low_q, mid_q=mid_q, hi_q=hi_q,
                  title=title)


def vs_historical(orig_df, test_df, data_col, weight_col, query="",  # noqa: C901
                  low_q=0.05, mid_q=0.5, hi_q=0.95,
                  title=""):
    """Validate aggregated distributions against original data."""
    if query != "":
        orig_df = orig_df.copy().query(query)
        test_df = test_df.copy().query(query)
    if title != "":
        logger.info(title)
    if weight_col is None or weight_col == "":
        orig_df["ones"] = 1.0
        test_df["ones"] = 1.0
        weight_col = "ones"
    if low_q:
        low_range = historical_distribution(
            orig_df, data_col, weight_col, low_q)
        low_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], low_q)
        logger.info(
            f"{data_col} ({low_q:.0%}): {low_test:.6} >= {min(low_range):.6}")
        if low_test < min(low_range):
            raise ValueError(
                f"Lower value {low_test} below lower limit {min(low_range)} "
                f"in validation of {data_col}"
            )

    if mid_q:
        mid_range = historical_distribution(
            orig_df, data_col, weight_col, mid_q)
        mid_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], mid_q)
        logger.info(
            f"{data_col} ({mid_q:.0%}): {min(mid_range):.6} <= {mid_test:.6} "
            f"<= {max(mid_range):.6}")
        if mid_test < min(mid_range):
            raise ValueError(
                f"Middle value {mid_test} below lower limit {min(mid_range)} "
                f"in validation of {data_col}"
            )
        if mid_test > max(mid_range):
            raise ValueError(
                f"Middle value {mid_test} above upper limit {max(mid_range)} "
                f"in validation of {data_col}"
            )

    if hi_q:
        hi_range = historical_distribution(
            orig_df, data_col, weight_col, hi_q)
        hi_test = weighted_quantile(
            test_df[data_col], test_df[weight_col], hi_q)
        logger.info(
            f"{data_col} ({hi_q:.0%}): {hi_test:.6} <= {max(hi_range):.6}.")
        if hi_test > max(hi_range):
            raise ValueError(
                f"Upper value {hi_test} above upper limit {max(hi_range)} "
                f"in validation of {data_col}"
            )


def bounds_histogram(df, data_col, weight_col, query,
                     low_q, hi_q, low_bound, hi_bound,
                     title=""):
    """Plot a weighted histogram showing acceptable bounds/actual values."""
    if query != "":
        df = df.copy().query(query)
    if weight_col is None or weight_col == "":
        df["ones"] = 1.0
        weight_col = "ones"
    # Non-finite values screw up the plot but not the test:
    df = df[np.isfinite(df[data_col]) & np.isfinite(df[weight_col])]

    xmin = weighted_quantile(df[data_col], df[weight_col], 0.01)
    xmax = weighted_quantile(df[data_col], df[weight_col], 0.99)

    plt.hist(df[data_col], weights=df[weight_col],
             range=(xmin, xmax), bins=50, color="black", label=data_col)

    if low_bound:
        plt.axvline(low_bound, lw=3, ls='--', color='red',
                    label=f"lower bound for {low_q:.0%}")
        plt.axvline(
            weighted_quantile(df[data_col], df[weight_col], low_q),
            lw=3, color="red", label=f"actual {low_q:.0%}")
    if hi_bound:
        plt.axvline(hi_bound, lw=3, ls='--', color='blue',
                    label=f"upper bound for {hi_q:.0%}")
        plt.axvline(weighted_quantile(df[data_col], df[weight_col], hi_q),
                    lw=3, color="blue", label=f"actual {hi_q:.0%}")

    plt.title(title)
    plt.xlabel(data_col)
    plt.ylabel(weight_col)
    plt.legend()
    plt.show()


def historical_histogram(orig_df, test_df, data_col, weight_col, query="",
                         low_q=0.05, mid_q=0.5, hi_q=0.95,
                         low_bound=None, hi_bound=None,
                         title=""):
    """Weighted histogram comparing distribution with historical subsamples."""
    if query != "":
        orig_df = orig_df.copy().query(query)

    if weight_col is None or weight_col == "":
        orig_df["ones"] = 1.0
        if test_df is not None:
            test_df["ones"] = 1.0
        weight_col = "ones"

    orig_df = orig_df[
        np.isfinite(orig_df[data_col]) &
        np.isfinite(orig_df[weight_col])
    ]

    if test_df is not None:
        test_df = test_df.copy().query(query)
        test_df = test_df[
            np.isfinite(test_df[data_col]) &
            np.isfinite(test_df[weight_col])
        ]

    xmin = weighted_quantile(orig_df[data_col], orig_df[weight_col], 0.01)
    xmax = weighted_quantile(orig_df[data_col], orig_df[weight_col], 0.99)

    test_alpha = 1.0
    if test_df is not None:
        plt.hist(test_df[data_col], weights=test_df[weight_col],
                 range=(xmin, xmax), bins=50, color="yellow", alpha=0.5,
                 label="Test Distribution")
        test_alpha = 0.5
    else:
        test_df = orig_df
    plt.hist(orig_df[data_col], weights=orig_df[weight_col],
             range=(xmin, xmax), bins=50, color="black", alpha=test_alpha,
             label="Original Distribution")

    if low_q:
        low_range = historical_distribution(
            orig_df, data_col, weight_col, low_q)
        plt.axvspan(min(low_range), max(low_range),
                    color="red", alpha=0.2,
                    label=f"Historical range of {low_q:.0%}")
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], low_q),
            color="red", label=f"Tested {low_q:.0%}")

    if mid_q:
        mid_range = historical_distribution(
            orig_df, data_col, weight_col, mid_q)
        plt.axvspan(min(mid_range), max(mid_range), color="green",
                    alpha=0.2, label=f"historical range of {mid_q:.0%}")
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], mid_q),
            color="green", label=f"Tested {mid_q:.0%}")

    if hi_q:
        high_range = historical_distribution(
            orig_df, data_col, weight_col, hi_q)
        plt.axvspan(min(high_range), max(high_range), color="blue",
                    alpha=0.2, label=f"Historical range of {hi_q:.0%}")
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], hi_q),
            color="blue", label=f"Tested {hi_q:.0%}")

    plt.title(title)
    plt.xlabel(data_col)
    plt.ylabel(weight_col)
    plt.legend()
    plt.show()


def plot_vs_bounds(df, validation_cases):
    """Run through a data validation based on absolute bounds."""
    for args in validation_cases:
        try:
            vs_bounds(df, **args)
        except ValueError as e:
            logger.error(str(e))
            warnings.warn("ERROR: Validation Failed")

        bounds_histogram(df, **args)


def plot_vs_self(df, validation_cases):
    """Validate a bunch of distributions against themselves."""
    for args in validation_cases:
        try:
            vs_self(df, **args)
        except ValueError as e:
            logger.error(str(e))
            warnings.warn("ERROR: Validation Failed")

        historical_histogram(df, test_df=None, **args)


def plot_vs_agg(orig_df, agg_df, validation_cases):
    """Validate a bunch of distributions against aggregated versions."""
    for args in validation_cases:
        try:
            vs_historical(orig_df, agg_df, **args)
        except ValueError as e:
            logger.error(str(e))
            warnings.warn("ERROR: Validation Failed")

        historical_histogram(orig_df, agg_df, **args)

###############################################################################
###############################################################################
# Data Validation Test Cases:
# These need to be accessible both by to PyTest, and to the validation
# nnotebooks, so they are stored here where they can be imported from anywhere.
###############################################################################
###############################################################################

###############################################################################
# FERC 1 Steam Plants
###############################################################################


plants_steam_ferc1_capacity = [
    {
        "title": "All Plant Capacity",
        "query": "",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 2000.0,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Steam Plant Capacity",
        "query": "plant_type=='steam'",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 2500.0,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
]

plants_steam_ferc1_expenses = [
    {
        "title": "Capital Expenses (median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 2e5,
        "hi_q": 0.5,
        "hi_bound": 6e5,
        "data_col": "capex_per_mw",
        "weight_col": "capacity_mw",
    },
    {
        "title": "Capital Expenses (upper tail)",
        "query": "",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 2.2e6,
        "data_col": "capex_per_mw",
        "weight_col": "capacity_mw",
    },
    {
        "title": "OpEx Fuel (median)",
        "query": "opex_fuel_per_mwh>0",
        "low_q": 0.5,
        "low_bound": 10.0,
        "hi_q": 0.5,
        "hi_bound": 25.0,
        "data_col": "opex_fuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "OpEx Fuel (upper tail)",
        "query": "opex_fuel_per_mwh>0",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 80.0,
        "data_col": "opex_fuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Nonfuel OpEx (central)",
        "query": "opex_nonfuel_per_mwh>0",
        "low_q": 0.3,
        "low_bound": 2.5,
        "hi_q": 0.3,
        "hi_bound": 5.0,
        "data_col": "opex_nonfuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Nonfuel OpEx (upper tail)",
        "query": "opex_nonfuel_per_mwh>0",
        "low_q": 0.05,
        "low_bound": 1.2,
        "hi_q": 0.95,
        "hi_bound": 25.0,
        "data_col": "opex_nonfuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
]

plants_steam_ferc1_capacity_ratios = [
    {
        "title": "Capacity Factor (Tails)",
        "query": "capacity_factor>0.05",
        "low_q": 0.5,
        "low_bound": 0.5,
        "hi_q": 0.95,
        "hi_bound": 0.9,
        "data_col": "capacity_factor",
        "weight_col": "",
    },
    {
        "title": "Capacity Factor (Central)",
        "query": "capacity_factor>0.05",
        "low_q": 0.7,
        "low_bound": 0.6,
        "hi_q": 0.7,
        "hi_bound": 0.8,
        "data_col": "capacity_factor",
        "weight_col": "",
    },
    {
        "title": "Peak Demand Ratio (median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 0.91,
        "hi_q": 0.5,
        "hi_bound": 1.0,
        "data_col": "peak_demand_ratio",
        "weight_col": "",
    },
    {
        "title": "Peak Demand Ratio (tails)",
        "query": "",
        "low_q": 0.05,
        "low_bound": 0.4,
        "hi_q": 0.95,
        "hi_bound": 1.3,
        "data_col": "peak_demand_ratio",
        "weight_col": "",
    },
    {
        "title": "Water Limited Ratio (median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 0.89,
        "hi_q": 0.5,
        "hi_bound": 0.95,
        "data_col": "water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Water Limited Ratio (tails)",
        "query": "",
        "low_q": 0.05,
        "low_bound": 0.63,
        "hi_q": 0.95,
        "hi_bound": 1.15,
        "data_col": "water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Not Water Limited Ratio (median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 0.92,
        "hi_q": 0.5,
        "hi_bound": 1.0,
        "data_col": "not_water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Not Water Limited Ratio (tails)",
        "query": "",
        "low_q": 0.05,
        "low_bound": 0.73,
        "hi_q": 0.95,
        "hi_bound": 1.2,
        "data_col": "not_water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Capability Ratio (median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 0.9,
        "hi_q": 0.5,
        "hi_bound": 0.98,
        "data_col": "capability_ratio",
        "weight_col": "",
    },
    {
        "title": "Capability Ratio (tails)",
        "query": "",
        "low_q": 0.05,
        "low_bound": 0.64,
        "hi_q": 0.95,
        "hi_bound": 1.18,
        "data_col": "capability_ratio",
        "weight_col": "",
    },
]

plants_steam_ferc1_connected_hours = [
    {  # Currently failing b/c ~10% of plants have way more than 8760 hours...
        "title": "Plant Hours Connected (min/max)",
        "query": "",
        "low_q": 0.0,
        "low_bound": 0.0,
        "hi_q": 1.0,
        "hi_bound": 8760.0,
        "data_col": "plant_hours_connected_while_generating",
        "weight_col": "capacity_mw",
    },
]

plants_steam_ferc1_self = [
    {
        "title": "All Plant Capacity",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Capacity Factor",
        "query": "capacity_factor>0.05",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "capacity_factor",
        "weight_col": "",
    },
    {
        "title": "OpEx per MWh",
        "query": "opex_per_mwh>0",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "opex_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Fuel OpEx per MWh",
        "query": "opex_fuel_per_mwh>0",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "opex_fuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Nonfuel OpEx per MWh",
        "query": "opex_nonfuel_per_mwh>0",
        "low_q": 0.05,
        "mid_q": 0.3,
        "hi_q": 0.95,
        "data_col": "opex_nonfuel_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "CapEx per MW",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "capex_per_mw",
        "weight_col": "capacity_mw",
    },
    {
        "title": "Installation Year",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "installation_year",
        "weight_col": "",
    },
    {
        "title": "Construction Year",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "construction_year",
        "weight_col": "",
    },
    {
        "title": "Water Limited Capacity ratio",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Not Water Limited Capacity ratio",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "not_water_limited_ratio",
        "weight_col": "",
    },
    {
        "title": "Capability Ratio",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "capability_ratio",
        "weight_col": "",
    },
    {
        "title": "Peak Demand Ratio",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "peak_demand_ratio",
        "weight_col": "",
    },
    {
        "title": "Plant Hours Connected",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.9,
        "data_col": "plant_hours_connected_while_generating",
        "weight_col": "capacity_mw",
    },
]

fuel_ferc1_self = [
    {
        "title": "mmbtu per unit (Coal)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
    {
        "title": "mmbtu per unit (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
    {
        "title": "mmbtu per unit (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
    {
        "title": "Cost per mmbtu (Coal)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
    {
        "title": "Cost per mmbtu (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
    {
        "title": "Cost per mmbtu (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
    {
        "title": "Cost per unit burned (Coal)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
    {
        "title": "Cost per unit burned (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
    {
        "title": "Cost per unit burned (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_coal_mmbtu_per_unit_bounds = [
    {
        "title": "Coal heat content (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "low_bound": 15.0,
        "hi_q": 0.95,
        "hi_bound": 26.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_oil_mmbtu_per_unit_bounds = [
    {
        "title": "mmbtu per unit (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 5.5,
        "hi_q": 0.95,
        "hi_bound": 6.8,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_gas_mmbtu_per_unit_bounds = [
    {
        "title": "mmbtu per unit (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "low_bound": 0.98,
        "hi_q": 0.95,
        "hi_bound": 1.08,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_coal_cost_per_mmbtu_bounds = [
    {
        "title": "Cost per mmbtu (Coal)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "low_bound": 0.75,
        "hi_q": 0.95,
        "hi_bound": 4.2,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
]
fuel_ferc1_oil_cost_per_mmbtu_bounds = [
    {
        "title": "Cost per mmbtu (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 5.0,
        "hi_q": 0.95,
        "hi_bound": 25.0,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
]
fuel_ferc1_gas_cost_per_mmbtu_bounds = [
    {
        "title": "Cost per mmbtu (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "low_bound": 1.8,
        "hi_q": 0.95,
        "hi_bound": 12.0,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "fuel_consumed_mmbtu",
    },
]
fuel_ferc1_coal_cost_per_unit_bounds = [
    {
        "title": "Cost per unit burned (Coal)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.10,
        "low_bound": 7.0,
        "hi_q": 0.95,
        "hi_bound": 100.0,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_oil_cost_per_unit_bounds = [
    {
        "title": "Cost per unit burned (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 25.0,
        "hi_q": 0.95,
        "hi_bound": 140.0,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
]
fuel_ferc1_gas_cost_per_unit_bounds = [
    {
        "title": "Cost per unit burned (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "low_bound": 2.0,
        "hi_q": 0.95,
        "hi_bound": 12.0,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_qty_burned",
    },
]

###############################################################################
# Fuel by Plant FERC 1
###############################################################################

fbp_ferc1_self = [
    {
        "title": "Cost per mmbtu (Gas)",
        "query": "",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.90,
        "data_col": "gas_cost_per_mmbtu",
        "weight_col": "",
    },
    {
        "title": "Cost per mmbtu (Oil)",
        "query": "",
        "low_q": 0.1,
        "mid_q": 0.5,
        "hi_q": 0.9,
        "data_col": "oil_cost_per_mmbtu",
        "weight_col": "",
    },
    {
        "title": "Cost per mmbtu (Coal)",
        "query": "",
        "low_q": 0.10,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "coal_cost_per_mmbtu",
        "weight_col": "",
    },
]

fbp_ferc1_gas_cost_per_mmbtu_bounds = [
    {
        "title": "Gas cost per MMBTU (Tails)",
        "query": "",
        "low_q": 0.05,
        "low_bound": 1.5,
        "hi_q": 0.90,
        "hi_bound": 15.0,
        "data_col": "gas_cost_per_mmbtu",
        "weight_col": "",
    },
    {
        "title": "Gas Cost per MMBTU (Median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 2.0,
        "hi_q": 0.5,
        "hi_bound": 10.0,
        "data_col": "gas_cost_per_mmbtu",
        "weight_col": "",
    },
]
fbp_ferc1_oil_cost_per_mmbtu_bounds = [
    {
        "title": "Oil cost per MMBTU (Tails)",
        "query": "",
        "low_q": 0.1,
        "low_bound": 4.0,
        "hi_q": 0.90,
        "hi_bound": 25.0,
        "data_col": "oil_cost_per_mmbtu",
        "weight_col": "",
    },
    {
        "title": "Oil Cost per MMBTU (Median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 7.0,
        "hi_q": 0.5,
        "hi_bound": 17.0,
        "data_col": "oil_cost_per_mmbtu",
        "weight_col": "",
    },
]
fbp_ferc1_coal_cost_per_mmbtu_bounds = [
    {
        "title": "Coal cost per MMBTU (Tails)",
        "query": "",
        "low_q": 0.1,
        "low_bound": 0.75,
        "hi_q": 0.95,
        "hi_bound": 4.5,
        "data_col": "coal_cost_per_mmbtu",
        "weight_col": "",
    },
    {
        "title": "Coal Cost per MMBTU (Median)",
        "query": "",
        "low_q": 0.5,
        "low_bound": 1.0,
        "hi_q": 0.5,
        "hi_bound": 2.5,
        "data_col": "coal_cost_per_mmbtu",
        "weight_col": "",
    },
]

###############################################################################
# EIA923 Generation Fuel data validation against fixed values
###############################################################################


gf_eia923_coal_heat_content = [
    {
        "title": "All coal heat content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 10.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid coal heat content values (all coal types).

The Generation Fuel table does not break different coal types out separately,
so we can only test the validity of the entire suite of coal records.

Based on IEA coal grade definitions:
https://www.iea.org/statistics/resources/balancedefinitions/
"""

gf_eia923_gas_heat_content = [
    {
        "title": "All gas heat content (middle)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.50,
        "low_bound": 0.975,
        "hi_q": 0.50,
        "hi_bound": 1.075,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "All gas heat content (middle)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.20,
        "low_bound": 0.95,
        "hi_q": 0.90,
        "hi_bound": 1.1,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid natural gas heat content values.

Focuses on natural gas proper. Lower bound excludes other types of gaseous
fuels intentionally.
"""

gf_eia923_oil_heat_content = [
    {
        "title": "Diesel Fuel Oil heat content (tails)",
        "query": "fuel_type_code_aer=='DFO'",
        "low_q": 0.05,
        "low_bound": 5.5,
        "hi_q": 0.95,
        "hi_bound": 6.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Diesel Fuel Oil heat content (middle)",
        "query": "fuel_type_code_aer=='DFO'",
        "low_q": 0.50,
        "low_bound": 5.75,
        "hi_q": 0.50,
        "hi_bound": 5.85,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "All petroleum heat content (tails)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 5.0,
        "hi_q": 0.95,
        "hi_bound": 6.5,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid petroleum based fuel heat content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""

###############################################################################
# EIA 923 Generation Fuel validations against aggregated historical data.
###############################################################################
gf_eia923_agg = [
    {
        "title": "Coal heat content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Petroleum heat content",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {  # Weird little population of ~5% at 1/10th correct heat content
        "title": "Gas heat content",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""EIA923 Boiler Fuel data validation against aggregated data."""


###############################################################################
# EIA923 Boiler Fuel data validation against fixed values
###############################################################################


bf_eia923_coal_heat_content = [
    {
        "title": "Bituminous coal heat content (middle)",
        "query": "fuel_type_code=='BIT'",
        "low_q": 0.50,
        "low_bound": 20.5,
        "hi_q": 0.50,
        "hi_bound": 26.5,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Bituminous coal heat content (tails)",
        "query": "fuel_type_code=='BIT'",
        "low_q": 0.05,
        "low_bound": 17.0,
        "hi_q": 0.95,
        "hi_bound": 30.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Sub-bituminous coal heat content (middle)",
        "query": "fuel_type_code=='SUB'",
        "low_q": 0.50,
        "low_bound": 16.5,
        "hi_q": 0.50,
        "hi_bound": 18.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Sub-bituminous coal heat content (tails)",
        "query": "fuel_type_code=='SUB'",
        "low_q": 0.05,
        "low_bound": 15.0,
        "hi_q": 0.95,
        "hi_bound": 20.5,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite heat content (middle)",
        "query": "fuel_type_code=='LIG'",
        "low_q": 0.50,
        "low_bound": 12.0,
        "hi_q": 0.50,
        "hi_bound": 14.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite heat content (tails)",
        "query": "fuel_type_code=='LIG'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 15.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "All coal heat content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 10.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid coal (bituminous, sub-bituminous, and lignite) heat content values.

Based on IEA coal grade definitions:
https://www.iea.org/statistics/resources/balancedefinitions/
"""

bf_eia923_oil_heat_content = [
    {
        "title": "Diesel Fuel Oil heat content (tails)",
        "query": "fuel_type_code=='DFO'",
        "low_q": 0.05,
        "low_bound": 5.5,
        "hi_q": 0.95,
        "hi_bound": 6.0,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Diesel Fuel Oil heat content (middle)",
        "query": "fuel_type_code=='DFO'",
        "low_q": 0.50,
        "low_bound": 5.75,
        "hi_q": 0.50,
        "hi_bound": 5.85,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "All petroleum heat content (tails)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "low_bound": 5.0,
        "hi_q": 0.95,
        "hi_bound": 6.5,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid petroleum based fuel heat content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""

bf_eia923_gas_heat_content = [
    {
        "title": "Natural Gas heat content (middle)",
        "query": "fuel_type_code_pudl=='gas'",
        "hi_q": 0.50,
        "hi_bound": 1.036,
        "low_q": 0.50,
        "low_bound": 1.018,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {  # This may fail because of bad data at 0.1 mmbtu/unit
        "title": "Natural Gas heat content (tails)",
        "query": "fuel_type_code_pudl=='gas'",
        "hi_q": 0.99,
        "hi_bound": 1.15,
        "low_q": 0.01,
        "low_bound": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid natural gas heat content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs. May
fail because of a population of bad data around 0.1 mmbtu/unit. This appears
to be an off-by-10x error, possibly due to reporting error in units used.
"""

bf_eia923_coal_ash_content = [
    {
        "title": "Bituminous coal ash content (middle)",
        "query": "fuel_type_code=='BIT'",
        "low_q": 0.50,
        "low_bound": 6.0,
        "hi_q": 0.50,
        "hi_bound": 15.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Sub-bituminous coal ash content (middle)",
        "query": "fuel_type_code=='SUB'",
        "low_q": 0.50,
        "low_bound": 4.5,
        "hi_q": 0.50,
        "hi_bound": 7.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite ash content (middle)",
        "query": "fuel_type_code=='LIG'",
        "low_q": 0.50,
        "low_bound": 7.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "All coal ash content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 4.0,
        "hi_q": 0.50,
        "hi_bound": 20.0,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
]
"""Valid coal ash content (%). Based on historical reporting in EIA 923."""

bf_eia923_coal_sulfur_content = [
    {
        "title": "Coal sulfur content (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "hi_q": 0.95,
        "hi_bound": 4.0,
        "low_q": 0.05,
        "low_bound": 0.15,
        "data_col": "sulfur_content_pct",
        "weight_col": "fuel_consumed_units",
    },
]
"""
Valid coal sulfur content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""
###############################################################################
# Validate bf_eia923 data against its historical self:
###############################################################################
bf_eia923_self = [
    {
        "title": "Bituminous coal ash content",
        "query": "fuel_type_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.25,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Subbituminous coal ash content",
        "query": "fuel_type_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite coal ash content",
        "query": "fuel_type_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Bituminous coal heat content",
        "query": "fuel_type_code=='BIT'",
        "low_q": 0.07,
        "mid_q": 0.5,
        "hi_q": 0.98,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Subbituminous coal heat content",
        "query": "fuel_type_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.90,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite heat content",
        "query": "fuel_type_code=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Diesel Fuel Oil heat content",
        "query": "fuel_type_code=='DFO'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""EIA923 Boiler Fuel data validation against itself."""

###############################################################################
# EIA 923 Boiler Fuel validations against aggregated historical data.
###############################################################################
bf_eia923_agg = [
    {
        "title": "Coal ash content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.2,
        "mid_q": 0.7,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {  # Coal sulfur content is one-sided. Needs an absolute test.
        "title": "Coal sulfur content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "mid_q": False,
        "hi_q": False,
        "data_col": "sulfur_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Coal heat content",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Petroleum heat content",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {  # Weird little population of ~5% at 1/10th correct heat content
        "title": "Gas heat content",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
]
"""EIA923 Boiler Fuel data validation against aggregated data."""

###############################################################################
# EIA923 Fuel Receipts and Costs validation against fixed values
###############################################################################

frc_eia923_coal_ant_heat_content = [
    {
        "title": "Anthracite coal heat content (middle)",
        "query": "energy_source_code=='ANT'",
        "low_q": 0.50,
        "low_bound": 20.5,
        "hi_q": 0.50,
        "hi_bound": 26.5,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
    {
        "title": "Anthracite coal heat content (tails)",
        "query": "energy_source_code=='ANT'",
        "low_q": 0.05,
        "low_bound": 22.0,
        "hi_q": 0.95,
        "hi_bound": 29.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable anthracite coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_coal_bit_heat_content = [
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
        "low_bound": 18.0,
        "hi_q": 0.95,
        "hi_bound": 29.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable bituminous coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_coal_sub_heat_content = [
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
        "hi_bound": 20.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable Sub-bituminous coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_coal_lig_heat_content = [
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
]
"""
Check for reasonable lignite coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_coal_cc_heat_content = [
    {
        "title": "Refined coal heat content (tails)",
        "query": "energy_source_code=='RC'",
        "low_q": 0.05,
        "low_bound": 6.5,
        "hi_q": 0.95,
        "hi_bound": 16.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable refined coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_coal_wc_heat_content = [
    {
        "title": "Waste coal heat content (tails)",
        "query": "energy_source_code=='WC'",
        "low_q": 0.05,
        "low_bound": 6.5,
        "hi_q": 0.95,
        "hi_bound": 16.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable waste coal heat content.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_oil_dfo_heat_content = [
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
]
"""
Check for reasonable diesel fuel oil heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_gas_sgc_heat_content = [
    {
        "title": "Coal syngas heat content (tails)",
        "query": "energy_source_code=='SGC'",
        "low_q": 0.05,
        "low_bound": 0.2,
        "hi_q": 0.95,
        "hi_bound": 0.3,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable coal syngas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_oil_jf_heat_content = [
    {
        "title": "Jet fuel heat content (tails)",
        "query": "energy_source_code=='JF'",
        "low_q": 0.05,
        "low_bound": 5.0,
        "hi_q": 0.95,
        "hi_bound": 6.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable jet fuel heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_oil_ker_heat_content = [
    {
        "title": "Kerosene heat content (tails)",
        "query": "energy_source_code=='KER'",
        "low_q": 0.05,
        "low_bound": 5.4,
        "hi_q": 0.95,
        "hi_bound": 6.1,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable kerosene heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_petcoke_heat_content = [
    {
        "title": "Petroleum coke heat content (tails)",
        "query": "energy_source_code=='PC'",
        "low_q": 0.05,
        "low_bound": 24.0,
        "hi_q": 0.95,
        "hi_bound": 30.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable petroleum coke heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_rfo_heat_content = [
    {
        "title": "Residual fuel oil heat content (tails)",
        "query": "energy_source_code=='RFO'",
        "low_q": 0.05,
        "low_bound": 5.7,
        "hi_q": 0.95,
        "hi_bound": 6.9,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable residual fuel oil heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_propane_heat_content = [
    {
        "title": "Propane heat content (tails)",
        "query": "energy_source_code=='PG'",
        "low_q": 0.05,
        "low_bound": 2.5,
        "hi_q": 0.95,
        "hi_bound": 2.75,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable propane heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_petcoke_syngas_heat_content = [
    {
        "title": "Petcoke syngas heat content (tails)",
        "query": "energy_source_code=='SGP'",
        "low_q": 0.05,
        "low_bound": 0.2,
        "hi_q": 0.95,
        "hi_bound": 1.1,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable petcoke syngas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_waste_oil_heat_content = [
    {
        "title": "Waste oil heat content (tails)",
        "query": "energy_source_code=='WO'",
        "low_q": 0.05,
        "low_bound": 3.0,
        "hi_q": 0.95,
        "hi_bound": 5.8,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable waste oil heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_blast_furnace_gas_heat_content = [
    {
        "title": "Blast furnace gas heat content (tails)",
        "query": "energy_source_code=='BFG'",
        "low_q": 0.05,
        "low_bound": 0.07,
        "hi_q": 0.95,
        "hi_bound": 0.12,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable blast furnace gas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_natural_gas_heat_content = [
    {
        "title": "Natural gas heat content (tails)",
        "query": "energy_source_code=='NG'",
        "low_q": 0.05,
        "low_bound": 0.8,
        "hi_q": 0.95,
        "hi_bound": 1.2,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable natural gas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_other_gas_heat_content = [
    {
        "title": "Other gas heat content (tails)",
        "query": "energy_source_code=='OG'",
        "low_q": 0.05,
        "low_bound": 0.07,
        "hi_q": 0.95,
        "hi_bound": 3.3,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable other gas heat contents.

Based on values given in the EIA 923 instructions, but with the lower bound
set by the expected lower bound of heat content on blast furnace gas (since
there were "other" gasses with bounds lower than the expected 0.32 in the data)
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_ag_byproduct_heat_content = [
    {
        "title": "Agricultural byproduct heat content (tails)",
        "query": "energy_source_code=='AB'",
        "low_q": 0.05,
        "low_bound": 7.0,
        "hi_q": 0.95,
        "hi_bound": 18.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable agricultural byproduct heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_muni_solids_heat_content = [
    {
        "title": "Municipal solid waste heat content (tails)",
        "query": "energy_source_code=='MSW'",
        "low_q": 0.05,
        "low_bound": 9.0,
        "hi_q": 0.95,
        "hi_bound": 12.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable municipal solid waste heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_biomass_solids_heat_content = [
    {
        "title": "Other biomass solids heat content (tails)",
        "query": "energy_source_code=='OBS'",
        "low_q": 0.05,
        "low_bound": 8.0,
        "hi_q": 0.95,
        "hi_bound": 25.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable other biomass solids heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_wood_solids_heat_content = [
    {
        "title": "Wood solids heat content (tails)",
        "query": "energy_source_code=='WDS'",
        "low_q": 0.05,
        "low_bound": 7.0,
        "hi_q": 0.95,
        "hi_bound": 18.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable wood solids heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_biomass_liquids_heat_content = [
    {
        "title": "Other biomass liquids heat content (tails)",
        "query": "energy_source_code=='OBL'",
        "low_q": 0.05,
        "low_bound": 3.5,
        "hi_q": 0.95,
        "hi_bound": 4.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable other biomass liquids heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_sludge_heat_content = [
    {
        "title": "Sludge waste heat content (tails)",
        "query": "energy_source_code=='SLW'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 16.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable sludget waste heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_black_liquor_heat_content = [
    {
        "title": "Black liquor heat content (tails)",
        "query": "energy_source_code=='BLQ'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 14.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable black liquor heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_wood_liquids_heat_content = [
    {
        "title": "Wood waste liquids heat content (tails)",
        "query": "energy_source_code=='WDL'",
        "low_q": 0.05,
        "low_bound": 8.0,
        "hi_q": 0.95,
        "hi_bound": 14.0,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable wood waste liquids heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_landfill_gas_heat_content = [
    {
        "title": "Landfill gas heat content (tails)",
        "query": "energy_source_code=='LFG'",
        "low_q": 0.05,
        "low_bound": 0.3,
        "hi_q": 0.95,
        "hi_bound": 0.6,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable landfill gas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
"""

frc_eia923_biomass_gas_heat_content = [
    {
        "title": "Other biomass gas heat content (tails)",
        "query": "energy_source_code=='OBG'",
        "low_q": 0.05,
        "low_bound": 0.36,
        "hi_q": 0.95,
        "hi_bound": 1.6,
        "data_col": "heat_content_mmbtu_per_unit",
        "weight_col": "fuel_qty_units",
    },
]
"""
Check for reasonable other biomass gas heat contents.

Based on values given in the EIA 923 instructions:
https://www.eia.gov/survey/form/eia_923/instructions.pdf
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
Valid coal sulfur content values.

Based on historically reported values in EIA 923 Fuel Receipts and Costs.
"""

frc_eia923_coal_mercury_content = [
    {  # Based on USGS FS095-01 https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
        "title": "Coal mercury content (upper tail)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": False,
        "low_bound": False,
        "hi_q": 0.95,
        "hi_bound": 0.125,
        "data_col": "mercury_content_ppm",
        "weight_col": "fuel_qty_units",
    },
    {  # Based on USGS FS095-01 https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
        "title": "Coal mercury content (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 0.00,
        "hi_q": 0.50,
        "hi_bound": 0.1,
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

###############################################################################
# MCOE output validations, against fixed bounds
###############################################################################
mcoe_gas_capacity_factor = [
    {
        "title": "Natural Gas Capacity Factor (middle, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01' and capacity_factor!=0.0",
        "low_q": 0.65,
        "low_bound": 0.40,
        "hi_q": 0.65,
        "hi_bound": 0.70,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
    {
        "title": "Natural Gas Capacity Factor (tails, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01' and capacity_factor!=0.0",
        "low_q": 0.15,
        "low_bound": 0.01,
        "hi_q": 0.95,
        "hi_bound": .95,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
]
"""Static constraints on natural gas generator capacity factors."""

mcoe_coal_capacity_factor = [
    {
        "title": "Coal Capacity Factor (middle)",
        "query": "fuel_type_code_pudl=='coal' and capacity_factor!=0.0",
        "low_q": 0.6,
        "low_bound": 0.5,
        "hi_q": 0.6,
        "hi_bound": 0.9,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
    {
        "title": "Coal Capacity Factor (tails)",
        "query": "fuel_type_code_pudl=='coal' and capacity_factor!=0.0",
        "low_q": 0.10,
        "low_bound": 0.04,
        "hi_q": 0.95,
        "hi_bound": .95,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
]
"""Static constraints on coal fired generator capacity factors."""

mcoe_gas_heat_rate = [
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Unit Heat Rates (middle, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.50,
        "low_bound": 7.0,
        "hi_q": 0.50,
        "hi_bound": 7.5,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Unit Heat Rates (tails, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "low_bound": 6.5,
        "hi_q": 0.95,
        "hi_bound": 13.0,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
]
"""Static constraints on gas fired generator heat rates."""

mcoe_coal_heat_rate = [
    {
        "title": "Coal Unit Heat Rates (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 10.0,
        "hi_q": 0.50,
        "hi_bound": 11.0,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Coal Unit Heat Rates (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "low_bound": 9.0,
        "hi_q": 0.95,
        "hi_bound": 12.5,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
]
"""Static constraints on coal fired generator heat rates."""

mcoe_fuel_cost_per_mwh = [
    {
        "title": "Coal Fuel Costs (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 18.0,
        "hi_q": 0.50,
        "hi_bound": 27.0,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Coal Fuel Costs (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 50.0,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Fuel Costs (middle, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.50,
        "low_bound": 20.0,
        "hi_q": 0.50,
        "hi_bound": 30.0,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Fuel Costs (tails, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "low_bound": 10.0,
        "hi_q": 0.95,
        "hi_bound": 50.0,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
]
"""Static constraints on fuel costs per MWh net generation."""

mcoe_fuel_cost_per_mmbtu = [
    {
        "title": "Coal Fuel Costs (middle)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.50,
        "low_bound": 1.5,
        "hi_q": 0.50,
        "hi_bound": 3.0,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
    {
        "title": "Coal Fuel Costs (tails)",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "low_bound": 1.2,
        "hi_q": 0.95,
        "hi_bound": 4.5,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Fuel Costs (middle, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.50,
        "low_bound": 2.0,
        "hi_q": 0.50,
        "hi_bound": 4.0,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Natural Gas Fuel Costs (tails, 2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "low_bound": 1.75,
        "hi_q": 0.95,
        "hi_bound": 6.0,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
]
"""Static constraints on fuel costs per mmbtu of fuel consumed."""

# Because of copious NA values, fuel costs are only useful at monthly
# resolution, and we really need rolling windows and a full time series for
# them to be most useful
mcoe_self_fuel_cost_per_mmbtu = [
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Nautral Gas Fuel Cost (2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
    {
        "title": "Coal Fuel Cost",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mmbtu",
        "weight_col": "total_mmbtu",
    },
]

mcoe_self_fuel_cost_per_mwh = [
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Nautral Gas Fuel Cost (2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Coal Fuel Cost",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_mwh",
        "weight_col": "net_generation_mwh",
    },
]

mcoe_self = [
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Nautral Gas Capacity Factor (2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.10,
        "mid_q": 0.65,
        "hi_q": 0.95,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
    {
        "title": "Coal Capacity Factor",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.60,
        "hi_q": 0.95,
        "data_col": "capacity_factor",
        "weight_col": "capacity_mw",
    },
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Nautral Gas Heat Rates (2015+)",
        "query": "fuel_type_code_pudl=='gas' and report_date>='2015-01-01'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Coal Heat Rates",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "heat_rate_mmbtu_mwh",
        "weight_col": "net_generation_mwh",
    },
]

###############################################################################
# EIA 860 output validation tests
###############################################################################

gens_eia860_vs_bound = [
    {
        "title": "Bituminous coal capacity (tails)",
        "query": "energy_source_code_1=='BIT'",
        "low_q": 0.45,
        "low_bound": 30.0,
        "hi_q": 0.90,
        "hi_bound": 700.0,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Subbituminous and Lignite Coal Capacity test...",
        "query": "energy_source_code_1=='SUB' or energy_source_code_1=='LIG'",
        "low_q": 0.35,
        "low_bound": 30.0,
        "hi_q": 0.90,
        "hi_bound": 800.0,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Natural Gas Capacity test",
        "query": "energy_source_code_1=='NG'",
        "low_q": 0.55,
        "low_bound": 30.0,
        "hi_q": 0.90,
        "hi_bound": 250.0,
        "data_col": "capacity_mw",
        "weight_col": "",
    }, ]

gens_eia860_self = [
    {
        "title": "All Capacity test...",
        "query": 'ilevel_0 in ilevel_0',
        "low_q": 0.55,
        "mid_q": 0.70,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": ""
    },
    {
        "title": "Nuclear Capacity test...",
        "query": "energy_source_code_1=='NUC'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": ""
    },
    {
        "title": "All Coal Capacity test...",
        "query": "energy_source_code_1=='BIT' or energy_source_code_1=='SUB' or energy_source_code_1=='LIG'",
        "low_q": 0.25,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": ""
    },
    {
        "title": "Subbituminous and Lignite Coal Capacity test...",
        "query": "energy_source_code_1=='SUB' or energy_source_code_1=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": ""
    },
    {
        "title": "Natural Gas Capacity test...",
        "query": "energy_source_code_1=='NG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": ""
    },
    {
        "title": "Nameplate power factor",
        "query": "energy_source_code_1=='NG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "nameplate_power_factor",
        "weight_col": ""}
]

###############################################################################
# Naming issues...
###############################################################################
# Differences between tables for *very* similar columns:
#  * fuel_type_code (BF) vs. energy_source_code (FRC)
#  * fuel_qty_units (FRC) vs. fuel_consumed_units (BF)
#  * fuel_mmbtu_per_unit (BF) vs. heat_content_mmbtu_per_unit (BF)
#
# Codes that could be expanded for readability:
#  * fuel_type_code (BF) => fuel_type
#  * energy_source_code (FRC) => energy_source
#
# Columns that don't conform to the naming conventions:
#  * fuel_type_code_pudl isn'ta code -- should be just fuel_type_pudl
