"""PUDL data validation functions and test case specifications.

What defines a data validation?
  * What data are we checking?
    * What table or output does it come from?
    * What selection criteria do we apply to that table or output?
  * What are we checking it against?
    * Itself (helps validate that the tests themselves are working)
    * A processed version of itself (aggregation or derived values)
    * A hard-coded external standard (e.g. heat rates, fuel heat content)
"""

import numpy as np
import pandas as pd
from dagster import AssetCheckResult
from matplotlib import pyplot as plt

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def intersect_indexes(indexes: list[pd.Index]) -> pd.Index:
    """Calculate the intersection of a collection of pandas Indexes.

    Args:
        indexes: a list of pandas.Index objects

    Returns:
        The intersection of all values found in the input indexes.
    """
    shared_idx = indexes[0]
    for idx in indexes:
        shared_idx = shared_idx.intersection(idx, sort=None)
    return shared_idx


def check_date_freq(df1: pd.DataFrame, df2: pd.DataFrame, mult: int) -> None:
    """Verify an expected relationship between time frequencies of two dataframes.

    Identify all distinct values of ``report_date`` in each of the input
    dataframes and check that the number of distinct ``report_date`` values in
    ``df2`` is ``mult`` times the number of ``report_date`` values in ``df1``
    across only those years which appear in both dataframes. This is primarily
    aimed at comparing annual and monthly dataframes, but should
    also work with e.g. annual (df1) and quarterly (df2) frequency data using
    ``mult=4``.

    Note the function assumes that a dataframe with sub-annual frequency will
    cover the entire year it's part of. If you have a partial year of monthly
    data in one dataframe that overlaps with annual data in another dataframe
    you'll probably get unexpected behavior.

    We use this method rather than attempting to infer a frequency from the
    observed values because often we have only a single year of data, and you
    need at least 3 values in a DatetimeIndex to infer the frequency.

    Args:
        df1: A dataframe with a column named ``report_date`` which contains dates.
        df2: A dataframe with a column named ``report_date`` which contains dates.
        mult: A multiplicative factor indicating the expected ratio between the number
            of distinct date values found in ``df1`` and ``df2``.  E.g. if ``df1`` is
            annual and ``df2`` is monthly, ``mult`` should be 12.

    Returns:
        None

    Raises:
        AssertionError: if the number of distinct ``report_date`` values in
            ``df2`` is not ``mult`` times the number of distinct
            ``report_date`` values in ``df1``.
        ValueError: if either ``df1`` or ``df2`` does not have a
            column named ``report_date``
    """
    if ("report_date" not in df1.columns) or ("report_date" not in df2.columns):
        raise ValueError("Missing report_date column in one or both input DataFrames")

    # Remove ytd values that mess up ratio assumptions
    if "data_maturity" in df2:
        df2 = df2[df2["data_maturity"] != "incremental_ytd"].copy()

    idx1 = pd.DatetimeIndex(df1.report_date.unique())
    idx2 = pd.DatetimeIndex(df2.report_date.unique())

    overlap = intersect_indexes([idx1, idx2])
    overlap1 = [d for d in idx1 if d.year in overlap.year]
    overlap2 = [d for d in idx2 if d.year in overlap.year]

    n1 = len(overlap1)
    n2 = len(overlap2)
    if mult * n1 != n2:
        raise AssertionError(
            f"Expected ratio of distinct report_date values to be {mult}, "
            f"but found {n2} / {n1} = {n2 / n1}"
        )


def no_null_rows(df, cols="all", df_name="", thresh=0.9):
    """Check for rows filled with NA values indicating bad merges.

    Sum up the number of NA values in each row and the columns specified by
    ``cols``. If the NA values make up more than ``thresh`` of the columns
    overall, the row is considered Null and the check fails.

    Args:
        df (pandas.DataFrame): DataFrame to check for null rows.
        cols (iterable or "all"): The labels of columns to check for
            all-null values. If "all" check all columns.

    Returns:
        pandas.DataFrame: The input DataFrame, for use with DataFrame.pipe().

    Raises:
        ValueError: If the fraction of NA values in any row is greater than
        ``thresh``.
    """
    if cols == "all":
        cols = df.columns

    null_rows = df[cols].isna().sum(axis="columns") / len(cols) > thresh
    if null_rows.any():
        raise ValueError(
            f"Found {null_rows.sum(axis='rows')} null rows in {df_name}./n {df[null_rows]}"
        )

    return df


def no_null_cols(
    df: pd.DataFrame, cols: str = "all", df_name: str = ""
) -> pd.DataFrame:
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

    null_cols = [c for c in cols if c in df.columns and df[c].isna().all()]
    if null_cols:
        raise ValueError(f"Null columns found in {df_name}: {null_cols}")

    return df


def group_mean_continuity_check(
    df: pd.DataFrame,
    thresholds: dict[str, float],
    groupby_col: str,
    n_outliers_allowed: int = 0,
) -> AssetCheckResult:
    """Check that certain variables don't vary by too much.

    Groups and sorts the data by ``groupby_col``, then takes the mean across
    each group. Useful for saying something like "the average water usage of
    cooling systems didn't jump by 10x from 2012-2013."

    Args:
        df: the df with the actual data
        thresholds: a mapping from column names to the ratio by which those
            columns are allowed to fluctuate from one group to the next.
        groupby_col: the column by which we will group the data.
        n_outliers_allowed: how many data points are allowed to be above the
        threshold.
    """
    pct_change = (
        df.loc[:, [groupby_col] + list(thresholds.keys())]
        .groupby(groupby_col, sort=True)
        .mean()
        .pct_change()
        .abs()
        .dropna()
    )
    discontinuity = pct_change >= thresholds
    metadata = {
        col: {
            "top5": list(pct_change[col][discontinuity[col]].nlargest(n=5)),
            "threshold": thresholds[col],
        }
        for col in thresholds
        if discontinuity[col].sum() > 0
    }
    if (discontinuity.sum() > n_outliers_allowed).any():
        return AssetCheckResult(passed=False, metadata=metadata)

    return AssetCheckResult(passed=True, metadata=metadata)


def weighted_quantile(data: pd.Series, weights: pd.Series, quantile: float) -> float:
    """Calculate the weighted quantile of a Series or DataFrame column.

    This function allows us to take two columns from a :class:`pandas.DataFrame` one of
    which contains an observed value (data) like heat content per unit of fuel, and the
    other of which (weights) contains a quantity like quantity of fuel delivered which
    should be used to scale the importance of the observed value in an overall
    distribution, and calculate the values that the scaled distribution will have at
    various quantiles.

    Args:
        data: A series containing numeric data.
        weights: Weights to use in scaling the data. Must have the same length as data.
        quantile: A number between 0 and 1, representing the quantile at which we want
            to find the value of the weighted data.

    Returns:
        The value in the weighted data corresponding to the given quantile. If there are
        no values in the data, return :mod:`numpy.nan`.
    """
    if (quantile < 0) or (quantile > 1):
        raise ValueError("quantile must have a value between 0 and 1.")
    if len(data) != len(weights):
        raise ValueError("data and weights must have the same length")
    df = (
        pd.DataFrame({"data": data, "weights": weights})
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .sort_values(by="data")
    )
    Sn = df.weights.cumsum()  # noqa: N806
    # This conditional is necessary because sometimes new columns get
    # added to the EIA data, and so they won't show up in prior years.
    if len(Sn) > 0:
        Pn = (Sn - 0.5 * df.weights) / Sn.iloc[-1]  # noqa: N806
        return np.interp(quantile, Pn, df.data)

    return np.nan


def historical_distribution(
    df: pd.DataFrame, data_col: str, weight_col: str, quantile: float
) -> list[float]:
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
            weighted_quantile(
                df[df.report_year == year][data_col],
                df[df.report_year == year][weight_col],
                quantile,
            )
        ]
    # these values can be NaN, if there were no values in that column for some
    # years in the data:
    return [d for d in dist if not np.isnan(d)]


def bounds_histogram(
    df, data_col, weight_col, query, low_q, hi_q, low_bound, hi_bound, title=""
):
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

    plt.hist(
        df[data_col],
        weights=df[weight_col],
        range=(xmin, xmax),
        bins=50,
        color="black",
        label=data_col,
    )

    if low_bound:
        plt.axvline(
            low_bound, lw=3, ls="--", color="red", label=f"lower bound for {low_q:.0%}"
        )
        plt.axvline(
            weighted_quantile(df[data_col], df[weight_col], low_q),
            lw=3,
            color="red",
            label=f"actual {low_q:.0%}",
        )
    if hi_bound:
        plt.axvline(
            hi_bound, lw=3, ls="--", color="blue", label=f"upper bound for {hi_q:.0%}"
        )
        plt.axvline(
            weighted_quantile(df[data_col], df[weight_col], hi_q),
            lw=3,
            color="blue",
            label=f"actual {hi_q:.0%}",
        )

    plt.title(title)
    plt.xlabel(data_col)
    plt.ylabel(weight_col)
    plt.legend()
    plt.show()


def historical_histogram(
    orig_df,
    test_df,
    data_col,
    weight_col,
    query="",
    low_q=0.05,
    mid_q=0.5,
    hi_q=0.95,
    low_bound=None,
    hi_bound=None,
    title="",
):
    """Weighted histogram comparing distribution with historical subsamples."""
    if query != "":
        orig_df = orig_df.copy().query(query)

    if weight_col is None or weight_col == "":
        orig_df["ones"] = 1.0
        if test_df is not None:
            test_df["ones"] = 1.0
        weight_col = "ones"

    orig_df = orig_df[np.isfinite(orig_df[data_col]) & np.isfinite(orig_df[weight_col])]

    if test_df is not None:
        test_df = test_df.copy().query(query)
        test_df = test_df[
            np.isfinite(test_df[data_col]) & np.isfinite(test_df[weight_col])
        ]

    xmin = weighted_quantile(orig_df[data_col], orig_df[weight_col], 0.01)
    xmax = weighted_quantile(orig_df[data_col], orig_df[weight_col], 0.99)

    test_alpha = 1.0
    if test_df is not None:
        plt.hist(
            test_df[data_col],
            weights=test_df[weight_col],
            range=(xmin, xmax),
            bins=50,
            color="yellow",
            alpha=0.5,
            label="Test Distribution",
        )
        test_alpha = 0.5
    else:
        test_df = orig_df
    plt.hist(
        orig_df[data_col],
        weights=orig_df[weight_col],
        range=(xmin, xmax),
        bins=50,
        color="black",
        alpha=test_alpha,
        label="Original Distribution",
    )

    if low_q:
        low_range = historical_distribution(orig_df, data_col, weight_col, low_q)
        plt.axvspan(
            min(low_range),
            max(low_range),
            color="red",
            alpha=0.2,
            label=f"Historical range of {low_q:.0%}",
        )
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], low_q),
            color="red",
            label=f"Tested {low_q:.0%}",
        )

    if mid_q:
        mid_range = historical_distribution(orig_df, data_col, weight_col, mid_q)
        plt.axvspan(
            min(mid_range),
            max(mid_range),
            color="green",
            alpha=0.2,
            label=f"historical range of {mid_q:.0%}",
        )
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], mid_q),
            color="green",
            label=f"Tested {mid_q:.0%}",
        )

    if hi_q:
        high_range = historical_distribution(orig_df, data_col, weight_col, hi_q)
        plt.axvspan(
            min(high_range),
            max(high_range),
            color="blue",
            alpha=0.2,
            label=f"Historical range of {hi_q:.0%}",
        )
        plt.axvline(
            weighted_quantile(test_df[data_col], test_df[weight_col], hi_q),
            color="blue",
            label=f"Tested {hi_q:.0%}",
        )

    plt.title(title)
    plt.xlabel(data_col)
    plt.ylabel(weight_col)
    plt.legend()
    plt.show()


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


core_ferc1__yearly_steam_plants_sched402_self = [
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
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "mmbtu per unit (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "mmbtu per unit (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
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
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Cost per unit burned (Oil)",
        "query": "fuel_type_code_pudl=='oil'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Cost per unit burned (Gas)",
        "query": "fuel_type_code_pudl=='gas'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_cost_per_unit_burned",
        "weight_col": "fuel_consumed_units",
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


###############################################################################
# Validate bf_eia923 data against its historical self:
###############################################################################
bf_eia923_self = [
    {
        "title": "Bituminous coal ash content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.25,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Subbituminous coal ash content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite coal ash content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Bituminous coal heat content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.07,
        "mid_q": 0.5,
        "hi_q": 0.98,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Subbituminous coal heat content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.90,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Lignite heat content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_consumed_units",
    },
    {
        "title": "Diesel Fuel Oil heat content",
        "query": "energy_source_code=='DFO'",
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
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Subbituminous coal ash content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Lignite coal ash content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "ash_content_pct",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Bituminous coal heat content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.07,
        "mid_q": 0.5,
        "hi_q": 0.98,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Subbituminous coal heat content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.90,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Lignite heat content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Diesel Fuel Oil heat content",
        "query": "energy_source_code=='DFO'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "fuel_mmbtu_per_unit",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Bituminous coal moisture content",
        "query": "energy_source_code=='BIT'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Subbituminous coal moisture content",
        "query": "energy_source_code=='SUB'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 0.95,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_received_units",
    },
    {
        "title": "Lignite moisture content",
        "query": "energy_source_code=='LIG'",
        "low_q": 0.05,
        "mid_q": 0.5,
        "hi_q": 1.0,
        "data_col": "moisture_content_pct",
        "weight_col": "fuel_received_units",
    },
]
"""EIA923 fuel receipts & costs data validation against itself."""

###############################################################################
# MCOE output validations, against fixed bounds
###############################################################################

# Because of copious NA values, fuel costs are only useful at monthly
# resolution, and we really need rolling windows and a full time series for
# them to be most useful
mcoe_self_fuel_cost_per_mmbtu = [
    {  # EIA natural gas reporting really only becomes usable in 2015.
        "title": "Nautral Gas Fuel Costs (2015+)",
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
        "data_col": "unit_heat_rate_mmbtu_per_mwh",
        "weight_col": "net_generation_mwh",
    },
    {
        "title": "Coal Heat Rates",
        "query": "fuel_type_code_pudl=='coal'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "unit_heat_rate_mmbtu_per_mwh",
        "weight_col": "net_generation_mwh",
    },
]

###############################################################################
# EIA 860 output validation tests
###############################################################################

gens_eia860_self = [
    {
        "title": "All Capacity test...",
        "query": "ilevel_0 in ilevel_0",
        "low_q": 0.55,
        "mid_q": 0.70,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Nuclear Capacity test...",
        "query": "energy_source_code_1=='NUC'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "All Coal Capacity test...",
        "query": "energy_source_code_1=='BIT' or energy_source_code_1=='SUB' or energy_source_code_1=='LIG'",
        "low_q": 0.25,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Subbituminous and Lignite Coal Capacity test...",
        "query": "energy_source_code_1=='SUB' or energy_source_code_1=='LIG'",
        "low_q": 0.10,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Natural Gas Capacity test...",
        "query": "energy_source_code_1=='NG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "capacity_mw",
        "weight_col": "",
    },
    {
        "title": "Nameplate power factor",
        "query": "energy_source_code_1=='NG'",
        "low_q": 0.05,
        "mid_q": 0.50,
        "hi_q": 0.95,
        "data_col": "nameplate_power_factor",
        "weight_col": "",
    },
]

###############################################################################
# Naming issues...
###############################################################################
# Columns that don't conform to the naming conventions:
#  * fuel_type_code_pudl isn't a code -- should be just fuel_type_pudl
