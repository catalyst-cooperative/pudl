"""PUDL data validation functions and test case specifications.

Note that this module is being cannibalized and translated into dbt tests.
"""

import numpy as np
import pandas as pd
from dagster import AssetCheckResult
from matplotlib import pyplot as plt

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class ExcessiveNullRowsError(ValueError):
    """Exception raised when rows have excessive null values."""

    def __init__(self, message: str, null_rows: pd.DataFrame):
        """Initialize the ExcessiveNullRowsError with a message and DataFrame of null rows."""
        super().__init__(message)
        self.null_rows = null_rows


def no_null_rows(
    df: pd.DataFrame,
    cols="all",
    df_name: str = "",
    max_null_fraction: float = 0.9,
) -> pd.DataFrame:
    """Check for rows with excessive missing values, usually due to a merge gone wrong.

    Sum up the number of NA values in each row and the columns specified by ``cols``.
    If the NA values make up more than ``max_null_fraction`` of the columns overall, the
    row is considered Null and the check fails.

    Args:
        df: Table to check for null rows.
        cols: Columns to check for excessive null value. If "all" check all columns.
        df_name: Name of the dataframe, to aid in debugging/logging.
        max_null_fraction: The maximum fraction of NA values allowed in any row.

    Returns:
        The input DataFrame, for use with DataFrame.pipe().

    Raises:
        ExcessiveNullRowsError: If the fraction of NA values in any row is greater than
        ``max_null_fraction``.
    """
    if cols == "all":
        cols = df.columns

    null_rows = df[cols].isna().sum(axis="columns") / len(cols) > max_null_fraction
    if null_rows.any():
        raise ExcessiveNullRowsError(
            message=(
                f"Found {null_rows.sum(axis='rows')} excessively null rows in {df_name}.\n"
                f"{df[null_rows]}"
            ),
            null_rows=null_rows,
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
        # dbt weighted quantiles detour: the following group/sum operation is necessary to
        # match our weighted quantile definition in dbt, which treats repeated data values
        # as an n-way tie and pools the weights. see "Migrate vs_bounds" issue on github
        # for details:
        # https://github.com/catalyst-cooperative/pudl/issues/4106#issuecomment-2810774598
        .groupby("data")
        .sum()
        .reset_index()
        # /end dbt weighted quantiles detour
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
