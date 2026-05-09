"""Bespoke data quality checking utilities for PUDL data.

This module contains Python-implemented data quality checks that are either too
complex to express in SQL for dbt, or that need to be called from outside the Dagster
context. Add validation functions here when they are meant to be reused across multiple
asset checks, or when they don't cleanly apply to an individual asset.
"""

import numpy as np
import pandas as pd

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
    cols: list[str] | str = "all",
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
        cols = list(df.columns)

    null_rows = df[cols].isna().sum(axis="columns") / len(cols) > max_null_fraction
    if null_rows.any():
        raise ExcessiveNullRowsError(
            message=(
                f"Found {null_rows.sum()} excessively null rows in {df_name}.\n"
                f"{df[null_rows]}"
            ),
            null_rows=df[null_rows],
        )

    return df


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
