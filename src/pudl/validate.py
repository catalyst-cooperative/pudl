import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check


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
    """Check for rows with excessive missing values, usually due to a merge gone wrong."""
    if cols == "all":
        cols = df.columns

    null_rows = df[cols].isna().sum(axis="columns") / len(cols) > max_null_fraction
    if null_rows.any():
        raise ExcessiveNullRowsError(
            message=(
                f"Found {null_rows.sum(axis='rows')} excessively null rows in {df_name}.\n"
                f"{df[null_rows]}"
            ),
            null_rows=df[null_rows],
        )

    return df


@asset_check(asset="imputed_asset", name="no_null_rows_check", blocking=True)
def check_no_null_rows(imputed_df: pd.DataFrame) -> AssetCheckResult:
    """Check that no row in the DataFrame has more than 90% nulls."""
    max_null_fraction = 0.9
    cols = imputed_df.columns

    null_fraction = imputed_df[cols].isna().sum(axis=1) / len(cols)
    failing_rows = null_fraction > max_null_fraction

    if failing_rows.any():
        num_failing = failing_rows.sum()
        sample = imputed_df[failing_rows].head(3).to_dict(orient="records")

        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description=f"{num_failing} rows exceed {max_null_fraction:.0%} null threshold",
            metadata={
                "failing_rows_count": num_failing,
                "sample_failing_rows": sample,
                "max_null_fraction_threshold": max_null_fraction,
            },
        )

    return AssetCheckResult(
        passed=True,
        metadata={"max_null_fraction_threshold": max_null_fraction},
    )


def group_mean_continuity_check(
    df: pd.DataFrame,
    thresholds: dict[str, float],
    groupby_col: str,
    n_outliers_allowed: int = 0,
) -> AssetCheckResult:
    """Check that certain variables do not vary by too much across groups."""
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
    """Calculate the weighted quantile of a Series or DataFrame column."""
    if (quantile < 0) or (quantile > 1):
        raise ValueError("quantile must have a value between 0 and 1.")
    if len(data) != len(weights):
        raise ValueError("data and weights must have the same length")

    df = (
        pd.DataFrame({"data": data, "weights": weights})
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .groupby("data")
        .sum()
        .reset_index()
        .sort_values(by="data")
    )
    Sn = df.weights.cumsum()
    if len(Sn) > 0:
        Pn = (Sn - 0.5 * df.weights) / Sn.iloc[-1]
        return np.interp(quantile, Pn, df.data)
    return np.nan


def historical_distribution(
    df: pd.DataFrame, data_col: str, weight_col: str, quantile: float
) -> list[float]:
    """Calculate a historical distribution of weighted values of a column."""
    if "report_year" not in df.columns:
        df["report_year"] = pd.to_datetime(df.report_date).dt.year
    if not weight_col:
        df["ones"] = 1.0
        weight_col = "ones"

    report_years = df.report_year.unique()
    dist = []
    for year in report_years:
        dist.append(
            weighted_quantile(
                df[df.report_year == year][data_col],
                df[df.report_year == year][weight_col],
                quantile,
            )
        )
    return [d for d in dist if not np.isnan(d)]


def bounds_histogram(
    df,
    data_col,
    weight_col,
    query,
    low_q,
    hi_q,
    low_bound,
    hi_bound,
    title="",
):
    """Plot a weighted histogram showing acceptable bounds/actual values."""
    if query:
        df = df.copy().query(query)
    if not weight_col:
        df["ones"] = 1.0
        weight_col = "ones"

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
    if query:
        orig_df = orig_df.copy().query(query)
        test_df = test_df.copy().query(query)

    if not weight_col:
        orig_df["ones"] = 1.0
        test_df["ones"] = 1.0
        weight_col = "ones"

    orig_vals = historical_distribution(orig_df, data_col, weight_col, mid_q)
    xmin = min(min(orig_vals), test_df[data_col].min())
    xmax = max(max(orig_vals), test_df[data_col].max())

    plt.hist(
        test_df[data_col],
        weights=test_df[weight_col],
        range=(xmin, xmax),
        bins=50,
        color="black",
        label="current",
        alpha=0.6,
    )
    plt.hist(
        orig_vals,
        bins=50,
        range=(xmin, xmax),
        color="gray",
        alpha=0.5,
        label="historical",
    )
    if low_bound:
        plt.axvline(low_bound, lw=3, ls="--", color="red", label="lower bound")
    if hi_bound:
        plt.axvline(hi_bound, lw=3, ls="--", color="blue", label="upper bound")

    plt.title(title)
    plt.xlabel(data_col)
    plt.ylabel(weight_col)
    plt.legend()
    plt.show()
