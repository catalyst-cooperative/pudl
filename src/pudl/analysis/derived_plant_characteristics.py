"""Use EPA CEMS and EIA data to estimate generator operational characteristics."""

from typing import Any, Literal

import numpy as np
import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    Field,
    asset,
)

import pudl.logging_helpers
from pudl.metadata.enums import EPACEMS_STATES

logger = pudl.logging_helpers.get_logger(__name__)


HEAT_RATE_ANALYSIS_CONFIG_SCHEMA = {
    "num_years": Field(
        int,
        default_value=3,
        description=(
            "Number of historical EPA CEMS years to include, counting backward from "
            "the configured final year."
        ),
    ),
    "min_stable_consecutive_hours": Field(
        int,
        default_value=8,
        description=(
            "Minimum number of consecutive operating hours in a load-factor bin "
            "required for that bin to be considered a stable operating level."
        ),
    ),
    "load_factor_binning_method": Field(
        str,
        default_value="vectorized",
        description=(
            "Method used to assign hourly load-factor bins. 'pandas_cut' uses the "
            "original per-unit pandas groupby/pd.cut fallback "
            "(assign_groupwise_load_factor_bins); 'vectorized' uses a fully "
            "expression-based polars equivalent (assign_load_factor_bins_vectorized). "
            "Both are expected to produce equivalent output -- this flag exists "
            "to A/B test performance and correctness before retiring the pandas "
            "fallback. See notebooks/work-in-progress/sylvan-heat-rates.md."
        ),
    ),
    "ramp_rate_binning_method": Field(
        str,
        default_value="rank_split",
        description=(
            "Method used to summarize ramp-up/ramp-down rates. 'rank_split' takes "
            "the median of the top/bottom 5% of ramp rates by rank (fast, fully "
            "vectorized; _summarize_ramp_rates). 'qcut' reproduces the original "
            "script's pandas.qcut(q=20, duplicates='drop') per-unit quantile "
            "binning (_summarize_ramp_rates_qcut). The two methods diverge "
            "meaningfully whenever there are tied/duplicate ramp-rate values "
            "(the common case, since many hourly deltas are exactly zero) -- "
            "this flag exists to A/B test them before settling on one. See "
            "notebooks/work-in-progress/sylvan-heat-rates.md."
        ),
    ),
}


def _get_heat_rate_analysis_config(context) -> dict[str, Any]:
    """Extract heat rate analysis settings from Dagster asset config."""
    return {
        "num_years": context.op_config["num_years"],
        "min_stable_consecutive_hours": context.op_config[
            "min_stable_consecutive_hours"
        ],
        "load_factor_binning_method": context.op_config["load_factor_binning_method"],
        "ramp_rate_binning_method": context.op_config["ramp_rate_binning_method"],
    }


def filter_cems_for_heat_rate_analysis(
    core_epacems__hourly_emissions: pl.LazyFrame,
    final_year: int,
    num_years: int,
    states: list[str] | None = None,
) -> pl.LazyFrame:
    """Filter hourly EPA CEMS records to the configured analysis window.

    Args:
        core_epacems__hourly_emissions: Hourly CEMS emissions and gross load data.
        final_year: Final EPA CEMS year to include in the analysis.
        num_years: Number of historical years to include, counting backward from
            ``final_year``.
        states: Optional list of two-letter state abbreviations to include.
            Default is None, which will grab all states.

    Returns:
        Hourly EPA CEMS records filtered to the requested years and states.
    """
    start_year = final_year - num_years
    cems_columns = [
        "plant_id_eia",
        "plant_id_epa",
        "emissions_unit_id_epa",
        "operating_datetime_utc",
        "year",
        "state",
        "operating_time_hours",
        "gross_load_mw",
        "heat_content_mmbtu",
    ]

    # Apply filters
    cems_lf = core_epacems__hourly_emissions.select(cems_columns).filter(
        pl.col("year").is_between(start_year, final_year, closed="both")
    )
    if states:
        cems_lf = cems_lf.filter(pl.col("state").is_in(states))
    return cems_lf


def consecutive_run_ids(datetime_col: str = "operating_datetime_utc") -> pl.Expr:
    """Identify consecutive hourly runs within a datetime column."""
    return pl.col(datetime_col).diff().dt.total_hours().ne(1).fill_null(True).cum_sum()


def min_up_down_times(
    hourly_plant_unit: pd.DataFrame,
    min_stable_level_bin,  # TODO: What type is this?: pd.Interval,
    load_factor_col: str,
    load_factor_bin_col: str,
) -> tuple[float, float, Any, Any]:
    """Estimate minimum up and down times from hourly plant-unit observations."""
    if min_stable_level_bin is None:
        return np.nan, np.nan, None, None

    df = hourly_plant_unit.sort("operating_datetime_utc")

    # Get minimum up times.
    up = df.filter(pl.col(load_factor_bin_col) >= min_stable_level_bin).with_columns(
        consecutive_run_ids().alias("run_id")
    )

    if not up.limit(1).collect().is_empty():
        up_runs = up.group_by("run_id").agg(
            [
                pl.len().alias("run_size"),
                pl.col("operating_datetime_utc").min().alias("start_time"),
            ]
        )

        min_up = up_runs.sort("run_size").head(1)

        min_up_time = min_up["run_size"][0]
        min_up_datetime_utc = min_up["start_time"][0]
    else:
        min_up_time, min_up_datetime_utc = np.nan, None

    # Get minimum down times.
    down = df.filter(pl.col(load_factor_col).is_null()).with_columns(
        consecutive_run_ids().alias("run_id")
    )

    if down.height > 0:
        down_runs = down.group_by("run_id").agg(
            [
                pl.len().alias("run_size"),
                pl.col("operating_datetime_utc").min().alias("start_time"),
            ]
        )

        min_down = down_runs.sort("run_size").head(1)

        min_down_time = min_down["run_size"][0]
        min_down_datetime_utc = min_down["start_time"][0]
    else:
        min_down_time, min_down_datetime_utc = np.nan, None

    return min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc


def heat_rate(
    hourly_plant_unit: pl.LazyFrame,
    min_stable_level_bin,  # pd.Interval,
    load_factor_col: str,
    load_factor_bin_col: str,
    heat_rate_col: str,
) -> tuple[float, float]:
    """Estimate median heat rates at maximum load and minimum stable load."""
    heat_rates = hourly_plant_unit.filter(
        pl.col(load_factor_col).is_not_null() & pl.col(heat_rate_col).is_not_null()
    )

    if heat_rates.limit(1).collect().is_empty() or min_stable_level_bin is None:
        return np.nan, np.nan

    max_load_factor_bin = heat_rates.select(pl.col(load_factor_bin_col).max()).item()

    max_load_heat_rate = (
        heat_rates.filter(pl.col(load_factor_bin_col) == max_load_factor_bin)
        .select(pl.col(heat_rate_col).median())
        .item()
    )

    min_stable_heat_rate = (
        heat_rates.filter(pl.col(load_factor_bin_col) == min_stable_level_bin)
        .select(pl.col(heat_rate_col).median())
        .item()
    )

    return max_load_heat_rate, min_stable_heat_rate


def _add_run_id(
    lf: pl.LazyFrame,
    unit_cols: list[str],
    state_col: str | None = None,
) -> pl.Series:
    """Generate run IDs for consecutive hourly observations within each unit."""
    same_unit_and_bin = lf.select(
        result=pl.all_horizontal([pl.col(c).eq(pl.col(c).shift()) for c in unit_cols])
    ).to_series()
    consecutive_hour = lf.select(
        pl.col("operating_datetime_utc").diff().dt.total_seconds().truediv(3600).eq(1)
    ).to_series()

    same_state = lf.select(
        True if state_col is None else pl.col(state_col).eq(pl.col(state_col).shift())
    ).to_series()
    # The first row of the frame has no previous row to shift/diff against, so
    # same_unit_and_bin and consecutive_hour are both null there, which would
    # otherwise propagate as null through cum_sum() instead of starting run 0 --
    # fill_null(True) says "the first row always starts a new run," mirroring
    # consecutive_run_ids()'s .ne(1).fill_null(True) above.
    return (
        (~(same_unit_and_bin & consecutive_hour & same_state)).fill_null(True).cum_sum()
    )


def assign_groupwise_load_factor_bins(
    cems_working: pl.LazyFrame,
    unit_cols: list[str],
    load_factor_col: str,
):
    """Exact per-unit pd.cut(bins=10) using LazyFrame + minimal pandas fallback."""
    # compute group load factor
    stats = (
        cems_working.sort(unit_cols + ["operating_datetime_utc"])
        .group_by(unit_cols)
        .agg(
            # drop nulls before n_unique-ing so we don't get a bunch of null valid stats
            pl.col(load_factor_col).drop_nulls().n_unique().alias("load_factor_nunique")
        )
    )
    cems_working = cems_working.join(stats, on=unit_cols, how="left")

    valid = cems_working.filter(pl.col("load_factor_nunique") > 1)
    invalid = cems_working.filter(pl.col("load_factor_nunique") <= 1)

    # use pd.cut on the valid rows
    valid_pd = valid.collect().to_pandas(use_pyarrow_extension_array=True)
    valid_pd["load_factor_bin"] = valid_pd.groupby(unit_cols, group_keys=False)[
        load_factor_col
    ].apply(
        lambda s: pd.cut(
            s,
            bins=10,
            right=True,
            include_lowest=False,
        )
    )
    valid_pl = pl.from_pandas(valid_pd)
    valid_pl = valid_pl.with_columns(
        pl.col("state").cast(pl.Categorical),
        pl.col("load_factor_bin").cast(
            pl.Struct({"left": pl.Float64, "right": pl.Float64})
        ),
    )

    # recombine with rest of rows
    combined = pl.concat(
        [
            valid_pl,
            invalid.with_columns(
                pl.lit(None)
                .cast(valid_pl["load_factor_bin"].dtype)
                .alias("load_factor_bin")
            ).collect(),
        ]
    )

    # Rank bins for each set of unit_cols
    result = combined.with_columns(
        pl.col("load_factor_bin").struct[0].alias("load_factor_bin_left"),
        pl.col("load_factor_bin").struct[1].alias("load_factor_bin_right"),
    ).with_columns(
        pl.col("load_factor_bin_left")
        .rank(method="dense")
        .over(unit_cols)
        .alias("load_factor_bin_ordinal")
    )

    return result


def assign_load_factor_bins_vectorized(
    cems_working: pl.LazyFrame,
    unit_cols: list[str],
    load_factor_col: str,
) -> pl.DataFrame:
    """Fully vectorized, per-unit equal-width load-factor binning.

    Alternate implementation of :func:`assign_groupwise_load_factor_bins` that
    replaces its per-unit ``pandas`` ``groupby().apply(pd.cut)`` fallback with
    polars window expressions, so no per-unit Python loop is required to compute
    the bin edges themselves. It reproduces
    ``pandas.cut(bins=10, right=True, include_lowest=False)`` computed
    independently per unit: 10 equal-width bins spanning that unit's own observed
    min/max ``load_factor_col`` (``width = (max - min) / 10``), except that only
    the *lowest* bin's left edge is padded by 0.1% of the range (or by 0.001 when
    the range is zero) so that the unit's minimum observation falls inside the
    first (right-closed) bin rather than outside every bin -- matching pandas'
    ``_bins_to_cuts`` behavior of shifting only ``bins[0]``, not redistributing
    the padding across all ten bins.

    Returns the same output schema as :func:`assign_groupwise_load_factor_bins`
    (including the ``load_factor_bin`` struct and dense-ranked
    ``load_factor_bin_ordinal``) so it's a drop-in replacement for callers.
    This is intended for A/B comparison against the original implementation
    before retiring the pandas fallback -- see
    ``notebooks/work-in-progress/sylvan-heat-rates.md`` and
    ``tests/unit/analysis/derived_plant_characteristics_test.py`` for the
    empirical basis of that comparison.
    """
    stats = cems_working.group_by(unit_cols).agg(
        pl.col(load_factor_col).drop_nulls().n_unique().alias("load_factor_nunique")
    )
    cems_working = cems_working.join(stats, on=unit_cols, how="left")

    lo = pl.col(load_factor_col).min().over(unit_cols)
    hi = pl.col(load_factor_col).max().over(unit_cols)
    span = hi - lo
    pad = pl.when(span == 0).then(0.001).otherwise(span * 0.001)
    width = pl.when(span == 0).then(0.002 / 10).otherwise(span / 10)
    # Round before ceil-ing: a value that lands exactly on a bin edge (e.g.
    # x == lo + 3*width) can come out as 2.9999999999996 or 3.0000000000004
    # depending on float rounding in the division, which would otherwise push it
    # into the wrong bin.
    bin_idx = (((pl.col(load_factor_col) - lo) / width).round(9)).ceil().clip(1, 10)

    eligible = (pl.col("load_factor_nunique") > 1) & pl.col(
        load_factor_col
    ).is_not_null()
    # Only the lowest bin's left edge is padded -- the other nine bin widths are
    # exactly `width`, matching pandas' behavior of shifting only `bins[0]`.
    bin_left = (
        pl.when(bin_idx == 1).then(lo - pad).otherwise(lo + (bin_idx - 1) * width)
    )
    bin_right = lo + bin_idx * width

    result = cems_working.with_columns(
        pl.when(eligible).then(bin_left).alias("load_factor_bin_left"),
        pl.when(eligible).then(bin_right).alias("load_factor_bin_right"),
        pl.col("state").cast(pl.Categorical),
    ).with_columns(
        pl.when(eligible)
        .then(
            pl.struct(
                left=pl.col("load_factor_bin_left"),
                right=pl.col("load_factor_bin_right"),
            )
        )
        .alias("load_factor_bin")
    )

    # Rank bins for each set of unit_cols, exactly as assign_groupwise_load_factor_bins
    # does, so downstream ordinal-based logic (skip lowest bin, find max bin, etc.)
    # behaves identically regardless of which binning method produced the edges.
    result = result.with_columns(
        pl.col("load_factor_bin_left")
        .rank(method="dense")
        .over(unit_cols)
        .alias("load_factor_bin_ordinal")
    )

    return result.collect()


def _summarize_ramp_rates(
    cems_with_stable_bins: pl.LazyFrame,
    unit_cols: list[str],
    generation_col: str,
) -> pl.LazyFrame:
    """Summarize exact per-unit ramp rates using the original qcut approach."""
    ramp_input = (
        # TODO: add in this line to remove startup time.
        # filter_for_min_stable_bin(cems_with_stable_bins)
        cems_with_stable_bins.sort(
            unit_cols + ["operating_datetime_utc"]
        )  # Ensure proper diff order
        .with_columns(
            (
                pl.col("operating_datetime_utc")
                .diff()
                .over(unit_cols)  # Take the diff for each group of unit cols
                .dt.total_seconds(fractional=True)
                / (3600)
            ).alias("time_delta"),
            pl.col(generation_col).diff().over(unit_cols).alias("generation_delta"),
        )
        .with_columns(
            (pl.col("generation_delta") / pl.col("time_delta")).alias("ramp_rate")
        )
        .with_columns(ramp_rate=(pl.col("generation_delta") / pl.col("time_delta")))
        .with_columns(pl.col("ramp_rate").replace([float("inf"), float("-inf")], None))
        .drop_nulls("ramp_rate")
    )
    bin_expression = (pl.len() / 20).cast(pl.Int64)
    return (
        ramp_input.group_by(unit_cols)
        .having(pl.len() >= 20)
        .agg(
            # ramp_down_rate: median of the bottom 5% of ramp_rate
            pl.col("ramp_rate")
            .sort()
            .head(bin_expression)
            .median()
            .alias("ramp_down_rate"),
            # ramp_up_rate: median of the top 5% of ramp_rate
            pl.col("ramp_rate")
            .sort()
            .tail(bin_expression)
            .median()
            .alias("ramp_up_rate"),
        )
        .cast({"ramp_up_rate": pl.Float64, "ramp_down_rate": pl.Float64})
    )


def _summarize_ramp_rates_qcut(
    cems_with_stable_bins: pl.DataFrame | pl.LazyFrame,
    unit_cols: list[str],
    generation_col: str,
) -> pl.DataFrame:
    """Summarize per-unit ramp rates using the original script's qcut approach.

    Reimplementation of the original pandas script's ``pandas.qcut(ramp_rate, q=20,
    duplicates="drop")`` per-unit binning, provided as an A/B comparison against the
    vectorized rank-based :func:`_summarize_ramp_rates`. ``duplicates="drop"`` collapses
    adjacent quantile edges that land on the same value -- common here, since a large
    share of hourly ramp rates are exactly zero -- so the two methods can diverge
    meaningfully. This function requires a per-unit pandas ``groupby`` loop and is not
    intended as a long-term implementation; it exists to empirically characterize that
    divergence before choosing (and vectorizing) a final methodology. See
    ``notebooks/work-in-progress/sylvan-heat-rates.md``.
    """
    ramp_input = (
        cems_with_stable_bins.lazy()
        .sort(unit_cols + ["operating_datetime_utc"])
        .with_columns(
            (
                pl.col("operating_datetime_utc")
                .diff()
                .over(unit_cols)
                .dt.total_seconds(fractional=True)
                / 3600
            ).alias("time_delta"),
            pl.col(generation_col).diff().over(unit_cols).alias("generation_delta"),
        )
        .with_columns(ramp_rate=(pl.col("generation_delta") / pl.col("time_delta")))
        .with_columns(pl.col("ramp_rate").replace([float("inf"), float("-inf")], None))
        .drop_nulls("ramp_rate")
        .select(unit_cols + ["ramp_rate"])
        .collect()
    )

    ramp_pd = ramp_input.to_pandas()

    records = []
    for keys, group in ramp_pd.groupby(unit_cols, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        ramp_rate = group["ramp_rate"]
        try:
            bins = pd.qcut(ramp_rate, q=20, duplicates="drop")
        except ValueError:
            # Fewer than 2 distinct values -- qcut can't form any bins.
            ramp_down_rate = ramp_up_rate = np.nan
        else:
            low_bin, high_bin = bins.min(), bins.max()
            ramp_down_rate = ramp_rate[bins == low_bin].median()
            ramp_up_rate = ramp_rate[bins == high_bin].median()
        records.append(
            dict(zip(unit_cols, keys, strict=True))
            | {"ramp_down_rate": ramp_down_rate, "ramp_up_rate": ramp_up_rate}
        )

    summary_pd = pd.DataFrame.from_records(
        records, columns=[*unit_cols, "ramp_down_rate", "ramp_up_rate"]
    )
    return pl.from_pandas(summary_pd).cast(
        {"ramp_up_rate": pl.Float64, "ramp_down_rate": pl.Float64}
    )


def handle_adjustment_in_cems(
    cems: pl.LazyFrame, unit_cols: list[str], adjusted: bool = False
) -> (pl.LazyFrame, dict):
    """Filter CEMS data, computing derived columns if not adjusted."""
    cems_working = cems

    if adjusted:
        load_factor_col = "load_factor_adjusted_cems"
        generation_col = "net_generation_mwh_cems"
        heat_rate_col = "heat_rate_net_generation_cems"
        max_load_col = "max_cap_mw"
        output_max_load_col = "max_cap_mw"
        output_ramp_rate_col_suffix = "max_cap_mw"

    else:
        # Calculate max gross load and derived columns
        max_gross_load = cems.group_by(unit_cols).agg(
            pl.col("gross_load_mw").max().alias("max_gross_load_mw")
        )
        cems_working = (
            cems.join(max_gross_load, on=unit_cols)
            .with_columns(
                [
                    (pl.col("gross_load_mw") / pl.col("max_gross_load_mw")).alias(
                        "load_factor"
                    ),
                    (pl.col("gross_load_mw") * pl.col("operating_time_hours")).alias(
                        "gross_load_mwh"
                    ),
                ]
            )
            .with_columns(
                (pl.col("heat_content_mmbtu") / pl.col("gross_load_mwh")).alias(
                    "heat_rate_mmbtu_per_mwh"
                ),
            )
        )
        load_factor_col = "load_factor"
        generation_col = "gross_load_mwh"
        heat_rate_col = "heat_rate_mmbtu_per_mwh"
        max_load_col = "max_gross_load_mw"
        output_max_load_col = "max_gross_load_mw"
        output_ramp_rate_col_suffix = "max_gross_load"

    return (
        cems_working.sort(unit_cols + ["operating_datetime_utc"]),
        {
            "load_factor_col": load_factor_col,
            "generation_col": generation_col,
            "heat_rate_col": heat_rate_col,
            "max_load_col": max_load_col,
            "output_max_load_col": output_max_load_col,
            "output_ramp_rate_col_suffix": output_ramp_rate_col_suffix,
        },
    )


def prep_output_df(
    cems: pl.DataFrame,
    unit_cols: list[str],
    output_max_load_col: str,
    output_ramp_rate_col_suffix: str,
) -> pl.DataFrame:
    """Set up aggregated output dataframe with empty calculated columns."""
    # The order of these columns matters. some states have no valid units
    # to bin and calculate all the operational characteristics so this shell
    # of a DataFrame is the output. And polars only wants to concat on
    # DataFrames that are ordered exactly the same
    base_agg = {}
    if "plant_id_eia" in cems.columns:
        base_agg["plant_id_eia"] = pl.col("plant_id_eia").first()
    base_agg[output_max_load_col] = pl.col(output_max_load_col).first()

    output = (
        cems.group_by(unit_cols)
        .agg(**base_agg)
        .with_columns(
            pl.lit(None).cast(pl.Float64).alias("min_stable_level"),
            pl.lit(None).cast(pl.Float64).alias("min_up_time_hours"),
            pl.lit(None).cast(pl.Float64).alias("min_down_time_hours"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias("heat_rate_at_max_load_factor_mmbtu_per_mwh"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias("heat_rate_at_min_stable_level_mmbtu_per_mwh"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias(f"ramp_up_rate_fraction_of_{output_ramp_rate_col_suffix}_per_min"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias(f"ramp_down_rate_fraction_of_{output_ramp_rate_col_suffix}_per_min"),
        )
    )
    return output


def compute_stable_runs(
    binned_cems: pl.LazyFrame, unit_cols: list[str], min_stable_consecutive_hours: int
) -> pl.LazyFrame:
    """Given a certain consecutive hour threshold, find runs with stable behavior.

    We remove the lowest load factor bin for this.
    """
    stable_runs = (
        binned_cems.filter(pl.col("load_factor_bin_ordinal") > 1)
        .group_by(
            unit_cols
            + [
                "load_factor_bin_ordinal",
                "load_factor_bin_left",
                "load_factor_bin",
                "bin_run_id",
            ]
        )
        .len()
        .rename({"len": "run_length"})
    )

    stable_bins = (
        stable_runs.filter(pl.col("run_length") >= min_stable_consecutive_hours)
        .sort(unit_cols + ["load_factor_bin_ordinal"])
        .unique(subset=unit_cols, keep="first")
        .rename(
            {
                "load_factor_bin_ordinal": "min_stable_bin_right",
                "load_factor_bin_left": "min_stable_level",
                "load_factor_bin": "min_stable_bin",
            }
        )
        .select(
            unit_cols + ["min_stable_bin_right", "min_stable_level", "min_stable_bin"]
        )
    )

    return stable_bins


def compute_heat_rate_at_max_load(
    heat_rate_input: pl.LazyFrame,
    unit_cols: list[str],
    heat_rate_col: str,
) -> pl.LazyFrame:
    """Compute the heat rate at the maximum load (by bin)."""
    max_bin = heat_rate_input.group_by(unit_cols).agg(
        pl.col("load_factor_bin_ordinal").max().alias("max_load_bin_right")
    )

    return (
        heat_rate_input.join(max_bin, on=unit_cols)
        .filter(pl.col("load_factor_bin_ordinal") == pl.col("max_load_bin_right"))
        .group_by(unit_cols)
        .agg(
            pl.col(heat_rate_col)
            .median()
            .alias("heat_rate_at_max_load_factor_mmbtu_per_mwh")
        )
    )


def compute_min_stable_heat_rates(
    heat_rate_input: pl.LazyFrame,
    stable_runs: pl.LazyFrame,
    unit_cols: list[str],
    heat_rate_col: str,
) -> pl.LazyFrame:
    """Compute the heat rate for the minimum stable run."""
    return (
        heat_rate_input.join(
            stable_runs.select(unit_cols + ["min_stable_bin"]),
            on=unit_cols,
            how="inner",
        )
        .filter(pl.col("load_factor_bin") == pl.col("min_stable_bin"))
        .group_by(unit_cols)
        .agg(
            pl.col(heat_rate_col)
            .median()
            .alias("heat_rate_at_min_stable_level_mmbtu_per_mwh")
        )
    )


def filter_for_min_stable_bin(df: pl.LazyFrame) -> pl.LazyFrame:
    """Filter out records below the minimum stable bin."""
    return df.filter(
        (pl.col("load_factor_bin").struct[0] >= pl.col("min_stable_bin").struct[0])
        & (pl.col("load_factor_bin").struct[1] >= pl.col("min_stable_bin").struct[1])
    )


def calculate_min_up_or_down_times(
    output: pl.LazyFrame,
    cems_with_stable_bins: pl.LazyFrame,
    unit_cols: list[str],
    up_or_down: Literal["up", "down"],
) -> pl.LazyFrame:
    """Calculate minimum up or down times.

    Hourly data points are considered "up" when the ``load_factor_bin`` is greater than the
    ``min_stable_bin`` (calculated in :func:`compute_stable_runs`).
    """
    if up_or_down == "up":
        # up times are considered up when the load_factor_bin is greater than the
        # min_stable_bin (calculated in compute_stable_runs)
        runs = filter_for_min_stable_bin(cems_with_stable_bins)
    else:
        runs = cems_with_stable_bins.filter(pl.col("load_factor_bin").is_null())

    if not runs.is_empty():
        min_up_or_down_times = (
            runs.with_columns(
                _add_run_id(runs, unit_cols=unit_cols).alias(f"{up_or_down}_run_id")
            )
            .group_by(unit_cols + [f"{up_or_down}_run_id"])
            .len()
            .group_by(unit_cols)
            .agg(pl.col("len").min().alias(f"min_{up_or_down}_time_hours"))
        )
        # the output already had all columns bc of prep_output_df including these min
        # up or down time hours. So we join and then take the non-null value.
        output = (
            output.join(min_up_or_down_times, on=unit_cols, how="left", suffix="_y")
            .with_columns(
                pl.coalesce(
                    [
                        pl.col(f"min_{up_or_down}_time_hours_y"),
                        pl.col(f"min_{up_or_down}_time_hours"),
                    ]
                ).alias(f"min_{up_or_down}_time_hours")
            )
            .drop(f"min_{up_or_down}_time_hours_y")
        )
    return output


def estimate_operational_characteristics_by_unit(
    cems: pl.LazyFrame,
    min_stable_consecutive_hours: int,
    adjusted: bool = False,
    load_factor_binning_method: Literal["pandas_cut", "vectorized"] = "vectorized",
    ramp_rate_binning_method: Literal["rank_split", "qcut"] = "rank_split",
) -> pl.DataFrame:
    """Estimate operational characteristics for every EPA CEMS plant-unit pair.

    Most calculations are done in a dataframe-wide, vectorized manner. Ramp-rate
    summaries still use a grouped helper so the output matches the original script's
    per-unit ``qcut`` behavior.

    Args:
        cems: Filtered hourly EPA CEMS records.
        min_stable_consecutive_hours: Minimum number of consecutive hours in a
            load-factor bin for that bin to count as a stable operating level.
        adjusted: Whether to compute characteristics from EIA-adjusted net
            generation instead of raw EPA CEMS gross load.
        load_factor_binning_method: ``"vectorized"`` (default) uses
            :func:`assign_load_factor_bins_vectorized`, a fully
            expression-based polars equivalent of ``pandas.cut``. ``"pandas_cut"``
            uses :func:`assign_groupwise_load_factor_bins`, the original per-unit
            pandas fallback, kept for A/B comparison; see
            ``notebooks/work-in-progress/sylvan-heat-rates.md``.
        ramp_rate_binning_method: ``"rank_split"`` (default) uses
            :func:`_summarize_ramp_rates`, the vectorized top/bottom-5%-by-rank
            approach. ``"qcut"`` uses :func:`_summarize_ramp_rates_qcut`, which
            reproduces the original script's ``pandas.qcut`` behavior. Exposed for A/B
            comparison; see ``notebooks/work-in-progress/sylvan-heat-rates.md``.

    """
    # Filter and pre-process CEMS based on adjustment boolean
    unit_cols = ["plant_id_epa", "emissions_unit_id_epa"]
    cems_working, col_dict = handle_adjustment_in_cems(cems, unit_cols, adjusted)
    # Assign groupwise load factor bins
    if load_factor_binning_method == "vectorized":
        cems_working = assign_load_factor_bins_vectorized(
            cems_working=cems_working,
            unit_cols=unit_cols,
            load_factor_col=col_dict["load_factor_col"],
        )
    else:
        cems_working = assign_groupwise_load_factor_bins(
            cems_working=cems_working,
            unit_cols=unit_cols,
            load_factor_col=col_dict["load_factor_col"],
        )

    # Set up dataframe with analytical columns
    output = prep_output_df(
        cems_working,
        unit_cols,
        col_dict["output_max_load_col"],
        col_dict["output_ramp_rate_col_suffix"],
    )

    valid_units = (
        cems_working.filter(pl.col("load_factor_nunique") > 1)
        .select(unit_cols)
        .unique()
    )

    if valid_units.is_empty():
        return output

    valid_cems = cems_working.join(valid_units, on=unit_cols, how="inner")
    binned_cems = valid_cems.filter(pl.col("load_factor_bin").is_not_null()).pipe(
        lambda lf: lf.with_columns(
            bin_run_id=_add_run_id(lf, unit_cols=unit_cols + ["load_factor_bin"])
        )
    )

    # Compute stable runs
    stable_runs = compute_stable_runs(
        binned_cems, unit_cols, min_stable_consecutive_hours
    )

    # Compute heat rates
    heat_rate_input = binned_cems.drop_nulls(
        [col_dict["load_factor_col"], col_dict["heat_rate_col"]]
    )

    max_load_heat_rates = compute_heat_rate_at_max_load(
        heat_rate_input, unit_cols, col_dict["heat_rate_col"]
    )
    min_stable_heat_rates = compute_min_stable_heat_rates(
        heat_rate_input, stable_runs, unit_cols, col_dict["heat_rate_col"]
    )
    cems_with_stable_bins = valid_cems.join(
        stable_runs.select(unit_cols + ["min_stable_bin_right", "min_stable_bin"]),
        on=unit_cols,
        how="left",
    )
    ramp_rate_summary_fn = (
        _summarize_ramp_rates_qcut
        if ramp_rate_binning_method == "qcut"
        else _summarize_ramp_rates
    )
    ramp_rates = ramp_rate_summary_fn(
        cems_with_stable_bins=cems_with_stable_bins,
        unit_cols=unit_cols,
        generation_col=col_dict["generation_col"],
    )

    output = (
        # Add stable bins back to the main output DF
        output.join(
            stable_runs,
            on=unit_cols,
            how="left",
            suffix="_stable",
        )
        .with_columns(
            pl.coalesce(
                [pl.col("min_stable_level_stable"), pl.col("min_stable_level")]
            ).alias("min_stable_level")
        )
        .drop("min_stable_level_stable")
        .join(max_load_heat_rates, on=unit_cols, how="left", suffix="_y")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("heat_rate_at_max_load_factor_mmbtu_per_mwh_y"),
                    pl.col("heat_rate_at_max_load_factor_mmbtu_per_mwh"),
                ]
            ).alias("heat_rate_at_max_load_factor_mmbtu_per_mwh")
        )
        .drop("heat_rate_at_max_load_factor_mmbtu_per_mwh_y")
        .join(min_stable_heat_rates, on=unit_cols, how="left", suffix="_y")
        .with_columns(
            pl.coalesce(
                [
                    pl.col("heat_rate_at_min_stable_level_mmbtu_per_mwh_y"),
                    pl.col("heat_rate_at_min_stable_level_mmbtu_per_mwh"),
                ]
            ).alias("heat_rate_at_min_stable_level_mmbtu_per_mwh")
        )
        .drop("heat_rate_at_min_stable_level_mmbtu_per_mwh_y")
        .pipe(
            calculate_min_up_or_down_times,
            cems_with_stable_bins,
            unit_cols=unit_cols,
            up_or_down="up",
        )
        .pipe(
            calculate_min_up_or_down_times,
            cems_with_stable_bins,
            unit_cols=unit_cols,
            up_or_down="down",
        )
        .join(
            ramp_rates,
            on=unit_cols,
            how="left",
        )
        .with_columns(
            (
                pl.col("ramp_up_rate") / pl.col(col_dict["output_max_load_col"]) / 60
            ).alias(
                f"ramp_up_rate_fraction_of_{col_dict['output_ramp_rate_col_suffix']}_per_min"
            ),
            (
                pl.col("ramp_down_rate") / pl.col(col_dict["output_max_load_col"]) / 60
            ).alias(
                f"ramp_down_rate_fraction_of_{col_dict['output_ramp_rate_col_suffix']}_per_min"
            ),
            pl.col("heat_rate_at_max_load_factor_mmbtu_per_mwh"),
            pl.col("heat_rate_at_min_stable_level_mmbtu_per_mwh"),
        )
    )

    ordered_cols = [
        "plant_id_epa",
        "emissions_unit_id_epa",
        "plant_id_eia",
        col_dict["output_max_load_col"],
        "min_stable_level",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_level_mmbtu_per_mwh",
        f"ramp_up_rate_fraction_of_{col_dict['output_ramp_rate_col_suffix']}_per_min",
        f"ramp_down_rate_fraction_of_{col_dict['output_ramp_rate_col_suffix']}_per_min",
    ]

    return output.select(ordered_cols)


def operational_characteristics_factory(
    state: str,
) -> AssetsDefinition:
    """State partition of out_epacems__yearly_operational_characteristics."""

    @asset(
        required_resource_keys={"global_data_config"},
        name=f"_out_epacems__yearly_operational_characteristics_{state}",
        ins={"core_epacems__hourly_emissions": AssetIn()},
        config_schema=HEAT_RATE_ANALYSIS_CONFIG_SCHEMA,
        # op_tags={"memory-use": "high"},
    )
    def _out_epacems_state(
        context: AssetExecutionContext,
        core_epacems__hourly_emissions: pl.LazyFrame,
    ) -> pl.DataFrame:
        """Estimate EPA CEMS unit operational characteristics.

        This table corresponds to the script output named ``epa_op_char_output_df.csv``.
        """
        heat_rate_config = _get_heat_rate_analysis_config(context)
        year_quarters = context.resources.global_data_config.pudl.epacems.year_quarters
        max_full_year = int(
            max(
                year_quarter.removesuffix("q4")
                for year_quarter in year_quarters
                if year_quarter.endswith("q4")
            )
        )
        logger.info(
            f"Deriving unit-level operational characteristics from {state} EPA CEMS "
        )

        cems = filter_cems_for_heat_rate_analysis(
            core_epacems__hourly_emissions=core_epacems__hourly_emissions,
            final_year=max_full_year,
            num_years=heat_rate_config["num_years"],
            states=[state],
        )
        state_lf = estimate_operational_characteristics_by_unit(
            cems=cems,
            min_stable_consecutive_hours=heat_rate_config[
                "min_stable_consecutive_hours"
            ],
            load_factor_binning_method=heat_rate_config["load_factor_binning_method"],
            ramp_rate_binning_method=heat_rate_config["ramp_rate_binning_method"],
        )
        state_lf = state_lf.with_columns(
            pl.repeat(state, n=state_lf.height).alias("state"),
            pl.repeat(max_full_year, n=state_lf.height).alias("report_year"),
        )
        return state_lf

    return _out_epacems_state


operational_characteristics_assets = [
    operational_characteristics_factory(state) for state in EPACEMS_STATES
]


@asset(
    name="out_epacems__yearly_operational_characteristics",
    ins={
        f"_out_epacems__yearly_operational_characteristics_{state}": AssetIn()
        for state in EPACEMS_STATES
    },
    io_manager_key="pudl_io_manager",
    compute_kind="Python",
)
def out_epacems__yearly_operational_characteristics(context, **kwargs) -> pd.DataFrame:
    """Estimate EPA CEMS unit operational characteristics."""
    return pl.concat(kwargs.values()).to_pandas()


@asset(
    name="out_epacems__yearly_operational_characteristics_single_asset",
    required_resource_keys={"global_data_config"},
    ins={"core_epacems__hourly_emissions": AssetIn()},
    # Deliberately omits io_manager_key="pudl_io_manager": that IO manager requires
    # the asset name to match a registered pudl.metadata.resources.Resource, which
    # this experimental, not-a-real-output-table asset intentionally isn't. Falls
    # back to Dagster's default (unconfigured) IO manager instead.
    config_schema=HEAT_RATE_ANALYSIS_CONFIG_SCHEMA,
    compute_kind="Python",
)
def out_epacems__yearly_operational_characteristics_single_asset(
    context: AssetExecutionContext,
    core_epacems__hourly_emissions: pl.LazyFrame,
) -> pd.DataFrame:
    """Estimate EPA CEMS unit operational characteristics for every state.

    Alternative to :func:`operational_characteristics_factory` +
    :func:`out_epacems__yearly_operational_characteristics`: rather than fanning
    out into one Dagster asset per state (51 assets, each carrying its own
    scheduling/IO overhead) plus a combiner asset, this loops over every state
    within a single asset. With the vectorized binning path, even the largest
    state (California) takes about a second to compute, so the per-asset
    overhead of the fan-out plausibly outweighs the benefit of parallelizing
    across states. Produces row-equivalent output (row order is not
    guaranteed to match) to ``out_epacems__yearly_operational_characteristics``
    for direct comparison.
    """
    heat_rate_config = _get_heat_rate_analysis_config(context)
    year_quarters = context.resources.global_data_config.pudl.epacems.year_quarters
    max_full_year = int(
        max(
            year_quarter.removesuffix("q4")
            for year_quarter in year_quarters
            if year_quarter.endswith("q4")
        )
    )

    state_dfs = []
    for state in sorted(EPACEMS_STATES):
        logger.info(
            f"Deriving unit-level operational characteristics from {state} EPA CEMS"
        )
        cems = filter_cems_for_heat_rate_analysis(
            core_epacems__hourly_emissions=core_epacems__hourly_emissions,
            final_year=max_full_year,
            num_years=heat_rate_config["num_years"],
            states=[state],
        )
        state_df = estimate_operational_characteristics_by_unit(
            cems=cems,
            min_stable_consecutive_hours=heat_rate_config[
                "min_stable_consecutive_hours"
            ],
            load_factor_binning_method=heat_rate_config["load_factor_binning_method"],
            ramp_rate_binning_method=heat_rate_config["ramp_rate_binning_method"],
        )
        state_df = state_df.with_columns(
            pl.repeat(state, n=state_df.height).alias("state"),
            pl.repeat(max_full_year, n=state_df.height).alias("report_year"),
        )
        state_dfs.append(state_df)

    return pl.concat(state_dfs).to_pandas()


##################
## EIA-Based stuff
##################


# def filter_eia_generators_for_heat_rate_analysis(
#     out_eia__monthly_generators: pd.DataFrame,
#     report_date: str,
#     states: list[str] | None = None,
# ) -> pl.LazyFrame:
#     """Filter monthly EIA generator records to the configured snapshot.

#     Args:
#         out_eia__monthly_generators: Monthly EIA generator attributes.
#         report_date: Report date to use as the EIA generator snapshot.
#         states: Optional list of two-letter state abbreviations to include.

#     Returns:
#         Monthly generator records filtered to the requested snapshot and states.
#     """
#     report_timestamp = pd.Timestamp(report_date)
#     generators = (
#         pl.from_pandas(out_eia__monthly_generators)
#         .lazy()
#         .filter(pl.col("report_date").str.to_datetime() == report_timestamp)
#     )

#     if states:
#         generators = generators.filter(pl.col("state").is_in(states))

#     return generators


# def filter_eia_epa_mapping_for_heat_rate_analysis(
#     core_epa__assn_eia_epacamd: pd.DataFrame,
#     eia_epa_mapping_year: int,
# ) -> pl.LazyFrame:
#     """Filter the EPA/EIA crosswalk to one configured report year.

#     Args:
#         core_epa__assn_eia_epacamd: EPA/EIA crosswalk table.
#         eia_epa_mapping_year: Report year to use when mapping EPA units to EIA
#             generators.

#     Returns:
#         Unique EPA unit to EIA generator mappings for the requested report year.
#     """
#     return (
#         pl.from_pandas(core_epa__assn_eia_epacamd)
#         .lazy()
#         .filter(pl.col("report_year") == eia_epa_mapping_year)
#         .select(
#             [
#                 "plant_id_epa",
#                 "emissions_unit_id_epa",
#                 "plant_id_eia",
#                 "generator_id",
#             ]
#         )
#         .unique()
#     )


# def summarize_eia_generators(
#     generators: pl.LazyFrame,
#     eia_epa_mapping: pl.LazyFrame,
# ) -> dict[str, pl.LazyFrame]:
#     """Summarize EIA generator capacity at plant, generator, and EPA unit levels.

#     Args:
#         generators: Filtered monthly EIA generator records.
#         eia_epa_mapping: Filtered EPA/EIA crosswalk records.

#     Returns:
#         Dictionary containing plant-generator, plant, and plant-unit summaries.
#     """
#     generator_cols = [
#         "plant_id_eia",
#         "generator_id",
#         "report_date",
#         "prime_mover_code",
#         "capacity_mw",
#         "summer_capacity_mw",
#         "winter_capacity_mw",
#         "latitude",
#         "longitude",
#     ]

#     capacity_cols = ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]

#     # Create a generator-level summary
#     plant_gen = (
#         generators.select(generator_cols)
#         .with_columns(
#             max_cap_mw=pl.max_horizontal(capacity_cols),
#         )
#         .join(
#             eia_epa_mapping,
#             on=["plant_id_eia", "generator_id"],
#             how="left",
#         )
#     )

#     # Create a plant-level summary
#     plant = (
#         generators.group_by(["plant_id_eia", "report_date"])
#         .agg(pl.col(capacity_cols).sum())
#         .with_columns(
#             max_cap_mw=pl.max_horizontal(capacity_cols),
#         )
#         .with_columns(
#             max_mwh=pl.col("max_cap_mw") * 24 * 30,
#         )
#     )

#     # Create an EPA unit-level summary
#     plant_unit = (
#         plant_gen.group_by(["plant_id_eia", "emissions_unit_id_epa"])
#         .agg(pl.col(capacity_cols).sum())
#         .with_columns(
#             max_cap_mw=pl.max_horizontal(capacity_cols),
#         )
#     )

#     return {
#         "plant_gen": plant_gen,
#         "plant": plant,
#         "plant_unit": plant_unit,
#     }


# def summarize_eia923_monthly_plant_fuel(
#     core_eia923__monthly_generation_fuel: pd.DataFrame,
#     eia_plant_summary: pl.LazyFrame,
#     plant_ids_eia: pd.Series,
#     start_year: int,
# ) -> pl.LazyFrame:
#     """Summarize monthly EIA 923 plant generation and fuel consumption.

#     Args:
#         core_eia923__monthly_generation_fuel: Monthly plant fuel and generation
#             records.
#         eia_plant_summary: Plant-level generator capacity summary.
#         plant_ids_eia: EIA plant IDs to include.
#         start_year: First report year to include.

#     Returns:
#         Monthly plant-level EIA 923 generation, fuel, heat rate, and load factor.
#     """
#     # Filter data to desired plants and
#     eia923 = (
#         pl.from_pandas(core_eia923__monthly_generation_fuel)
#         .lazy()
#         .with_columns(
#             year=pl.col("report_date").dt.year(),
#             month=pl.col("report_date").dt.month(),
#         )
#         .filter(
#             (pl.col("year") >= start_year)
#             & (pl.col("data_maturity") == "final")
#             & (pl.col("plant_id_eia").is_in(plant_ids_eia.dropna().unique()))
#         )
#     )

#     monthly_plant = (
#         eia923.group_by(["plant_id_eia", "year", "month"])
#         .agg(
#             pl.col(
#                 [
#                     "net_generation_mwh",
#                     "fuel_consumed_mmbtu",
#                     "fuel_consumed_for_electricity_mmbtu",
#                 ]
#             ).sum()
#         )
#         .join(eia_plant_summary, on="plant_id_eia", how="left")
#         .with_columns(
#             heat_rate_mmbtu_per_mwh_net_generation=(
#                 pl.col("fuel_consumed_for_electricity_mmbtu")
#                 / pl.col("net_generation_mwh")
#             ),
#             load_factor_net_generation=pl.col("net_generation_mwh") / pl.col("max_mwh"),
#         )
#     )

#     return monthly_plant


# def summarize_cems_monthly_plant_operations(
#     cems: pl.LazyFrame,
#     eia_plant_summary: pl.LazyFrame,
# ) -> pd.DataFrame:
#     """Summarize monthly EPA CEMS plant gross load and fuel consumption.

#     Args:
#         cems: Filtered hourly EPA CEMS records.
#         eia_plant_summary: Plant-level generator capacity summary.

#     Returns:
#         Monthly plant-level CEMS gross load, fuel, heat rate, and load factor.
#     """
#     monthly_plant = (
#         cems.with_columns(month=pl.col("operating_datetime_utc").dt.month())
#         .group_by(["plant_id_eia", "year", "month"])
#         .agg(pl.col(["gross_load_mw", "heat_content_mmbtu"]).sum())
#         .join(eia_plant_summary, on="plant_id_eia", how="left")  # TODO: Validate merge?
#         .with_columns(
#             heat_rate_mmbtu_per_mwh_gross_load=(
#                 pl.col("heat_content_mmbtu") / pl.col("gross_load_mw")
#             ),
#             load_factor_gross_load=pl.col("gross_load_mw") / pl.col("max_mwh"),
#         )
#     )

#     return monthly_plant


# def estimate_gross_to_net_conversion_factors(
#     cems_monthly_plant_summary: pl.LazyFrame,
#     eia923_monthly_plant_summary: pl.LazyFrame,
# ) -> pl.LazyFrame:
#     """Estimate plant-level conversion factors from CEMS gross load to net generation.

#     Args:
#         cems_monthly_plant_summary: Monthly plant-level CEMS summary.
#         eia923_monthly_plant_summary: Monthly plant-level EIA 923 summary.

#     Returns:
#         Plant-level conversion factor estimates and supporting fit metadata.
#     """
#     conversion = (
#         cems_monthly_plant_summary.join(
#             eia923_monthly_plant_summary,
#             on=[
#                 "plant_id_eia",
#                 "year",
#                 "month",
#                 "report_date",
#                 "capacity_mw",
#                 "summer_capacity_mw",
#                 "winter_capacity_mw",
#                 "max_cap_mw",
#                 "max_mwh",
#             ],
#             how="left",
#             suffix="_eia923",
#         )
#         .with_columns(
#             gen_cems_to_net_gen_conversion_factor=(
#                 pl.col("net_generation_mwh") / pl.col("gross_load_mw")
#             ),
#             fuel_cems_to_eia923_conversion_factor=(
#                 pl.col("fuel_consumed_for_electricity_mmbtu")
#                 / pl.col("heat_content_mmbtu")
#             ),
#         )
#         .with_columns(pl.all().replace([float("inf"), float("-inf")], None))
#         .drop_nulls(
#             [
#                 "plant_id_eia",
#                 "load_factor_gross_load",
#                 "gen_cems_to_net_gen_conversion_factor",
#             ]
#         )
#         .filter(
#             pl.col("load_factor_gross_load").is_between(0, 1)
#             & pl.col("gen_cems_to_net_gen_conversion_factor").is_between(0, 1)
#         )
#     )

#     plant_fits = conversion.group_by("plant_id_eia").agg(
#         [
#             pl.lit(0.0).alias("a1"),
#             pl.col("gen_cems_to_net_gen_conversion_factor").mean().alias("a0"),
#             pl.lit("constant").alias("fit_type"),
#             pl.col("load_factor_gross_load").min().alias("min_obs_lf"),
#             pl.col("load_factor_gross_load").max().alias("max_obs_lf"),
#             pl.len().alias("n_obs"),
#             pl.col("fuel_cems_to_eia923_conversion_factor").mean(),
#             # same as a0
#             pl.col("gen_cems_to_net_gen_conversion_factor")
#             .mean()
#             .alias("gen_cems_to_net_gen_conversion_factor_at_min_load_factor"),
#             # same as a0
#             pl.col("gen_cems_to_net_gen_conversion_factor")
#             .mean()
#             .alias("gen_cems_to_net_gen_conversion_factor_at_max_load_factor"),
#         ]
#     )

#     return plant_fits


# def add_adjusted_net_generation_to_cems(
#     cems: pl.LazyFrame,
#     conversion_factors: pl.LazyFrame,
#     eia_plant_unit_summary: pl.LazyFrame,
# ) -> pl.LazyFrame:
#     """Add estimated net generation and adjusted heat rates to hourly CEMS records.

#     Args:
#         cems: Filtered hourly EPA CEMS records.
#         conversion_factors: Plant-level gross-to-net and fuel conversion factors.
#         eia_plant_unit_summary: EIA capacity summary by plant and EPA emissions unit.

#     Returns:
#         Hourly CEMS records with estimated net generation, adjusted fuel, adjusted heat
#         rates, and adjusted load factors.
#     """
#     cems_adjusted = (
#         cems.join(conversion_factors, on="plant_id_eia", how="left")
#         .join(
#             eia_plant_unit_summary.select(
#                 ["plant_id_eia", "emissions_unit_id_epa", "capacity_mw", "max_cap_mw"]
#             ),
#             on=["plant_id_eia", "emissions_unit_id_epa"],
#             how="left",
#         )
#         .with_columns(
#             net_generation_mwh_cems=(
#                 pl.col("gross_load_mw")
#                 * pl.col("gen_cems_to_net_gen_conversion_factor_at_max_load_factor")
#             ),
#             fuel_consumed_for_electricity_mmbtu_cems=(
#                 pl.col("heat_content_mmbtu")
#                 * pl.col("fuel_cems_to_eia923_conversion_factor")
#             ),
#         )
#         .with_columns(
#             heat_rate_net_generation_cems=(
#                 pl.col("fuel_consumed_for_electricity_mmbtu_cems")
#                 / pl.col("net_generation_mwh_cems")
#             ),
#             load_factor_adjusted_cems=(
#                 pl.col("net_generation_mwh_cems") / pl.col("max_cap_mw")
#             ),
#         )
#     )

#     return cems_adjusted
