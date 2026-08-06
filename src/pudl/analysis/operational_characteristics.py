"""Use EPA CEMS and EIA data to estimate generator operational characteristics."""

from typing import Literal

import pandas as pd
import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetIn,
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
}


def _get_heat_rate_analysis_config(
    context: AssetExecutionContext,
) -> dict[str, int]:
    """Extract heat rate analysis settings from Dagster asset config."""
    return {
        "num_years": context.op_config["num_years"],
        "min_stable_consecutive_hours": context.op_config[
            "min_stable_consecutive_hours"
        ],
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


def _add_run_id_expr(
    unit_cols: list[str],
    state_col: str | None = None,
) -> pl.Expr:
    """Build an expression assigning run IDs to consecutive hourly observations.

    Assumes the frame is already sorted by ``unit_cols`` (and, implicitly,
    ``operating_datetime_utc``), since it relies on ``.shift()`` to compare each row
    to its immediate predecessor.
    """
    same_unit_and_bin = pl.all_horizontal(
        [pl.col(c).eq(pl.col(c).shift()) for c in unit_cols]
    )
    consecutive_hour = (
        pl.col("operating_datetime_utc").diff().dt.total_seconds().truediv(3600).eq(1)
    )
    same_state = (
        pl.lit(True)
        if state_col is None
        else pl.col(state_col).eq(pl.col(state_col).shift())
    )
    return (
        (~(same_unit_and_bin & consecutive_hour & same_state)).fill_null(True).cum_sum()
    )


def assign_groupwise_load_factor_bins(
    cems_working: pl.LazyFrame,
    unit_cols: list[str],
    load_factor_col: str,
) -> pl.DataFrame:
    """Fully vectorized, per-unit equal-width load-factor binning.

    This uses polars but is replicating the ``pandas.cut`` methodology. This results
    in a ``load_factor_bin` `column with 10 unique values with a two dimensional
    structure as a datatype. The left value of the structure is the lower bound of the
    load factors within that bin and the right value it the higher bound. Using the
    ``load_factor_bin`` we also assign ``load_factor_bin_rank`` which is the lower
    bound of the lowest ``load_factor_bin``.

    This function uses polars but is attempting to directly reproduce
    ``pandas.cut(bins=10, right=True, include_lowest=False)`` within each unit group.
    The pandas methodology computes 10 equal-width bins spanning that unit's own
    observed min/max ``load_factor_col`` (``width = (max - min) / 10``), except that
    only the *lowest* bin's left edge is padded by 0.1% of the range (or by 0.001 when
    the range is zero) so that the unit's minimum observation falls inside the
    first (right-closed) bin rather than outside every bin -- matching pandas'
    ``_bins_to_cuts`` behavior of shifting only ``bins[0]``, not redistributing
    the padding across all ten bins.
    """
    # compute group load factor
    cems_working = cems_working.with_columns(
        pl.col(load_factor_col)
        .drop_nulls()
        .n_unique()
        .over(unit_cols)
        .alias("load_factor_nunique")
    )

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
    bin_lower = (
        pl.when(bin_idx == 1).then(lo - pad).otherwise(lo + (bin_idx - 1) * width)
    )
    bin_upper = lo + bin_idx * width

    result = cems_working.with_columns(
        pl.when(eligible).then(bin_lower).alias("load_factor_bin_lower"),
        pl.when(eligible).then(bin_upper).alias("load_factor_bin_upper"),
        pl.col("state").cast(pl.Categorical),
    ).with_columns(
        pl.when(eligible)
        .then(
            pl.struct(
                left=pl.col("load_factor_bin_lower"),
                right=pl.col("load_factor_bin_upper"),
            )
        )
        .alias("load_factor_bin"),
        # Rank bins for each set of unit_cols
        pl.col("load_factor_bin_lower")
        .rank(method="dense")
        .over(unit_cols)
        .alias("load_factor_bin_rank"),
    )

    # This collect() is intentional and important for performance: cems_working
    # feeds several independent downstream branches (prep_output_df, the
    # valid_cems/binned_cems chain, and cems_with_stable_bins), each of which
    # would otherwise re-trigger the .over(unit_cols) window computations above
    # (min/max/n_unique/rank -- each a full pass over the unit's hourly data)
    # from scratch. Materializing here forces that shared computation to happen
    # exactly once. Measured empirically: removing this collect() and staying
    # lazy through to the end of the pipeline was ~3.5x slower and used ~30% more
    # peak memory on real CEMS data, because the query planner doesn't common
    # subexpression eliminate across those branches under the streaming engine.
    return result.collect()


def summarize_ramp_rates(
    cems_with_stable_bins: pl.DataFrame,
    unit_cols: list[str],
    generation_col: str,
) -> pl.DataFrame:
    """Summarize exact per-unit ramp rates using the original qcut approach."""
    ramp_input = (
        # TODO: (Later) add in this line to remove startup time.
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
            ramp_rate=(pl.col("generation_delta") / pl.col("time_delta")).replace(
                [float("inf"), float("-inf")], None
            )
        )
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


def handle_adjustment_in_cems(
    cems: pl.LazyFrame, unit_cols: list[str], adjusted: bool = False
) -> tuple[pl.LazyFrame, dict[str, str]]:
    """Filter CEMS data, computing derived columns if not adjusted.

    This enables us to adjust the load factor based using net generation
    instead of gross generation. Adjusting the load factor is not yet
    implemented fully. A draft is below add_adjusted_net_generation_to_cems.

    This returns a lazframe and a dictionary with keys of the column references
    and values of the column names to use.

    TODO: Consider simplification or use of a dataclass or other lightweight data
    structure. Implement changes when we implement
    add_adjusted_net_generation_to_cems below.
    """
    cems_working = cems

    if adjusted:
        load_factor_col = "load_factor_adjusted_cems"
        generation_col = "net_generation_mwh_cems"
        heat_rate_col = "heat_rate_net_generation_cems"
        max_load_col = "max_cap_mw"

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

    return (
        cems_working.sort(unit_cols + ["operating_datetime_utc"]),
        {
            "load_factor_col": load_factor_col,
            "generation_col": generation_col,
            "heat_rate_col": heat_rate_col,
            "max_load_col": max_load_col,
        },
    )


def prep_output_df(
    cems: pl.DataFrame, unit_cols: list[str], max_load_col: str
) -> pl.DataFrame:
    """Set up aggregated output dataframe with empty calculated columns.

    Every unit gets a row here, even ones that don't have enough distinct load
    factors to bin (e.g. constant-load units) -- those come back all-null except for
    identifying columns and max load. Downstream steps merge their real values on top
    of this shell, so every unit is guaranteed to appear in the final output.
    """
    return (
        cems.group_by(unit_cols)
        .agg(
            pl.col("plant_id_eia").first(),
            pl.col("state").first(),
            pl.col(max_load_col).first(),
        )
        .with_columns(
            pl.lit(None).cast(pl.Float64).alias("min_stable_load_factor"),
            pl.lit(None).cast(pl.Float64).alias("min_up_time_hours"),
            pl.lit(None).cast(pl.Float64).alias("min_down_time_hours"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias("heat_rate_at_max_load_factor_mmbtu_per_mwh"),
            pl.lit(None)
            .cast(pl.Float64)
            .alias("heat_rate_at_min_stable_load_factor_mmbtu_per_mwh"),
            pl.lit(None).cast(pl.Float64).alias("ramp_up_rate_per_min"),
            pl.lit(None).cast(pl.Float64).alias("ramp_down_rate_per_min"),
        )
    )


def compute_minimum_stable_bin(
    binned_cems: pl.DataFrame, unit_cols: list[str], min_stable_consecutive_hours: int
) -> pl.DataFrame:
    """Given a certain consecutive hour threshold, find runs with stable behavior.

    This function determines the minimum stable load load factor bin, which means the
    lowest load factor which we see instances of consecutive running.

    For every record above the first load_factor_bin (aka when a unit is effectively
    off), first calculate how long any given "run" is. A "run" here is defined as
    a set of consecutive hours that are within the same ``load_factor_bin`` within a
    given unit.

    Once we know how long all the runs are, we find all of the runs that are longer
    than ``min_stable_consecutive_hours`` and we find the ``load_factor_bin`` which
    corresponds to the lowest ``load_factor_bin`` to get the minimum stable bin.
    """
    stable_runs = (
        binned_cems.filter(pl.col("load_factor_bin_rank") > 1)
        .group_by(
            unit_cols
            + [
                # these three load_factor_bin* columns are all derived from
                # load_factor_bin (aka we want them in stable_runs but they are
                # not adding to unique groups in this groupby).
                "load_factor_bin_rank",
                "load_factor_bin_lower",
                "load_factor_bin",
                "bin_run_id",
            ]
        )
        .len()
        .rename({"len": "run_length"})
    )

    stable_bins = (
        stable_runs.filter(pl.col("run_length") >= min_stable_consecutive_hours)
        .sort(unit_cols + ["load_factor_bin_rank"])
        # maintain_order=True is required for keep="first" to reliably mean "lowest
        # load_factor_bin_rank" -- without it polars doesn't guarantee that dedup
        # respects the preceding sort.
        .unique(subset=unit_cols, keep="first", maintain_order=True)
        .rename(
            {
                "load_factor_bin_rank": "min_stable_bin_upper",
                "load_factor_bin_lower": "min_stable_load_factor",
                "load_factor_bin": "min_stable_bin",
            }
        )
        .select(
            unit_cols
            + ["min_stable_bin_upper", "min_stable_load_factor", "min_stable_bin"]
        )
    )

    return stable_bins


def compute_heat_rate_at_max_load(
    heat_rate_input: pl.DataFrame,
    unit_cols: list[str],
    heat_rate_col: str,
) -> pl.DataFrame:
    """Compute the heat rate at the maximum load (by bin)."""
    max_bin = heat_rate_input.group_by(unit_cols).agg(
        pl.col("load_factor_bin_rank").max().alias("max_load_bin_upper")
    )

    return (
        heat_rate_input.join(max_bin, on=unit_cols)
        .filter(pl.col("load_factor_bin_rank") == pl.col("max_load_bin_upper"))
        .group_by(unit_cols)
        .agg(
            pl.col(heat_rate_col)
            .median()
            .alias("heat_rate_at_max_load_factor_mmbtu_per_mwh")
        )
    )


def compute_min_stable_heat_rates(
    heat_rate_input: pl.DataFrame,
    min_stable_bins: pl.DataFrame,
    unit_cols: list[str],
    heat_rate_col: str,
) -> pl.DataFrame:
    """Compute the heat rate for the minimum stable run."""
    return (
        heat_rate_input.join(
            min_stable_bins.select(unit_cols + ["min_stable_bin"]),
            on=unit_cols,
            how="inner",
        )
        .filter(pl.col("load_factor_bin") == pl.col("min_stable_bin"))
        .group_by(unit_cols)
        .agg(
            pl.col(heat_rate_col)
            .median()
            .alias("heat_rate_at_min_stable_load_factor_mmbtu_per_mwh")
        )
    )


def filter_for_min_stable_bin(df: pl.DataFrame) -> pl.DataFrame:
    """Filter out records below the minimum stable bin."""
    return df.filter(
        (pl.col("load_factor_bin").struct[0] >= pl.col("min_stable_bin").struct[0])
        & (pl.col("load_factor_bin").struct[1] >= pl.col("min_stable_bin").struct[1])
    )


def calculate_min_up_or_down_times(
    output: pl.DataFrame,
    cems_with_stable_bins: pl.DataFrame,
    unit_cols: list[str],
    up_or_down: Literal["up", "down"],
) -> pl.DataFrame:
    """Calculate minimum up or down times.

    Hourly data points are considered "up" when the ``load_factor_bin`` is greater than
    the ``min_stable_bin`` (calculated in :func:`compute_minimum_stable_bin`). Runs are
    considered "down" when there is no load_factor_bin (which is equivalent to having no
    load during that hour).
    """
    if up_or_down == "up":
        # up times are considered up when the load_factor_bin is greater than the
        # min_stable_bin (calculated in compute_minimum_stable_bin)
        runs = filter_for_min_stable_bin(cems_with_stable_bins)
    else:
        runs = cems_with_stable_bins.filter(pl.col("load_factor_bin").is_null())

    min_up_or_down_times = (
        runs.with_columns(
            _add_run_id_expr(unit_cols=unit_cols).alias(f"{up_or_down}_run_id")
        )
        .group_by(unit_cols + [f"{up_or_down}_run_id"])
        .len()
        .group_by(unit_cols)
        .agg(pl.col("len").min().alias(f"min_{up_or_down}_time_hours"))
        .cast({f"min_{up_or_down}_time_hours": pl.Float64})
    )
    # the output already had all columns bc of prep_output_df including these min
    # up or down time hours. So we join and then take the non-null value. If `runs`
    # was empty, this join is a no-op and the existing null column is left as-is.
    return output.drop(f"min_{up_or_down}_time_hours").join(
        min_up_or_down_times, on=unit_cols, how="left"
    )


def estimate_operational_characteristics_by_unit(
    cems: pl.LazyFrame,
    min_stable_consecutive_hours: int,
    adjusted: bool = False,
) -> pl.DataFrame:
    """Estimate operational characteristics for every EPA CEMS plant-unit pair.

    Everything through the initial load-factor binning step is lazily evaluated
    (see :func:`assign_groupwise_load_factor_bins`). Everything after that operates
    on the resulting eager ``DataFrame``, in a fully vectorized manner across every
    unit at once -- there's no per-unit or per-batch Python looping, and no
    ``pandas`` fallback.
    """
    # Filter and pre-process CEMS based on adjustment boolean
    unit_cols = ["plant_id_epa", "emissions_unit_id_epa"]
    cems_working, col_dict = handle_adjustment_in_cems(cems, unit_cols, adjusted)
    # Assign groupwise load factor bins
    cems_working = assign_groupwise_load_factor_bins(
        cems_working=cems_working,
        unit_cols=unit_cols,
        load_factor_col=col_dict["load_factor_col"],
    )

    # Set up dataframe with analytical columns: every unit (i.e. every set of
    # primary key values) gets a row, with nulls in all of the derived value
    # columns. Downstream steps merge their real values on top of this shell, so
    # units that don't have enough data to support the full analysis (e.g. a
    # constant-load unit) still show up in the output, just with null values.
    output = prep_output_df(
        cems_working,
        unit_cols,
        col_dict["max_load_col"],
    )
    # The load_factor_nunique column is assigned in assign_groupwise_load_factor_bins.
    # If there aren't more than one unique load factor, the rest of the calculations
    # can't be performed for that unit -- it stays null via the join below.
    valid_cems = cems_working.filter(pl.col("load_factor_nunique") > 1)
    binned_cems = valid_cems.filter(
        pl.col("load_factor_bin").is_not_null()
    ).with_columns(
        bin_run_id=_add_run_id_expr(unit_cols=unit_cols + ["load_factor_bin"])
    )

    # Compute heat rates
    heat_rate_input = binned_cems.drop_nulls(
        [col_dict["load_factor_col"], col_dict["heat_rate_col"]]
    )
    max_load_heat_rates = compute_heat_rate_at_max_load(
        heat_rate_input, unit_cols, col_dict["heat_rate_col"]
    )
    # Compute stable runs
    min_stable_bins = compute_minimum_stable_bin(
        binned_cems, unit_cols, min_stable_consecutive_hours
    )
    min_stable_heat_rates = compute_min_stable_heat_rates(
        heat_rate_input, min_stable_bins, unit_cols, col_dict["heat_rate_col"]
    )
    cems_with_stable_bins = valid_cems.join(
        min_stable_bins.select(unit_cols + ["min_stable_bin_upper", "min_stable_bin"]),
        on=unit_cols,
        how="left",
    )
    ramp_rates = summarize_ramp_rates(
        cems_with_stable_bins=cems_with_stable_bins,
        unit_cols=unit_cols,
        generation_col=col_dict["generation_col"],
    )

    output = (
        # Add stable bins back to the main output DF
        # bc we defined null version of all of the columns in output via prep_output_df
        # every time we merge in the derived data we drop that column first
        output.update(min_stable_bins, on=unit_cols, how="left")
        .update(max_load_heat_rates, on=unit_cols, how="left")
        .update(min_stable_heat_rates, on=unit_cols, how="left")
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
        .join(ramp_rates, on=unit_cols, how="left")
        .with_columns(
            (pl.col("ramp_up_rate") / pl.col(col_dict["max_load_col"]) / 60).alias(
                "ramp_up_rate_per_min"
            ),
            (pl.col("ramp_down_rate") / pl.col(col_dict["max_load_col"]) / 60).alias(
                "ramp_down_rate_per_min"
            ),
        )
    )

    ordered_cols = [
        "plant_id_epa",
        "emissions_unit_id_epa",
        "plant_id_eia",
        "state",
        col_dict["max_load_col"],
        "min_stable_load_factor",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
        "ramp_up_rate_per_min",
        "ramp_down_rate_per_min",
    ]

    return output.select(ordered_cols)


@asset(
    required_resource_keys={"global_data_config"},
    ins={"core_epacems__hourly_emissions": AssetIn()},
    config_schema=HEAT_RATE_ANALYSIS_CONFIG_SCHEMA,
    io_manager_key="pudl_io_manager",
    op_tags={"memory-use": "high"},  # Peak of ~16 GB as of 2026-08-05
)
def out_epacems__yearly_operational_characteristics(
    context: AssetExecutionContext,
    core_epacems__hourly_emissions: pl.LazyFrame,
) -> pd.DataFrame:
    """Estimate EPA CEMS unit operational characteristics for every unit.

    This table corresponds to the script output named ``epa_op_char_output_df.csv``.
    """
    heat_rate_config = _get_heat_rate_analysis_config(context)
    # Get the most recent full year of CEMS from the config. Grab all the
    # year-quarters, then find the max year with q4 in it.
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
            f"Deriving unit-level operational characteristics from {state} EPA CEMS "
        )
        # Filtering the lazyframe first down to only one state and only the last few
        # years (number of years based on the config stored in
        # HEAT_RATE_ANALYSIS_CONFIG_SCHEMA). Filter first to ensure the full hourly
        # cems data isn't loaded into memory.
        cems = filter_cems_for_heat_rate_analysis(
            core_epacems__hourly_emissions=core_epacems__hourly_emissions,
            final_year=max_full_year,
            num_years=heat_rate_config["num_years"],
            states=[state],
        )
        # This step does the bulk of the work. The output here is a table with one
        # record per unit with all of the derived characteristics
        state_df = estimate_operational_characteristics_by_unit(
            cems=cems,
            min_stable_consecutive_hours=heat_rate_config[
                "min_stable_consecutive_hours"
            ],
        )
        state_dfs.append(state_df)

    return (
        pl.concat(state_dfs)
        .with_columns(pl.lit(max_full_year).alias("report_year"))
        .to_pandas()
    )


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
