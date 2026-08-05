"""Isolate logic divergence between the original and new op_char pipelines.

Run both versions against the exact same rows pulled from the local PUDL parquet
outputs. Pull the "original-logic" from Jaxon's original pandas script, using the the
per-unit algorithm verbatim, against the same local parquet rows that are handed to the
new polars pipeline.

Usage::

    python isolated_component_comparison.py --help
    python isolated_component_comparison.py --states CA
    python isolated_component_comparison.py --final-year 2024 --num-years 3 --states CA,TX
"""

import os
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd
import polars as pl

from pudl.analysis.derived_plant_characteristics import (
    estimate_operational_characteristics_by_unit,
    filter_cems_for_heat_rate_analysis,
)

PUDL_OUTPUT = os.environ["PUDL_OUTPUT"]
HERE = Path(__file__).parent

# --- Original script's per-unit algorithm, copied verbatim for reference --------
# (from notebooks/work-in-progress/sylvan-heat-rates/pudl_thermal_characterization_2026-03-10.py)


def min_stable_level(hourly_plant_unit_df, consecutive_hours):
    """Find the smallest load-factor bin with a run of >= consecutive_hours hours."""
    min_stbl_lvl_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
    bins = sorted(min_stbl_lvl_df["load_factor_bin"].dropna().unique())
    for candidate_bin in bins[1:]:
        d = min_stbl_lvl_df[min_stbl_lvl_df["load_factor_bin"] == candidate_bin].copy()
        run_id = (
            d["operating_datetime_utc"]
            .diff()
            .dt.total_seconds()
            .div(3600)
            .ne(1)
            .cumsum()
        )
        if d.groupby(run_id).size().max() >= consecutive_hours:
            return candidate_bin, candidate_bin.left.max()
    return None, np.nan


def min_up_down_times(hourly_plant_unit_df, min_stbl_lvl):
    """Find the shortest observed run at/above the stable bin, and shortest outage."""
    up_down_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()

    up_df = up_down_df[up_down_df["load_factor_bin"] >= min_stbl_lvl].copy()
    up_run_id = (
        up_df["operating_datetime_utc"]
        .diff()
        .dt.total_seconds()
        .div(3600)
        .ne(1)
        .cumsum()
    )
    up_run_sizes = up_df.groupby(up_run_id).size()
    min_up_time = up_run_sizes.min()

    down_df = up_down_df[up_down_df["load_factor"].isna()].copy()
    down_run_id = (
        down_df["operating_datetime_utc"]
        .diff()
        .dt.total_seconds()
        .div(3600)
        .ne(1)
        .cumsum()
    )
    down_run_sizes = down_df.groupby(down_run_id).size()
    min_down_time = down_run_sizes.min()

    return min_up_time, min_down_time


def heat_rate(hourly_plant_unit_df, min_stbl_lvl):
    """Compute median heat rate at the top load-factor bin and at the stable bin."""
    heat_rates_df = hourly_plant_unit_df.copy()
    heat_rates_df = heat_rates_df.dropna(
        subset=["load_factor", "heat_rate_mmbtu_per_MWh"]
    )

    max_lf_bin = heat_rates_df["load_factor_bin"].max()
    max_lf_df = heat_rates_df[heat_rates_df["load_factor_bin"] == max_lf_bin]
    max_lf_hr = max_lf_df["heat_rate_mmbtu_per_MWh"].median()

    min_stbl_df = heat_rates_df[heat_rates_df["load_factor_bin"] == min_stbl_lvl]
    min_stbl_hr = min_stbl_df["heat_rate_mmbtu_per_MWh"].median()

    return max_lf_hr, min_stbl_hr


def ramp_rate(hourly_plant_unit_df):
    """Compute ramp-up/ramp-down rates via pandas.qcut(q=20) of hourly deltas."""
    ramp_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
    ramp_df["time_delta"] = (
        ramp_df["operating_datetime_utc"].diff().dt.total_seconds().div(3600)
    )
    ramp_df["mwh_delta"] = ramp_df["gross_load_MWh"].diff()
    ramp_df["ramp_rate"] = ramp_df["mwh_delta"] / ramp_df["time_delta"]
    ramp_df = ramp_df.dropna(subset=["ramp_rate"])

    ramp_df["ramp_rate_bin"] = pd.qcut(ramp_df["ramp_rate"], q=20, duplicates="drop")

    low_bin = ramp_df["ramp_rate_bin"].min()
    high_bin = ramp_df["ramp_rate_bin"].max()
    ramp_down_rate = ramp_df[ramp_df["ramp_rate_bin"] == low_bin]["ramp_rate"].median()
    ramp_up_rate = ramp_df[ramp_df["ramp_rate_bin"] == high_bin]["ramp_rate"].median()

    return ramp_up_rate, ramp_down_rate


def build_op_char_df(epa_cems_df, plant_id, unit_id, min_stable_consecutive_hours):
    """Compute one plant-unit's operational characteristics via the original algorithm."""
    epa_cems_plant_unit_df = epa_cems_df[
        (epa_cems_df["plant_id_epa"] == plant_id)
        & (epa_cems_df["emissions_unit_id_epa"] == unit_id)
    ].copy()
    max_gross_load_mw = epa_cems_plant_unit_df["gross_load_mw"].max()
    epa_cems_plant_unit_df["load_factor"] = (
        epa_cems_plant_unit_df["gross_load_mw"] / max_gross_load_mw
    )
    epa_cems_plant_unit_df["gross_load_MWh"] = (
        epa_cems_plant_unit_df["gross_load_mw"]
        * epa_cems_plant_unit_df["operating_time_hours"]
    )
    epa_cems_plant_unit_df["heat_rate_mmbtu_per_MWh"] = (
        epa_cems_plant_unit_df["heat_content_mmbtu"]
        / epa_cems_plant_unit_df["gross_load_MWh"]
    )
    epa_cems_plant_unit_df["operating_datetime_utc"] = pd.to_datetime(
        epa_cems_plant_unit_df["operating_datetime_utc"]
    )
    valid_load_factors = epa_cems_plant_unit_df["load_factor"].dropna()

    # Verbatim port of the original script's check -- not "fixed" for efficiency
    # here so this stays a faithful reference implementation to compare against.
    if valid_load_factors.nunique() > 1:  # noqa: PD101
        epa_cems_plant_unit_df["load_factor_bin"] = pd.cut(
            epa_cems_plant_unit_df["load_factor"],
            bins=10,
            right=True,
            include_lowest=False,
        )
        min_stbl_lvl_bin, min_stbl_lvl = min_stable_level(
            epa_cems_plant_unit_df, min_stable_consecutive_hours
        )
        max_lf_hr, min_stbl_hr = heat_rate(epa_cems_plant_unit_df, min_stbl_lvl_bin)
        min_up_time, min_down_time = min_up_down_times(
            epa_cems_plant_unit_df, min_stbl_lvl_bin
        )
        ramp_up_rate, ramp_down_rate = ramp_rate(epa_cems_plant_unit_df)
    else:
        min_stbl_lvl = np.nan
        max_lf_hr = min_stbl_hr = np.nan
        min_up_time = min_down_time = np.nan
        ramp_up_rate = ramp_down_rate = np.nan

    return pd.DataFrame(
        {
            "plant_id_epa": [plant_id],
            "emissions_unit_id_epa": [unit_id],
            "max_gross_load_mw": [max_gross_load_mw],
            "min_stable_level": [min_stbl_lvl],
            "min_up_time_hours": [min_up_time],
            "min_down_time_hours": [min_down_time],
            "heat_rate_at_max_load_factor_mmbtu_per_mwh": [
                round(max_lf_hr, 2) if pd.notna(max_lf_hr) else np.nan
            ],
            "heat_rate_at_min_stable_level_mmbtu_per_mwh": [
                round(min_stbl_hr, 2) if pd.notna(min_stbl_hr) else np.nan
            ],
            "ramp_up_rate_fraction_of_max_gross_load_per_min": [
                round(ramp_up_rate / max_gross_load_mw / 60, 4)
                if pd.notna(ramp_up_rate)
                else np.nan
            ],
            "ramp_down_rate_fraction_of_max_gross_load_per_min": [
                round(ramp_down_rate / max_gross_load_mw / 60, 4)
                if pd.notna(ramp_down_rate)
                else np.nan
            ],
        }
    )


def main(
    final_year: int,
    num_years: int,
    states: list[str],
    min_stable_consecutive_hours: int,
) -> int:
    """Run the same-input old-vs-new comparison and print/write a divergence summary."""
    cems_lf = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/core_epacems__hourly_emissions.parquet"
    )
    cems_filtered_lf = filter_cems_for_heat_rate_analysis(
        core_epacems__hourly_emissions=cems_lf,
        final_year=final_year,
        num_years=num_years,
        states=states,
    )
    cems_pd = cems_filtered_lf.collect().to_pandas()
    print(
        f"Pulled {len(cems_pd):,} hourly rows for {states} {final_year - num_years}-{final_year}"
    )

    unit_pairs = cems_pd[["plant_id_epa", "emissions_unit_id_epa"]].drop_duplicates()
    print(f"{len(unit_pairs)} plant-unit pairs")

    orig_rows = [
        build_op_char_df(
            cems_pd,
            row.plant_id_epa,
            row.emissions_unit_id_epa,
            min_stable_consecutive_hours,
        )
        for row in unit_pairs.itertuples()
    ]
    orig_df = pd.concat(orig_rows, ignore_index=True)

    new_df = estimate_operational_characteristics_by_unit(
        cems=cems_filtered_lf,
        min_stable_consecutive_hours=min_stable_consecutive_hours,
        adjusted=False,
    )
    if isinstance(new_df, pl.LazyFrame):
        new_df = new_df.collect()
    new_df = new_df.to_pandas()
    new_df["ramp_up_rate_fraction_of_max_gross_load_per_min"] = new_df[
        "ramp_up_rate_fraction_of_max_gross_load_per_min"
    ].round(4)
    new_df["ramp_down_rate_fraction_of_max_gross_load_per_min"] = new_df[
        "ramp_down_rate_fraction_of_max_gross_load_per_min"
    ].round(4)

    key = ["plant_id_epa", "emissions_unit_id_epa"]
    merged = orig_df.merge(
        new_df, on=key, how="outer", suffixes=("_orig", "_new"), indicator=True
    )

    print("\n=== Row presence (same input data for both) ===")
    print(merged["_merge"].value_counts())

    compare_cols = [
        "max_gross_load_mw",
        "min_stable_level",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_level_mmbtu_per_mwh",
        "ramp_up_rate_fraction_of_max_gross_load_per_min",
        "ramp_down_rate_fraction_of_max_gross_load_per_min",
    ]

    both = merged[merged["_merge"] == "both"].copy()
    print(f"\n=== Comparing {len(both)} plant-unit pairs, IDENTICAL input rows ===\n")

    summary_rows = []
    for col in compare_cols:
        o, n = both[f"{col}_orig"], both[f"{col}_new"]
        diff = (n - o).abs()
        rel = (diff / o.abs()).replace([float("inf")], pd.NA)
        n_null_mismatch = ((o.isna()) != (n.isna())).sum()
        summary_rows.append(
            {
                "column": col,
                "n_compared": diff.notna().sum(),
                "n_null_mismatch": n_null_mismatch,
                "max_abs_diff": diff.max(),
                "n_diff_gt_1pct": (rel > 0.01).sum(),
                "n_diff_gt_10pct": (rel > 0.10).sum(),
            }
        )

    summary = pd.DataFrame(summary_rows)
    pd.set_option("display.width", 160)
    print(summary.to_string(index=False))

    out_name = f"isolated_comparison_full_{'-'.join(states)}_{final_year - num_years}-{final_year}.csv"
    both.to_csv(HERE / out_name, index=False)
    print(f"\nWrote full row-by-row comparison to {out_name}")
    return 0


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--final-year",
    "-y",
    type=int,
    default=2025,
    show_default=True,
    help=(
        "Final EPA CEMS year to include -- matches the max_full_year logic in "
        "operational_characteristics_factory for the current data vintage."
    ),
)
@click.option(
    "--num-years",
    "-n",
    type=int,
    default=3,
    show_default=True,
    help="Number of years to include, counting back from --final-year.",
)
@click.option(
    "--states",
    "-s",
    default="CA",
    show_default=True,
    help="Comma-separated two-letter state abbreviations, e.g. 'CA,TX'.",
)
@click.option(
    "--min-stable-hours",
    "min_stable_consecutive_hours",
    type=int,
    default=8,
    show_default=True,
    help=(
        "Minimum consecutive operating hours in a load-factor bin for it to count "
        "as a stable operating level."
    ),
)
def cli(
    final_year: int, num_years: int, states: str, min_stable_consecutive_hours: int
):
    """Compare the original pandas and new polars op-char pipelines on identical rows.

    Pulls hourly EPA CEMS data for the requested years/states from
    $PUDL_OUTPUT/parquet/, runs both the original per-unit pandas algorithm and the
    new polars pipeline against it, and prints/writes a divergence summary.
    """
    return main(
        final_year=final_year,
        num_years=num_years,
        states=[s.strip() for s in states.split(",")],
        min_stable_consecutive_hours=min_stable_consecutive_hours,
    )


if __name__ == "__main__":
    sys.exit(cli())
