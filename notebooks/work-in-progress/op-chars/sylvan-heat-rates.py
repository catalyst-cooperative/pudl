import marimo

# Marimo cells are functions; docstrings, cell-scoped SCREAMING_SNAKE_CASE
# parameters/config constants, and a trailing bare expression (the cell's
# display value) are idiomatic here, not real problems. Cyclomatic complexity
# limits also don't fit: a single cell often bundles several small helpers
# together, inflating the count without reflecting real complexity.
# ruff: noqa: D100, D103, N803, N806, B018, C901

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Sylvan heat rates: old (pandas) vs. new (polars) operational-characteristics pipeline

    This notebook compares outputs from the original Sylvan heat rates script that relied on pandas (`pudl_thermal_characterization_2026-03-10.py`), and our new Dagsterized module that uses polars (`pudl.analysis.derived_plant_characteristics`). It runs the comparison **live**, against whatever EPA CEMS data is present under `$PUDL_OUTPUT/parquet/` on the machine that runs it (so make sure you've got some fresh CEMS outputs).

    Use the **state dropdown in the sidebar** to select any single EPA CEMS state; the whole notebook re-runs against just that state's data (we are not comparing across states).

    It covers:

    - What the algorithm computes, and how the two implementations differ structurally.
    - A same-input comparison of the old and new pipelines for the selected state, to separate real algorithmic divergence from data-vintage drift.
    - Explanations for where and why they disagree when possible.
    - Side-by-side comparisons of the two open algorithmic choices we're deciding between (load-factor binning method, ramp-rate binning method), so we can look at the actual data before picking one.
    - Measured speedup and memory savings from the vectorized polars binning path vs. the original per-unit pandas fallback.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What the script computes

    For every EPA CEMS emissions unit (`plant_id_epa` + `emissions_unit_id_epa`),
    three years of hourly gross-load and heat-input readings get turned into a
    handful of physically meaningful operating parameters that production-cost
    and capacity-expansion models need per generator, and that aren't reported
    anywhere in EIA-860:

    - `max_gross_load_mw` -- the unit's observed capacity.
    - `min_stable_level` -- the lowest load-factor bin the unit can sustain for
      a meaningful stretch (default: 8+ consecutive hours) without shutting down.
    - `min_up_time_hours` / `min_down_time_hours` -- shortest observed runs
      at/above the stable level, and shortest observed outages.
    - `heat_rate_at_max_load_factor` / `..._at_min_stable_level` -- fuel
      efficiency at full output vs. minimum stable output.
    - `ramp_up_rate_fraction_of_max_gross_load_per_min` /
      `ramp_down_rate_...` -- how fast the unit can change output.

    **Per unit**, the algorithm: (1) bins hourly load factor into 10
    *equal-width* bins spanning that unit's own observed min/max
    (`pandas.cut(bins=10, right=True, include_lowest=False)`); (2) walks the
    bins bottom-up, skipping the lowest, to find the first one with a run of
    at least 8 consecutive clock hours -- that's the stable level; (3) finds the
    shortest qualifying up-run and the shortest null-load-factor (outage) run;
    (4) takes median heat rate in the top bin and the stable bin; (5) splits
    hour-over-hour ramp rates into 20 quantile bins and takes the median of the
    extreme bins.

    ```python
    load_factor_bin = pd.cut(
        load_factor, bins=10, right=True, include_lowest=False
    )
    ```

    The **original script** does this with a Python `for` loop over every
    plant-unit pair, each iteration re-filtering a full in-memory pandas
    DataFrame -- O(units x rows). The **new module** vectorizes all five steps
    with polars group-by/window expressions across *all* units at once, using a
    fully expression-based replacement for the `pd.cut` binning step
    (`assign_load_factor_bins_vectorized`) that is now the
    **default** `load_factor_binning_method` -- the original per-unit pandas
    fallback (`assign_groupwise_load_factor_bins`) is still available alongside
    it for A/B comparison, but nothing depends on it by default anymore. That one
    change removes the only remaining per-unit Python loop from the pipeline
    (see "Performance" below for measured numbers), which alone is enough that
    looping over all 50 states inside a single Dagster asset -- rather than
    fanning out into 51 separate assets, each carrying its own Dagster
    scheduling/subprocess overhead -- turns out to be both simpler and faster.

    ### Why only ~245 EPA CEMS units in California?

    California has **4,101** EIA-860 generators as of the most recent report
    date, but only **245** EPA CEMS units. That's not a sampling artifact: EPA
    CEMS (Continuous Emissions Monitoring) is only required for fossil-fuel
    combustion units above certain Clean Air Act thresholds. It doesn't cover
    solar, wind, hydro, or most small units -- which make up the overwhelming
    majority of California's generator count. This analysis is fundamentally
    scoped to large fossil generators, by construction of the underlying data
    source, not a limitation of this pipeline.
    """)
    return


@app.cell
def _():
    import os
    import time
    import tracemalloc

    import marimo as mo
    import matplotlib.pyplot as plt
    import matplotx
    import numpy as np
    import pandas as pd
    import polars as pl

    import pudl.analysis.derived_plant_characteristics as opchar

    plt.style.use(matplotx.styles.onedark)

    PUDL_OUTPUT = os.environ["PUDL_OUTPUT"]
    CEMS_PARQUET_PATH = f"{PUDL_OUTPUT}/parquet/core_epacems__hourly_emissions.parquet"

    UNIT_COLS = ["plant_id_epa", "emissions_unit_id_epa"]
    FINAL_YEAR = 2025
    NUM_YEARS = 3
    MIN_STABLE_CONSECUTIVE_HOURS = 8

    mo.md(f"Reading EPA CEMS from `{CEMS_PARQUET_PATH}`.")
    return (
        CEMS_PARQUET_PATH,
        FINAL_YEAR,
        MIN_STABLE_CONSECUTIVE_HOURS,
        NUM_YEARS,
        UNIT_COLS,
        mo,
        np,
        opchar,
        pd,
        pl,
        plt,
        time,
        tracemalloc,
    )


@app.cell(hide_code=True)
def _(mo):
    ALL_STATES = [
        "AK",
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "PR",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    ]

    STATE = mo.ui.dropdown(
        options=ALL_STATES, value="CA", label="EPA CEMS state (2-letter)"
    )

    mo.sidebar(
        [
            mo.md(
                "### Sylvan heat rates: old vs. new\n\n"
                "Select a single EPA CEMS state. The whole notebook re-runs "
                "the old-vs-new comparison for that one state."
            ),
            STATE,
        ]
    )
    return (STATE,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Comparing the two pipelines: same inputs, not a naive CSV diff

    A first attempt at this comparison diffed the new pipeline against a
    frozen `epa_op_char_output_df.csv` the original script had produced at some
    earlier point against an S3 "nightly" snapshot. That was misleading: the
    underlying CEMS data has since been revised/extended, so even
    zero-logic columns like `max_gross_load_mw` (a plain `.max()`) disagreed.
    This notebook instead re-implements the original per-unit pandas algorithm
    **verbatim** (below -- the original script isn't importable without
    triggering S3 queries at module load time) and runs it against the exact
    same local-parquet rows handed to the new polars pipeline, so any
    disagreement reflects real algorithmic divergence, not data drift.
    """)
    return


@app.cell(hide_code=True)
def _(MIN_STABLE_CONSECUTIVE_HOURS, UNIT_COLS, np, pd):
    def _min_stable_level(hourly_plant_unit_df, consecutive_hours):
        """Smallest bin with >= consecutive_hours consecutive operating hours."""
        d = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
        bins = sorted(d["load_factor_bin"].dropna().unique())
        for candidate_bin in bins[1:]:
            sub = d[d["load_factor_bin"] == candidate_bin]
            run_id = (
                sub["operating_datetime_utc"]
                .diff()
                .dt.total_seconds()
                .div(3600)
                .ne(1)
                .cumsum()
            )
            if sub.groupby(run_id).size().max() >= consecutive_hours:
                return candidate_bin, candidate_bin.left.max()
        return None, np.nan

    def _min_up_down_times(hourly_plant_unit_df, min_stbl_lvl):
        # Original script's `>=` comparison against a pandas Categorical raises
        # TypeError when no bin qualifies as stable (min_stbl_lvl is None) --
        # verified against real CT CEMS data, where it hits 27/67 units. Original
        # script never surfaced this because it was never run against a state
        # where that many units lack a qualifying stable bin. `_heat_rate` avoids
        # the same crash only because `==` (unlike `>=`) against None returns an
        # empty, not an error -- so NaN is the behavior this mirrors.
        if min_stbl_lvl is None:
            return np.nan, np.nan

        d = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()

        up = d[d["load_factor_bin"] >= min_stbl_lvl]
        up_run_id = (
            up["operating_datetime_utc"]
            .diff()
            .dt.total_seconds()
            .div(3600)
            .ne(1)
            .cumsum()
        )
        min_up_time = up.groupby(up_run_id).size().min()

        down = d[d["load_factor"].isna()]
        down_run_id = (
            down["operating_datetime_utc"]
            .diff()
            .dt.total_seconds()
            .div(3600)
            .ne(1)
            .cumsum()
        )
        min_down_time = down.groupby(down_run_id).size().min()

        return min_up_time, min_down_time

    def _heat_rate(hourly_plant_unit_df, min_stbl_lvl):
        d = hourly_plant_unit_df.dropna(
            subset=["load_factor", "heat_rate_mmbtu_per_MWh"]
        )
        max_lf_bin = d["load_factor_bin"].max()
        max_lf_hr = d.loc[
            d["load_factor_bin"] == max_lf_bin, "heat_rate_mmbtu_per_MWh"
        ].median()
        min_stbl_hr = d.loc[
            d["load_factor_bin"] == min_stbl_lvl, "heat_rate_mmbtu_per_MWh"
        ].median()
        return max_lf_hr, min_stbl_hr

    def _ramp_rate(hourly_plant_unit_df):
        d = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
        time_delta = d["operating_datetime_utc"].diff().dt.total_seconds().div(3600)
        mwh_delta = d["gross_load_MWh"].diff()
        d["ramp_rate"] = mwh_delta / time_delta
        d = d.dropna(subset=["ramp_rate"])
        d["ramp_rate_bin"] = pd.qcut(d["ramp_rate"], q=20, duplicates="drop")
        low_bin, high_bin = d["ramp_rate_bin"].min(), d["ramp_rate_bin"].max()
        ramp_down_rate = d.loc[d["ramp_rate_bin"] == low_bin, "ramp_rate"].median()
        ramp_up_rate = d.loc[d["ramp_rate_bin"] == high_bin, "ramp_rate"].median()
        return ramp_up_rate, ramp_down_rate

    def build_op_char_df_original(
        epa_cems_df: pd.DataFrame, plant_id, unit_id
    ) -> pd.DataFrame:
        """Original script's per-unit algorithm, copied verbatim (not imported)."""
        d = epa_cems_df[
            (epa_cems_df["plant_id_epa"] == plant_id)
            & (epa_cems_df["emissions_unit_id_epa"] == unit_id)
        ].copy()
        max_gross_load_mw = d["gross_load_mw"].max()
        d["load_factor"] = d["gross_load_mw"] / max_gross_load_mw
        d["gross_load_MWh"] = d["gross_load_mw"] * d["operating_time_hours"]
        d["heat_rate_mmbtu_per_MWh"] = d["heat_content_mmbtu"] / d["gross_load_MWh"]
        d["operating_datetime_utc"] = pd.to_datetime(d["operating_datetime_utc"])
        valid_load_factors = d["load_factor"].dropna().to_numpy()

        if (
            valid_load_factors.shape[0] > 0
            and not (valid_load_factors[0] == valid_load_factors).all()
        ):
            d["load_factor_bin"] = pd.cut(
                d["load_factor"], bins=10, right=True, include_lowest=False
            )
            min_stbl_lvl_bin, min_stbl_lvl = _min_stable_level(
                d, MIN_STABLE_CONSECUTIVE_HOURS
            )
            max_lf_hr, min_stbl_hr = _heat_rate(d, min_stbl_lvl_bin)
            min_up_time, min_down_time = _min_up_down_times(d, min_stbl_lvl_bin)
            ramp_up_rate, ramp_down_rate = _ramp_rate(d)
        else:
            min_stbl_lvl = max_lf_hr = min_stbl_hr = np.nan
            min_up_time = min_down_time = ramp_up_rate = ramp_down_rate = np.nan

        return pd.DataFrame(
            {
                "plant_id_epa": [plant_id],
                "emissions_unit_id_epa": [unit_id],
                "max_gross_load_mw": [max_gross_load_mw],
                "min_stable_level": [min_stbl_lvl],
                "min_up_time_hours": [min_up_time],
                "min_down_time_hours": [min_down_time],
                "heat_rate_at_max_load_factor_mmbtu_per_mwh": [max_lf_hr],
                "heat_rate_at_min_stable_level_mmbtu_per_mwh": [min_stbl_hr],
                "ramp_up_rate_fraction_of_max_gross_load_per_min": [
                    ramp_up_rate / max_gross_load_mw / 60
                    if pd.notna(ramp_up_rate)
                    else np.nan
                ],
                "ramp_down_rate_fraction_of_max_gross_load_per_min": [
                    ramp_down_rate / max_gross_load_mw / 60
                    if pd.notna(ramp_down_rate)
                    else np.nan
                ],
            }
        )

    def run_original_pipeline(cems_pd: pd.DataFrame) -> pd.DataFrame:
        """Run the original per-unit algorithm over every plant-unit pair present."""
        pairs = cems_pd[UNIT_COLS].drop_duplicates()
        rows = [
            build_op_char_df_original(
                cems_pd, row.plant_id_epa, row.emissions_unit_id_epa
            )
            for row in pairs.itertuples()
        ]
        return pd.concat(rows, ignore_index=True)

    return (run_original_pipeline,)


@app.cell(hide_code=True)
def _(
    CEMS_PARQUET_PATH,
    FINAL_YEAR,
    MIN_STABLE_CONSECUTIVE_HOURS,
    NUM_YEARS,
    UNIT_COLS,
    opchar,
    pd,
    pl,
    run_original_pipeline,
    time,
):
    def _to_df(frame):
        return frame.collect() if isinstance(frame, pl.LazyFrame) else frame

    def load_filtered_cems(state: str) -> tuple[pl.LazyFrame, pd.DataFrame]:
        """Filtered hourly CEMS for one state, as both a LazyFrame and a pandas DataFrame."""
        cems_lf = pl.scan_parquet(CEMS_PARQUET_PATH)
        filtered_lf = opchar.filter_cems_for_heat_rate_analysis(
            core_epacems__hourly_emissions=cems_lf,
            final_year=FINAL_YEAR,
            num_years=NUM_YEARS,
            states=[state],
        )
        filtered_pd = filtered_lf.collect().to_pandas()
        return filtered_lf, filtered_pd

    def run_new_pipeline(
        cems_lf: pl.LazyFrame,
        load_factor_binning_method: str = "pandas_cut",
        ramp_rate_binning_method: str = "rank_split",
    ) -> tuple[pd.DataFrame, float]:
        """Run the new polars pipeline once, returning (result, elapsed_seconds)."""
        start = time.perf_counter()
        result = opchar.estimate_operational_characteristics_by_unit(
            cems=cems_lf,
            min_stable_consecutive_hours=MIN_STABLE_CONSECUTIVE_HOURS,
            load_factor_binning_method=load_factor_binning_method,
            ramp_rate_binning_method=ramp_rate_binning_method,
        )
        result_pd = _to_df(result).to_pandas()
        elapsed = time.perf_counter() - start
        return result_pd, elapsed

    BINNING_CONFIGS = {
        "pandas_cut__rank_split": ("pandas_cut", "rank_split"),  # current default
        "vectorized__rank_split": ("vectorized", "rank_split"),
        "pandas_cut__qcut": ("pandas_cut", "qcut"),
        "vectorized__qcut": ("vectorized", "qcut"),
    }

    def run_all_configs_for_state(state: str) -> dict:
        """Run the original algorithm plus all four new-pipeline binning configs."""
        cems_lf, cems_pd = load_filtered_cems(state)

        start = time.perf_counter()
        original_df = run_original_pipeline(cems_pd)
        original_elapsed = time.perf_counter() - start

        new_results = {}
        new_timings = {}
        for config_name, (lf_method, rr_method) in BINNING_CONFIGS.items():
            df, elapsed = run_new_pipeline(cems_lf, lf_method, rr_method)
            new_results[config_name] = df
            new_timings[config_name] = elapsed

        return {
            "state": state,
            "n_hourly_rows": len(cems_pd),
            "n_units": cems_pd[UNIT_COLS].drop_duplicates().shape[0],
            "original_df": original_df,
            "original_elapsed": original_elapsed,
            "new_results": new_results,
            "new_timings": new_timings,
        }

    return load_filtered_cems, run_all_configs_for_state


@app.cell(hide_code=True)
def _(STATE, mo):
    mo.md(f"""
    ## Running the comparison: {STATE.value}
    """)
    return


@app.cell(hide_code=True)
def _(STATE, mo, run_all_configs_for_state):
    result = run_all_configs_for_state(STATE.value)
    mo.md(
        f"**{STATE.value}**: {result['n_units']} plant-unit pairs, {result['n_hourly_rows']:,} hourly rows. "
        f"Original per-unit pandas algorithm: {result['original_elapsed']:.1f}s."
    )
    return (result,)


@app.cell(hide_code=True)
def _(UNIT_COLS, np, pd, plt):
    METRICS = [
        "max_gross_load_mw",
        "min_stable_level",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_level_mmbtu_per_mwh",
        "ramp_up_rate_fraction_of_max_gross_load_per_min",
        "ramp_down_rate_fraction_of_max_gross_load_per_min",
    ]

    _MATCH_COLOR = "#61afef"  # onedark blue
    _DIVERGE_COLOR = "#e06c75"  # onedark red

    def merged_for_comparison(
        df_x: pd.DataFrame, df_y: pd.DataFrame, x_label: str, y_label: str
    ) -> pd.DataFrame:
        """Inner-join two op-char result DataFrames on unit_cols for scatter comparison."""
        return df_x.merge(df_y, on=UNIT_COLS, suffixes=(f"_{x_label}", f"_{y_label}"))

    def plot_comparison_grid(
        merged: pd.DataFrame,
        x_label: str,
        y_label: str,
        title: str,
        subtitle: str = "",
        metrics: list[str] = METRICS,
        diverge_threshold: float = 0.10,
    ):
        """Grid (<=4 wide) of square 1:1 scatter plots, one per metric.

        Each panel shares a single axis range (min/max across both series) so the
        "matches exactly" case is a clean diagonal from the origin.
        """
        n = len(metrics)
        ncols = min(4, n)
        nrows = -(-n // ncols)
        fig, axes = plt.subplots(
            nrows, ncols, figsize=(7 * ncols, 7 * nrows), squeeze=False
        )
        axes = axes.flatten()

        # axes is padded to a full nrows*ncols grid, so it can be longer than metrics.
        for ax, metric in zip(axes, metrics, strict=False):
            xcol, ycol = f"{metric}_{x_label}", f"{metric}_{y_label}"
            sub = merged[[xcol, ycol]].dropna()
            if sub.empty:
                ax.set_title(f"{metric}\n(no valid data)")
                continue

            lo = float(min(sub[xcol].min(), sub[ycol].min()))
            hi = float(max(sub[xcol].max(), sub[ycol].max()))
            pad = (hi - lo) * 0.05 or max(abs(hi), 1.0) * 0.05
            lo, hi = lo - pad, hi + pad

            denom = sub[xcol].abs().to_numpy()
            denom = np.where(denom == 0, np.nan, denom)
            rel_diff = (sub[ycol] - sub[xcol]).abs().to_numpy() / denom
            diverges = rel_diff > diverge_threshold
            colors = np.where(diverges, _DIVERGE_COLOR, _MATCH_COLOR)

            ax.plot(
                [lo, hi],
                [lo, hi],
                linestyle="--",
                linewidth=1.5,
                color="#5c6370",
                zorder=1,
            )
            ax.scatter(
                sub[xcol],
                sub[ycol],
                s=60,
                alpha=0.75,
                c=colors,
                edgecolors="none",
                zorder=2,
            )
            ax.set_xlim(lo, hi)
            ax.set_ylim(lo, hi)
            ax.set_aspect("equal", adjustable="box")
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(
                f"{metric}\n{diverges.sum()}/{len(sub)} diverge > {diverge_threshold:.0%}"
            )

        for ax in axes[n:]:
            ax.axis("off")

        fig.suptitle(title, fontsize=20, y=1.06)
        if subtitle:
            fig.text(0.5, 1.005, subtitle, ha="center", fontsize=13, color="#abb2bf")
        fig.tight_layout(rect=[0, 0, 1, 0.97])
        return fig

    return METRICS, merged_for_comparison, plot_comparison_grid


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Old vs. new, same input rows: the state selected in the sidebar

    Both pipelines run against the exact same locally-filtered CEMS rows for the
    state selected in the sidebar. The new-pipeline results shown here use the
    config that most closely matches the original script
    (`pandas_cut` + `qcut`) -- the baseline for "did we faithfully replicate the
    script", before layering on the newer defaults. Points on the dashed diagonal
    agree exactly; red points diverge by more than 10%.
    """)
    return


@app.cell(hide_code=True)
def _(
    FINAL_YEAR,
    NUM_YEARS,
    STATE,
    merged_for_comparison,
    plot_comparison_grid,
    result,
):
    _default = result["new_results"]["pandas_cut__qcut"]
    _oldnew_merged = merged_for_comparison(
        result["original_df"], _default, "original", "new"
    )
    fig_oldnew = plot_comparison_grid(
        _oldnew_merged,
        "original",
        "new",
        f"{STATE.value}: original (pandas) vs. new (polars), closest-matching config",
        subtitle=f"{result['n_units']} plant-unit pairs, {FINAL_YEAR - NUM_YEARS}-{FINAL_YEAR}",
    )
    fig_oldnew
    return


@app.cell(hide_code=True)
def _(METRICS, STATE, merged_for_comparison, np, pd, plt, result):
    def divergence_summary(
        merged: pd.DataFrame, x_label: str, y_label: str, metrics: list[str] = METRICS
    ) -> pd.DataFrame:
        rows = []
        for metric in metrics:
            xcol, ycol = f"{metric}_{x_label}", f"{metric}_{y_label}"
            sub = merged[[xcol, ycol]].dropna()
            denom = sub[xcol].abs().to_numpy()
            denom = np.where(denom == 0, np.nan, denom)
            rel_diff = (sub[ycol] - sub[xcol]).abs().to_numpy() / denom
            rows.append(
                {
                    "metric": metric,
                    "share_gt_1pct": np.nanmean(rel_diff > 0.01),
                    "share_gt_10pct": np.nanmean(rel_diff > 0.10),
                }
            )
        return pd.DataFrame(rows).set_index("metric")

    def plot_divergence_bars(summaries: dict[str, pd.DataFrame], title: str):
        """summaries: {state_label: divergence_summary_df}, side by side per metric."""
        states = list(summaries.keys())
        metrics = summaries[states[0]].index.tolist()
        fig, axes = plt.subplots(
            1, len(states), figsize=(11 * len(states), 8), sharex=True
        )
        if len(states) == 1:
            axes = [axes]
        y = np.arange(len(metrics))
        bar_h = 0.35
        for ax, state in zip(axes, states, strict=True):
            df = summaries[state]
            ax.barh(
                y - bar_h / 2,
                df["share_gt_1pct"],
                height=bar_h,
                color="#e5c07b",
                label="> 1% relative diff",
            )
            ax.barh(
                y + bar_h / 2,
                df["share_gt_10pct"],
                height=bar_h,
                color="#e06c75",
                label="> 10% relative diff",
            )
            ax.set_yticks(y)
            ax.set_yticklabels(metrics)
            ax.set_xlabel("Share of plant-unit pairs")
            ax.set_xlim(0, 1)
            ax.xaxis.set_major_formatter(lambda x, _: f"{x:.0%}")
            ax.set_title(state)
            ax.invert_yaxis()
            ax.legend(loc="lower right")
        fig.suptitle(title, fontsize=20, y=1.02)
        fig.tight_layout()
        return fig

    oldnew_merged = merged_for_comparison(
        result["original_df"],
        result["new_results"]["pandas_cut__qcut"],
        "original",
        "new",
    )
    _oldnew_summary = divergence_summary(oldnew_merged, "original", "new")
    fig_divergence_summary = plot_divergence_bars(
        {STATE.value: _oldnew_summary},
        "Where do the two pipelines disagree most? (closest-matching config, same input rows)",
    )
    fig_divergence_summary
    return divergence_summary, plot_divergence_bars


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Why do `min_stable_level`, `min_up_time_hours`, and `min_down_time_hours`
    show identical >1% and >10% bars?** Not a plotting bug -- these columns are
    quantized. `min_up_time_hours`/`min_down_time_hours` are counts of whole
    hours, and the runs involved are typically short (single digits to low tens
    of hours), so the smallest possible nonzero disagreement -- off by one hour --
    is already a large relative difference (e.g. 1 hour off on an 8-hour run is
    12.5%). There's no way to land a discrepancy in the 1-10% range with counts
    that small, so every nonzero disagreement clears both thresholds and the two
    bars end up the same height. `min_stable_level` shows *zero* height for both
    because it's a continuous bin-edge value computed identically by both
    pipelines in this comparison (same `pandas_cut` binning method on both
    sides). The >1%/>10% split is informative for continuous ratios (heat rates,
    ramp rates) but not very meaningful for these two discrete columns.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Why do they disagree, and is it expected/acceptable?

    Because the "Old vs. new" baseline above uses the config that most closely
    matches the original script (`pandas_cut` + `qcut`), the large
    ramp-rate methodology gap is **not** the main story here -- it shows up
    instead in the "ramp-rate binning: rank_split vs. qcut" section below. What
    remains in the old-vs-new baseline is therefore the genuinely-implementation
    divergence between the verbatim per-unit pandas reference and the vectorized
    polars pipeline:

    **`min_up_time_hours` / `min_down_time_hours`:** found and fixed one small
    bug in `_add_run_id` -- but it turned out not to have much impact.
    `_add_run_id` built its "same unit/bin and consecutive hour" check with
    `pl.col(c).eq(pl.col(c).shift())`, which evaluates to `null` (not
    `True`/`False`) for the very first row of whatever frame it's called on --
    there's nothing to shift from. Its sibling helper, `consecutive_run_ids()`,
    guards against exactly this with `.fill_null(True)`; `_add_run_id` didn't,
    so `cum_sum()` propagated that null instead of starting run 0, splitting the
    first row of a run off into its own spurious length-1 "run" for whichever
    plant-unit happened to sort first.

    Re-running the same-input comparison *after* the fix produced identical
    divergence counts to before it. This bug can only ever affect one unit per
    pipeline invocation, so it can't explain 7+ mismatched units out of 240. The
    real cause of most of these mismatches is still open. One lead:
    `filter_for_min_stable_bin` compares `load_factor_bin`'s struct fields against
    `min_stable_bin`'s with `>=`, which should be equivalent to an ordinal
    comparison for well-formed bins, but floating-point precision differences from
    the pipeline's joins/casts could make a numerically-identical row fail that
    comparison by a hair -- several of the mismatched units show a suspicious "off
    by exactly 1 hour" pattern consistent with this, but it hasn't been confirmed.

    **Ramp rates: a small, expected residual under the closest-matching
    (`qcut`) config.** The original script passed *all* ramp-rate observations
    (including `±inf` from zero-width time deltas) to `pd.qcut`; the new
    pipeline drops non-finite ramp rates before binning, and it no longer rounds
    heat-rate/ramp outputs to 2 decimal places. So even with `qcut`, a handful of
    units can differ at the margins. The *big* ramp-rate story -- `rank_split`
    vs. `qcut` being genuinely different definitions of "extreme ramp rate" --
    is shown separately below.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The two algorithmic choices we're deciding between

    `derived_plant_characteristics.py` now exposes both choices as A/B-testable
    config flags (`load_factor_binning_method`, `ramp_rate_binning_method`),
    both defaulting to the original behavior. Below: the new-vs-new comparison
    for each choice, so we can look at real data before deciding.

    ### Load-factor binning: `pandas_cut` (current fallback) vs. `vectorized`

    `vectorized` replaces the per-unit `pandas.cut` fallback with a fully
    expression-based polars equivalent -- no per-unit Python loop, and (after
    fixing a bug caught while building it, where the first version padded every
    bin instead of just the lowest one) it should be numerically
    interchangeable with `pandas_cut`. If the panels below show a clean
    diagonal, that's the empirical case for switching.
    """)
    return


@app.cell(hide_code=True)
def _(STATE, merged_for_comparison, plot_comparison_grid, result):
    binning_merged = merged_for_comparison(
        result["new_results"]["pandas_cut__rank_split"],
        result["new_results"]["vectorized__rank_split"],
        "pandas_cut",
        "vectorized",
    )
    fig_binning_choice = plot_comparison_grid(
        binning_merged,
        "pandas_cut",
        "vectorized",
        f"{STATE.value}: load-factor binning method, pandas_cut vs. vectorized",
        subtitle="Same ramp-rate method (rank_split) in both -- isolating just the binning choice",
    )
    fig_binning_choice
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ramp-rate binning: `rank_split` (current new-pipeline default) vs. `qcut` (reproduces original script)

    Unlike the load-factor binning choice above, this one is expected to show
    real scatter -- `qcut(duplicates="drop")` and rank-based top/bottom-5% are
    genuinely different definitions of "extreme ramp rate" whenever there are
    tied values, which is the normal case (many hours have zero ramp). This
    only affects the two ramp-rate columns; everything else in the pipeline is
    identical between these two runs.
    """)
    return


@app.cell(hide_code=True)
def _(STATE, merged_for_comparison, plot_comparison_grid, result):
    ramp_merged = merged_for_comparison(
        result["new_results"]["pandas_cut__rank_split"],
        result["new_results"]["pandas_cut__qcut"],
        "rank_split",
        "qcut",
    )
    RAMP_METRICS = [
        "ramp_up_rate_fraction_of_max_gross_load_per_min",
        "ramp_down_rate_fraction_of_max_gross_load_per_min",
    ]
    fig_ramp_choice = plot_comparison_grid(
        ramp_merged,
        "rank_split",
        "qcut",
        f"{STATE.value}: ramp-rate binning method, rank_split vs. qcut",
        subtitle="Same load-factor binning (pandas_cut) in both -- isolating just the ramp-rate choice",
        metrics=RAMP_METRICS,
    )
    fig_ramp_choice
    return RAMP_METRICS, ramp_merged


@app.cell(hide_code=True)
def _(
    RAMP_METRICS,
    STATE,
    divergence_summary,
    plot_divergence_bars,
    ramp_merged,
):
    ramp_choice_summary = divergence_summary(
        ramp_merged, "rank_split", "qcut", metrics=RAMP_METRICS
    )
    fig_ramp_divergence_summary = plot_divergence_bars(
        {STATE.value: ramp_choice_summary},
        "Ramp-rate binning: how much do rank_split and qcut disagree? (same load-factor binning in both)",
    )
    fig_ramp_divergence_summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Performance: quantifying the pandas bottleneck

    The one remaining per-unit pandas step in the new pipeline is the
    load-factor binning fallback (`assign_groupwise_load_factor_bins`), which
    forces a full eager `.collect().to_pandas()` and a `groupby().apply(pd.cut)`
    Python loop over every unit. `assign_load_factor_bins_vectorized` replaces
    it with pure polars expressions. Isolating just that one function call
    (same prepared input both times, 5 repetitions, median reported) on real
    CEMS data:
    """)
    return


@app.cell(hide_code=True)
def _(STATE, UNIT_COLS, load_filtered_cems, np, opchar, pd, time, tracemalloc):
    def benchmark_binning_step(state: str, n_reps: int = 5) -> dict:
        cems_lf, _ = load_filtered_cems(state)
        cems_working, col_dict = opchar.handle_adjustment_in_cems(
            cems_lf, UNIT_COLS, adjusted=False
        )
        load_factor_col = col_dict["load_factor_col"]
        # Materialize the pre-binning frame once so both methods see identical, already-collected input.
        cems_working = cems_working.collect().lazy()

        results = {}
        for method_name, fn in [
            ("pandas_cut", opchar.assign_groupwise_load_factor_bins),
            ("vectorized", opchar.assign_load_factor_bins_vectorized),
        ]:
            timings = []
            peak_mem_bytes = []
            for _ in range(n_reps):
                tracemalloc.start()
                start = time.perf_counter()
                _ = fn(
                    cems_working=cems_working,
                    unit_cols=UNIT_COLS,
                    load_factor_col=load_factor_col,
                )
                timings.append(time.perf_counter() - start)
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                peak_mem_bytes.append(peak)
            results[method_name] = {
                "median_seconds": float(np.median(timings)),
                "all_seconds": timings,
                "median_peak_mb": float(np.median(peak_mem_bytes)) / 1e6,
            }
        return results

    binning_bench = benchmark_binning_step(STATE.value)

    pd.DataFrame(
        {
            method: {
                "median_seconds": r["median_seconds"],
                "median_peak_MB": r["median_peak_mb"],
            }
            for method, r in binning_bench.items()
        }
    ).T
    return (binning_bench,)


@app.cell(hide_code=True)
def _(STATE, binning_bench, np, plt):
    def plot_binning_benchmark(bench: dict, title: str):
        methods = ["pandas_cut", "vectorized"]
        fig, (ax_time, ax_mem) = plt.subplots(1, 2, figsize=(16, 7))
        colors = {"pandas_cut": "#e06c75", "vectorized": "#61afef"}
        x = np.arange(len(methods))

        times = [bench[m]["median_seconds"] for m in methods]
        bars = ax_time.bar(
            x, times, width=0.55, color=[colors[m] for m in methods], label=methods
        )
        ax_time.bar_label(bars, fmt="%.1fs", padding=3)
        ax_time.set_xticks(x)
        ax_time.set_xticklabels(methods)
        ax_time.set_ylabel("Median wall-clock seconds (5 reps)")
        ax_time.set_title("Time: assign_groupwise_load_factor_bins")

        mems = [bench[m]["median_peak_mb"] for m in methods]
        bars = ax_mem.bar(
            x, mems, width=0.55, color=[colors[m] for m in methods], label=methods
        )
        ax_mem.bar_label(bars, fmt="%.1f MB", padding=3)
        ax_mem.set_xticks(x)
        ax_mem.set_yscale("log")
        ax_mem.set_xticklabels(methods)
        ax_mem.set_ylabel("Median peak Python-heap allocation, MB (log scale)")
        ax_mem.set_title("Peak memory (tracemalloc): assign_groupwise_load_factor_bins")

        fig.suptitle(title, fontsize=20, y=1.05)
        fig.tight_layout(rect=[0, 0, 1, 0.95])
        return fig

    fig_binning_benchmark = plot_binning_benchmark(
        binning_bench,
        STATE.value,
        f"pandas_cut vs. vectorized: the one remaining pandas bottleneck, isolated ({STATE.value})",
    )
    fig_binning_benchmark
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Caveat on the memory numbers:** `tracemalloc` only tracks Python-heap
    allocations, not polars' native (Rust-side) memory. That actually favors
    this comparison in an interesting way: it means the multi-hundred-MB to
    ~1GB peaks reported for `pandas_cut` are *real, additional* Python-object
    memory the pandas fallback allocates on top of whatever polars itself
    uses underneath both paths -- from `.collect().to_pandas()` materializing
    the full filtered CEMS frame plus the per-unit `groupby().apply(pd.cut)`
    intermediate objects. The `vectorized` path allocates almost nothing on
    the Python heap because the binning computation stays inside polars'
    expression engine the whole time. The *relative* comparison (pandas_cut
    uses dramatically more Python-heap memory) is trustworthy even though the
    absolute vectorized numbers likely undercount its true (mostly native)
    memory use.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Test coverage

    `tests/unit/analysis/derived_plant_characteristics_test.py` (14 tests,
    `pixi run pytest --no-cov tests/unit/analysis/`) backs up the empirical
    comparisons above with fast, synthetic-fixture unit tests: run-length
    logic, both load-factor binning methods against each other and against
    hand-verified `pandas.cut` edge cases (including a regression test that
    `pandas.cut` only pads the lowest bin edge, not all ten), both ramp-rate
    binning methods (including a regression test for the tied-deltas divergence
    shown above), two regression tests for the `_add_run_id` first-row bug (one
    demonstrating the fix, one covering the Struct-column call shape that an
    earlier, incorrect fix attempt crashed on), and an end-to-end 3-unit fixture
    covering the stable/too-short/constant-load branches of the pipeline.

    ## Status

    1. **`_add_run_id`'s first-row bug is fixed** (`.fill_null(True)` moved onto
       the final combined expression, mirroring `consecutive_run_ids()`), and
       covered by regression tests. It turned out *not* to be the dominant cause
       of the `min_up_time_hours`/`min_down_time_hours` divergence shown above --
       see "Why do they disagree" above for the corrected analysis.
    2. **`assign_load_factor_bins_vectorized` is now the default
       `load_factor_binning_method`.** The empirical case is about as clean as it
       gets: every metric matches to floating-point precision, it's 10-20x
       faster, and it uses orders of magnitude less Python-heap memory -- with no
       methodology trade-off to weigh. (A fully-lazy-end-to-end version of the
       pipeline was also tried, but reverted as too invasive a change for the
       resource savings it bought -- both binning methods still collect to an
       eager DataFrame right after binning, same as before.) The single-asset-loop
       version of the Dagster asset
       (`out_epacems__yearly_operational_characteristics_single_asset`) is the
       direction going forward in place of the 51-asset fan-out, mainly because
       it avoids the fan-out's per-asset Dagster scheduling overhead.
    3. **The ramp-rate binning methodology is still an open decision.**
       `rank_split` (now the default) and `qcut` are both defensible, but they
       are not the same definition, and the gap is real (visible above, not just
       theoretical). This is a product/methodology decision for the team, not an
       engineering one -- worth a short, explicit discussion rather than
       defaulting to whichever one happens to be more convenient to compute.
    4. Once that's settled, retire the original per-unit pandas script and
       generalize the pipeline to all 50 states and (per the project's
       longer-term goal) additional years.

    See `notebooks/work-in-progress/sylvan-heat-rates.md` for additional
    readability/maintainability suggestions (two dead functions, naming, a
    repeated join-coalesce pattern worth factoring out) that don't affect
    correctness and weren't reproduced here.
    """)
    return


if __name__ == "__main__":
    app.run()
