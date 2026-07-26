import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import altair as alt
    import polars as pl
    from upath import UPath

    PUDL_OUTPUT = Path(os.environ["PUDL_OUTPUT"])
    LOCAL_PARQUET = PUDL_OUTPUT / "parquet"
    NIGHTLY_BASE = UPath("s3://pudl.catalyst.coop/nightly", anon=True)

    TABLE_BSA = "out_ferc1__yearly_detailed_balance_sheet_assets"
    TABLE_RB = "out_ferc1__yearly_rate_base"


@app.cell
def intro():
    import marimo as mo

    mo.md(
        r"""
        # Why do `out_ferc1__yearly_detailed_balance_sheet_assets` and `out_ferc1__yearly_rate_base` change on this branch?

        On the `simplify-dtypes` branch we switched PUDL's numeric outputs from
        32-bit to 64-bit types, standardized timestamp resolution, and moved from
        constrained Enum types to unconstrained Categorical types. Two FERC1 output
        tables came out of `dbt_helper validate` with unexpected differences from
        the `nightly` (main-branch) build:

        - **`out_ferc1__yearly_detailed_balance_sheet_assets`**: 0.4-0.6% more rows
          in *every* year, with no rows missing.
        - **`out_ferc1__yearly_rate_base`** (built downstream of the table above):
          the `ending_balance` column sums to a different value in every year.

        This notebook is a self-contained walkthrough of how we tracked the
        difference down to a single root cause, verified it, and chose a fix.
        **Bottom line up front:** this was not a regression introduced by the
        dtype changes -- it was a pre-existing bug (matching floating point values
        for *exact* equality) that the dtype changes happened to expose. The extra
        rows are legitimate data that the `nightly` build was silently missing.

        ## Requirements to run this notebook

        - A local `$PUDL_OUTPUT` directory containing this branch's materialized
          Parquet outputs for the two tables above (e.g. via
          `dg launch --assets "out_ferc1__yearly_detailed_balance_sheet_assets" --config src/pudl/package_data/settings/dg_full.yml`,
          and similarly for `out_ferc1__yearly_rate_base`).
        - Network access to `s3://pudl.catalyst.coop/nightly`, to pull the
          comparable `main`-branch outputs.
        """
    )
    return (mo,)


@app.cell
def symptom_intro(mo):
    mo.md(r"""
    ## 1. Confirm and quantify the symptom

    `dbt_helper validate` flagged row-count differences in
    `out_ferc1__yearly_detailed_balance_sheet_assets` in every year. Loading
    the local (this branch) and nightly (main) Parquet files directly
    confirms it: **local has more rows, and only more -- never fewer, in any
    year we checked.**
    """)
    return


@app.cell
def load_symptom():
    local_bsa = pl.read_parquet(LOCAL_PARQUET / f"{TABLE_BSA}.parquet")
    nightly_bsa = pl.read_parquet((NIGHTLY_BASE / f"{TABLE_BSA}.parquet").read_bytes())

    row_diff = len(local_bsa) - len(nightly_bsa)
    pl.DataFrame(
        {
            "output": ["local (this branch)", "nightly (main)", "difference"],
            "row_count": [len(local_bsa), len(nightly_bsa), row_diff],
        }
    )
    return local_bsa, nightly_bsa


@app.cell
def group_compare(local_bsa, nightly_bsa):
    GROUP_COLS = [
        "report_year",
        "table_name",
        "xbrl_factoid",
        "utility_type",
        "plant_function",
        "plant_status",
        "rate_base_category",
        "in_rate_base",
    ]

    def normalize(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col("report_year").cast(pl.Int64),
            pl.col("rate_base_category").cast(pl.String),
        )

    def agg_counts(df: pl.DataFrame) -> pl.DataFrame:
        return (
            normalize(df)
            .group_by(GROUP_COLS)
            .agg(
                n_rows=pl.len(),
                sum_ending_balance=pl.col("ending_balance").sum(),
            )
        )

    local_agg = agg_counts(local_bsa)
    nightly_agg = agg_counts(nightly_bsa)

    joined = local_agg.join(
        nightly_agg, on=GROUP_COLS, how="full", suffix="_nightly", nulls_equal=True
    )
    only_local = joined.filter(pl.col("n_rows_nightly").is_null())
    only_nightly = joined.filter(pl.col("n_rows").is_null())

    pl.DataFrame(
        {
            "": ["dimension-groups only in local", "dimension-groups only in nightly"],
            "count": [len(only_local), len(only_nightly)],
        }
    )
    return (only_local,)


@app.cell
def sample_narrative(mo):
    mo.md(r"""
    ## 2. What do the extra groups have in common?

    All 42 "only in local" groups come from **`core_ferc1__yearly_utility_plant_summary_sched200`**,
    `utility_type = electric`, `plant_status = in_service`, spread across most
    years -- and the `xbrl_factoid` values are not ordinary reported line
    items. They're synthetic **"off by" correction facts**:
    """)
    return


@app.cell
def sample_only_local(only_local):
    only_local.sort("report_year").select(
        pl.col("report_year").cast(pl.String),
        "xbrl_factoid",
        "utility_type",
        "n_rows",
        pl.col("sum_ending_balance").round(0).cast(pl.Int64),
    )
    return


@app.cell
def root_cause_narrative(mo):
    mo.md(r"""
    ## 3. Root cause: an exact floating-point equality merge

    These correction facts are generated by
    `Exploder.add_sizable_minority_corrections()` in `src/pudl/output/ferc1.py`.
    It exists to patch up a "sizable minority" of utilities that report a
    calculated factoid using a different set of subcomponents than everyone
    else. Three `OffByFactoid` rules are declared for this table's explosion
    (all `utility_type = "electric"`, matching what we found above).

    The bug: the original code decided which utility/year pairs need a
    correction by merging on **exact floating point equality** -- matching a
    computed `abs_diff` (the gap between a reported value and the value
    calculated from its normal subcomponents) against another factoid's raw
    reported value, via `pd.merge(..., left_on=[..., "abs_diff"], right_on=[..., value_col])`,
    with no tolerance at all.

    `ending_balance` moved from 32-bit to 64-bit floats on this branch
    That changes how upstream arithmetic on that column rounds, so a
    *different* -- and larger -- set of utility/year pairs land on exact
    equality than did under float32. Each newly-matched pair produces one new
    synthetic correction row, and corrections are only ever **added**
    (`pd.concat`), never removed. That's exactly the "more rows every year,
    never fewer" pattern we observed.

    ### Does this fully explain it?

    Filtering both datasets to just the three off-by correction factoids and
    comparing counts and summed balances:
    """)
    return


@app.cell
def quantify_match(local_bsa, nightly_bsa):
    OFF_BY_FACTOIDS = [
        "utility_plant_in_service_classified_and_property_under_capital_leases_off_by_utility_plant_in_service_completed_construction_not_classified_correction",
        "utility_plant_in_service_classified_and_property_under_capital_leases_off_by_utility_plant_in_service_property_under_capital_leases_correction",
        "depreciation_utility_plant_in_service_off_by_amortization_of_other_utility_plant_utility_plant_in_service_correction",
    ]

    local_corrections = local_bsa.filter(pl.col("xbrl_factoid").is_in(OFF_BY_FACTOIDS))
    nightly_corrections = nightly_bsa.filter(
        pl.col("xbrl_factoid").is_in(OFF_BY_FACTOIDS)
    )

    total_row_diff = len(local_bsa) - len(nightly_bsa)
    correction_row_diff = len(local_corrections) - len(nightly_corrections)
    total_sum_diff = (
        local_bsa["ending_balance"].sum() - nightly_bsa["ending_balance"].sum()
    )
    correction_sum_diff = (
        local_corrections["ending_balance"].sum()
        - nightly_corrections["ending_balance"].sum()
    )

    pl.DataFrame(
        {
            "metric": ["row-count difference", "ending_balance sum difference ($)"],
            "whole table": [total_row_diff, round(total_sum_diff)],
            "off-by correction rows only": [
                correction_row_diff,
                round(correction_sum_diff),
            ],
        }
    )
    return


@app.cell
def fix_narrative(mo):
    mo.md(r"""
    ## 4. The fix: never merge on floating point values

    Exact-value floating point equality is inherently fragile -- it depends
    on bit-for-bit reproducibility of arithmetic, which numeric
    representation changes (32- vs 64-bit), summation order, or even
    platform/BLAS differences can all disturb. The fix replaces the
    float-valued merge key with a two-step approach:

    1. Merge candidates on the **exact, integer** keys only --
       `report_year` and `utility_id_ferc1` -- restricting the candidate
       pool to a single utility, single year.
    2. Compare the reported values with `np.isclose(rtol=0.0, atol=...)`
       *after* the merge, instead of using them as merge keys directly.

    This still leaves one question: what's the right `atol`? Too tight, and
    we're back to being fragile to numeric noise. Too loose, and we risk
    fabricating "off by" corrections between two genuinely different
    reported figures that happen to land near each other by coincidence.
    """)
    return


@app.cell
def atol_intro(mo):
    mo.md(r"""
    ## 5. Choosing `atol`: look at the actual candidate pairs

    Rather than guess, we captured every *candidate* utility/year pair the
    merge considers -- before applying any tolerance filter -- along with
    how far apart the two values being compared are (`_delta`). This required
    a one-off instrumented run of `add_sizable_minority_corrections()`, since
    the intermediate `abs_diff` / `calculated_value` columns are intentionally
    dropped before the table is persisted to Parquet.
    """)
    return


@app.cell
def atol_data():
    candidates = pl.read_csv(Path(__file__).parent / "rate_base_off_by_candidates.csv")
    candidates
    return (candidates,)


@app.cell
def atol_sweep_table(candidates):
    ATOL_GRID = [0.0, 0.5, 1, 2, 5, 10, 20, 50, 100, 1_000, 10_000, 100_000]

    atol_sweep = pl.DataFrame(
        {
            "atol ($)": ATOL_GRID,
            "matching pairs": [
                int((candidates["_delta"] <= a).sum()) for a in ATOL_GRID
            ],
        }
    )
    atol_sweep
    return


@app.cell
def atol_sweep_plot(candidates):
    import matplotlib.pyplot as plt

    chosen_atol = 10.0

    sweep_for_plot = (
        candidates.filter(pl.col("_delta").is_not_null())
        .sort("_delta")
        .with_row_index("rank")
        .with_columns((pl.col("rank") + 1).alias("cumulative matches"))
        .filter(pl.col("_delta") <= 100_000_000)
    )

    fig, ax = plt.subplots(figsize=(16, 8))
    ax.plot(
        sweep_for_plot["_delta"],
        sweep_for_plot["cumulative matches"],
        marker=".",
        linewidth=1.5,
    )
    ax.axvline(
        chosen_atol,
        color="red",
        linestyle="--",
        label=f"chosen atol = ${chosen_atol:.0f}",
    )
    ax.set_xscale("symlog", linthresh=1)
    ax.set_xlim(left=-0.1)
    ax.set_xlabel("atol ($, tolerance for a match, symlog scale)", fontsize=13)
    ax.set_ylabel("cumulative number of matching pairs", fontsize=13)
    ax.set_title(
        "Matching pairs vs. tolerance -- note the plateau, then the jump past ~$1,000",
        fontsize=14,
    )
    ax.tick_params(axis="both", labelsize=11)
    ax.margins(y=0.1)
    ax.legend(loc="lower right", fontsize=11)
    fig.tight_layout(pad=2.5)
    fig
    return (plt,)


@app.cell
def reasoning_narrative(mo):
    mo.md(r"""
    ## 6. Why a $10 tolerance is defensible

    A few things make this a low-risk choice rather than an arbitrary one:

    - **The candidate pool is already narrow.** The merge only ever compares
      values within a single utility and a single report year -- not across
      the whole table. The chance of two *unrelated* multi-hundred-thousand-
      to billion-dollar figures for the same utility-year coincidentally
      landing within a few dollars of each other is low.
    - **`atol` is in absolute dollars, on a field that's usually in the
      hundreds of thousands to billions.** A same-utility-year match within
      $10 of a huge target value is a very different claim than "these two
      numbers are close" in the abstract.
    - **No candidate ever matches more than one target**, at any tolerance
      up to $5,000+. If matches were coincidental, we'd expect to
      occasionally see one row match two or more candidates as the tolerance
      widens. We don't.
    """)
    return


@app.cell
def multiplicity_check(candidates):
    multiplicity = pl.DataFrame(
        {
            "atol ($)": (grid := [0.5, 1, 2, 5, 10, 20, 100, 1_000, 5_000]),
            "rows with >1 match": [
                int(
                    candidates.filter(pl.col("_delta") <= a)
                    .group_by(["report_year", "utility_id_ferc1", "xbrl_factoid"])
                    .len()
                    .filter(pl.col("len") > 1)
                    .height
                )
                for a in grid
            ],
        }
    )
    multiplicity
    return


@app.cell
def sample_bands_narrative(mo):
    mo.md(r"""
    ## 7. Looking at the actual matches, band by band

    The clearest way to judge whether a match is real is to look at it. Below,
    a few examples from each delta band -- note how even at delta $2-10 the
    two values being compared are huge (tens of thousands to over a billion
    dollars), consistent with an independent small rounding difference
    between two reported schedules, not a coincidence. The tolerance is
    tiny relative to the values it's comparing.

    We also include the one clearly **spurious-looking** candidate we found,
    just past our chosen cutoff: `abs_diff=10.0` vs. `off_by value=-10.0`
    (delta $20) -- both values are tiny, so "close" is meaningless there. It's
    plausibly a sign-flip on the same near-zero fact, but at that scale it
    doesn't matter either way, and it's excluded by `atol=10`.
    """)
    return


@app.cell
def sample_bands(candidates):
    bands = [(0, 0.5), (2, 5), (5, 10), (10, 25)]
    pl.concat(
        [
            candidates.filter((pl.col("_delta") > lo) & (pl.col("_delta") <= hi))
            .sort("_delta")
            .head(3)
            .with_columns(pl.lit(f"(${lo}, ${hi}]").alias("delta band"))
            for lo, hi in bands
        ]
    ).select(
        "delta band",
        pl.col("report_year").cast(pl.String),
        "utility_id_ferc1",
        pl.col("abs_diff").round(0).cast(pl.Int64),
        pl.col("ending_balance_off_by").round(0).cast(pl.Int64).alias("off_by_value"),
        pl.col("_delta").round(2),
    )
    return


@app.cell
def decision_narrative(mo):
    mo.md(r"""
    ## 8. Decision: `atol=10`

    Putting it together: matches accumulate steadily from $0.50 up through about
    $100, stay at **zero multiplicity** that whole way, and every example we
    inspected in that range pairs large-magnitude values with a small absolute gap.
    Past several thousand dollars the count jumps and multiplicity appears.

    We picked **`atol=10`**, still two orders of magnitude below where matches
    become ambiguous, and every match up to that point looks like a genuine
    independent-rounding difference rather than a coincidence.

    This is implemented in `Exploder.add_sizable_minority_corrections()`
    (`src/pudl/output/ferc1.py`) as:

    ```python
    candidate_corrections = pd.merge(
        left=not_close,
        right=off_by_fact,
        on=["report_year", "utility_id_ferc1"],
        how="inner",
        suffixes=("\", "_off_by"),
    )
    data_corrections = candidate_corrections[
        np.isclose(
            candidate_corrections["abs_diff"],
            candidate_corrections[f"{value_col}_off_by"],
            rtol=0.0,
            atol=10.0,
        )
    ]
    ```
    """)
    return


@app.cell
def verify_narrative(mo):
    mo.md(r"""
    ## 9. Verifying the fix

    Re-materializing with the tolerance-based merge (`atol=10`) reproduces
    essentially the same output as the original (unstable) exact-equality floating
    point merge code -- not nightly's lower row count. That's an important, somewhat
    counterintuitive result: it means **nightly (main, float32) was under-applying
    legitimate corrections**, because float32 doesn't have enough precision to
    represent these large dollar figures exactly, so genuinely-equal values were
    failing the old exact-equality check there. This branch's 64-bit change happened
    to fix that side effect; our fix just makes the mechanism robust instead of
    lucky.

    `out_ferc1__yearly_rate_base` is built downstream of the exploded
    balance-sheet-assets table, so it inherits the same extra rows and altered
    `ending_balance` sums -- no separate mechanism needed to explain those changes.
    """)
    return


@app.cell
def double_count_check_narrative(mo):
    mo.md(r"""
    ### Wait -- is summing the whole table even valid?

    `out_ferc1__yearly_detailed_balance_sheet_assets` is an *exploded*
    calculation tree: parent factoids are calculated from child subcomponents.
    If the persisted table retained both parent (subtotal) rows and their
    child rows, summing `ending_balance` across the whole table for a
    utility-year would double- (or many-times-) count the same dollars.

    We can check this directly: `assets_and_other_debits` /
    `utility_type = "total"` in the **un-exploded** root table
    (`core_ferc1__yearly_balance_sheet_assets_sched110`) is the single
    canonical "total assets" figure FERC1 utilities report for a given
    utility-year. If `out_ferc1__yearly_detailed_balance_sheet_assets` is a
    properly disaggregated, leaf-level breakdown with no overlap, summing
    *all* of its rows for that same utility-year should reproduce that one
    number almost exactly.
    """)
    return


@app.cell
def double_count_check(local_bsa):
    core_bsa_root = pl.read_parquet(
        LOCAL_PARQUET / "core_ferc1__yearly_balance_sheet_assets_sched110.parquet"
    )
    canonical_totals = core_bsa_root.filter(
        (pl.col("asset_type") == "assets_and_other_debits")
        & (pl.col("utility_type") == "total")
    ).select(
        "utility_id_ferc1",
        "report_year",
        pl.col("ending_balance").alias("canonical_total"),
    )

    detail_sums = local_bsa.group_by("utility_id_ferc1", "report_year").agg(
        pl.col("ending_balance").sum().alias("detail_sum")
    )

    double_count_check = canonical_totals.join(
        detail_sums, on=["utility_id_ferc1", "report_year"], how="inner"
    ).with_columns((pl.col("detail_sum") / pl.col("canonical_total")).alias("ratio"))

    n_total = len(double_count_check)
    n_weird = double_count_check.filter(
        (pl.col("canonical_total").abs() > 1_000)
        & ((pl.col("ratio") > 1.5) | (pl.col("ratio") < 0.5))
    ).height

    pl.DataFrame(
        {
            "": [
                "utility-years compared",
                "25th percentile ratio (detail sum / canonical total)",
                "median ratio",
                "75th percentile ratio",
                "utility-years with ratio far from 1.0 (>1.5x or <0.5x)",
            ],
            "value": [
                str(n_total),
                str(round(double_count_check["ratio"].quantile(0.25), 4)),
                str(round(double_count_check["ratio"].quantile(0.5), 4)),
                str(round(double_count_check["ratio"].quantile(0.75), 4)),
                f"{n_weird} ({n_weird / n_total:.1%})",
            ],
        }
    )
    return


@app.cell
def double_count_conclusion(mo):
    mo.md(r"""
    The 25th, 50th, and 75th percentile ratios are all exactly **1.0**: for the
    large majority of utility-years, summing every row of the detailed table
    reproduces the single canonical total-assets figure almost exactly, with
    no evidence of systematic double-counting. There's no retained `"total"`
    bucket in `utility_type` either (only `electric`, `gas`, `common`,
    `other`, `other2`, `other3`) -- consistent with this being a properly
    disaggregated, leaf-level breakdown rather than a mix of totals and parts.

    There is a small tail (~2.7% of utility-years) where the ratio is far from
    1.0, including a few with the *opposite sign* -- worth further
    investigation on its own, but too small a fraction to change the
    conclusions above, and not specific to local vs. nightly (it would affect
    both sides of every diff we've computed equally).

    `out_ferc1__yearly_rate_base` is built by the same `Exploder`-based
    disaggregation process and shares the same `utility_type` universe (no
    `"total"` bucket), so the same reasoning applies there.
    """)
    return


@app.cell
def impact_narrative(mo):
    mo.md(r"""
    ## 10. Real-world impacts

    Breaking it down by year in dollars and as a percentage of the nightly-reported
    total shows the practical impact of this bug: how big a delta a downstream user
    of `ending_balance` would see in a given year, and does that swing grow, shrink,
    or stay roughly proportional over time?
    """)
    return


@app.cell
def verify_final(local_bsa, nightly_bsa):
    local_rb = pl.read_parquet(LOCAL_PARQUET / f"{TABLE_RB}.parquet")
    nightly_rb = pl.read_parquet((NIGHTLY_BASE / f"{TABLE_RB}.parquet").read_bytes())

    def yearly_diff(
        local: pl.DataFrame, nightly: pl.DataFrame, table: str
    ) -> pl.DataFrame:
        local_by_year = local.group_by("report_year").agg(
            pl.col("ending_balance").sum().alias("local_sum")
        )
        nightly_by_year = nightly.group_by("report_year").agg(
            pl.col("ending_balance").sum().alias("nightly_sum")
        )
        return (
            local_by_year.join(
                nightly_by_year,
                on="report_year",
                how="full",
                nulls_equal=True,
                coalesce=True,
            )
            .with_columns(
                (pl.col("local_sum") - pl.col("nightly_sum")).alias("diff ($)")
            )
            .with_columns(
                (pl.col("diff ($)") / pl.col("nightly_sum") * 100).alias(
                    "diff (% of nightly)"
                )
            )
            .with_columns(table=pl.lit(table))
            .sort("report_year")
        )

    yearly_diffs = pl.concat(
        [
            yearly_diff(local_bsa, nightly_bsa, TABLE_BSA),
            yearly_diff(local_rb, nightly_rb, TABLE_RB),
        ]
    ).select(
        "table",
        pl.col("report_year").cast(pl.String),
        pl.col("local_sum").round(0).cast(pl.Int64),
        pl.col("nightly_sum").round(0).cast(pl.Int64),
        pl.col("diff ($)").round(0).cast(pl.Int64),
        pl.col("diff (% of nightly)").round(4),
    )
    yearly_diffs
    return (yearly_diffs,)


@app.cell
def yearly_diff_plots_intro(mo):
    mo.md(r"""
    ### The same thing, visually

    Bars below zero mean local's `ending_balance` total for that year is
    *lower* than nightly's, even though local has more rows -- a reminder that
    extra correction rows aren't always positive numbers.
    """)
    return


@app.cell
def plot_yearly_diff_fn(plt, yearly_diffs):
    def plot_yearly_diff(table_name: str, title: str):
        d = yearly_diffs.filter(pl.col("table") == table_name).sort("report_year")
        years = d["report_year"].to_list()
        dollar_diff = d["diff ($)"].to_list()
        pct_diff = d["diff (% of nightly)"].to_list()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 8), sharex=True)

        ax1.bar(years, dollar_diff, color="#4C72B0")
        ax1.axhline(0, color="black", linewidth=0.8)
        ax1.set_ylabel("diff ($, local - nightly)", fontsize=13)
        ax1.set_title(f"{title}: ending_balance diff by year", fontsize=14)
        ax1.tick_params(axis="both", labelsize=11)

        ax2.bar(years, pct_diff, color="#DD8452")
        ax2.axhline(0, color="black", linewidth=0.8)
        ax2.set_ylabel("diff (% of nightly)", fontsize=13)
        ax2.set_xlabel("report_year", fontsize=13)
        ax2.tick_params(axis="both", labelsize=11)
        for label in ax2.get_xticklabels():
            label.set_rotation(45)
            label.set_ha("right")

        fig.tight_layout(pad=2.5)
        return fig

    return (plot_yearly_diff,)


@app.cell
def bsa_diff_plot(plot_yearly_diff):
    plot_yearly_diff(TABLE_BSA, "out_ferc1__yearly_detailed_balance_sheet_assets")
    return


@app.cell
def rb_diff_plot(plot_yearly_diff):
    plot_yearly_diff(TABLE_RB, "out_ferc1__yearly_rate_base")
    return


@app.cell
def conclusion(mo):
    mo.md(r"""
    ## 10. Conclusion

    - The row-count and `ending_balance`-sum differences in these two tables
      are **not a regression** from the dtype changes on this branch.
    - They trace entirely to a pre-existing bug: `add_sizable_minority_corrections()`
      matched correction candidates via exact floating-point equality, which
      is inherently fragile to numeric precision changes.
    - Under float32 (nightly/main), that fragility caused **real, legitimate**
      corrections to be silently missed. This branch's 64-bit change
      incidentally fixed most of that by chance; we've now fixed the
      underlying mechanism so it no longer depends on luck.
    - The fix never merges on floating point values: candidates are matched
      on exact integer keys (utility, year), then compared with a tolerance
      chosen from direct inspection of the actual candidate pairs, not a
      guess.
    - `dbt` expectations for both tables were updated to match the corrected,
      now-stable output.
    """)
    return


@app.cell
def appendix(mo):
    mo.md(r"""
    ## Appendix

    ### Why we were confident this was a real 32-to-64-bit change, and not scoped to a few large tables

    `src/pudl/metadata/dtypes.py` maps PUDL's abstract field types to concrete
    backend dtypes. On `main`, `FIELD_DTYPES_PYARROW["number"] = pa.float32()`
    (and `"integer": pa.int32()`) -- keyed only by the abstract type, with no
    per-table override anywhere in the codebase. `PudlParquetIOManager.handle_output()`
    (`src/pudl/dagster/io_managers.py`) writes *every* Dagster asset's
    DataFrame to Parquet using this schema -- including intermediate
    core-layer tables like `core_ferc1__yearly_utility_plant_summary_sched200`,
    which feeds into this table's explosion. Since Dagster persists and
    reloads each asset between steps, this means genuine float32 rounding was
    baked into `ending_balance` at *every* stage of the pipeline on main --
    not just the final output -- even though pandas' own in-memory dtype for
    `"number"` was always `"float64"` on both branches.

    ### Provenance of `rate_base_off_by_candidates.csv`

    The `abs_diff`, `calculated_value`, and other intermediate columns used
    for the atol sweep above are intentionally dropped before
    `out_ferc1__yearly_detailed_balance_sheet_assets` is written to Parquet,
    so they can't be recovered from `$PUDL_OUTPUT` alone. This file was
    captured via a one-off debug instrumentation of
    `add_sizable_minority_corrections()` (dumping `candidate_corrections`,
    with a computed `_delta` column, to CSV before the tolerance filter was
    applied), then removed from the shipped code. To regenerate it: add a
    `candidate_corrections.to_csv(...)` line right after the `pd.merge(...)`
    call that builds `candidate_corrections` in that method, then
    materialize `out_ferc1__yearly_detailed_balance_sheet_assets`.
    """)
    return


if __name__ == "__main__":
    app.run()
