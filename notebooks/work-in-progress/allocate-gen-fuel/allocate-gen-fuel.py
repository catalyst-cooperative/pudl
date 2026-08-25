import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import os

    import marimo as mo
    import matplotlib.pyplot as plt
    import matplotx
    import polars as pl
    import s3fs

    return matplotx, mo, os, pl, plt, s3fs


@app.cell
def _(mo):
    mo.md(r"""
    # Multi-year proposed/retired plant status fix — impact on allocated gen & fuel

    This branch (PR #5511) generalizes @grgmiller's fix (#5419) to
    `allocate_gen_fuel.identify_proposed_plants`, mirrors it onto the sibling
    `identify_retired_plants`, and unifies both directions into shared helper
    functions in `allocate_gen_fuel.py`.

    This notebook compares the allocated
    `out_eia923__monthly_generation_fuel_by_generator_energy_source` table **built
    locally on this branch** against the same table from the **published nightly
    build** (`main`, without this fix). Two things about that comparison:

    - Nightly reflects `main`, which also lacks the separate `tiebreaker` branch's
      non-determinism fixes (not yet merged). A small number of rows differ for
      reasons unrelated to this PR — see the sanity-check section below.
    - This PR's fixes should only ever *add* previously-dropped rows, never remove
      or alter existing ones directly — though rescuing a generator can still
      *reallocate* values for its plant-mates (see "Reallocation ripple effect").
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## The bug, in one paragraph

    `identify_proposed_plants`/`identify_retired_plants` need to answer: "is this
    plant *entirely* proposed (or retired) — does every generator reported for it
    share that status?" — before trusting its plant-level `gf`-table generation.
    The old code checked that uniformity across the *entire* multi-year input
    (e.g. spanning 2001-2026), keyed only on `plant_id_eia`. A plant reported
    `proposed` in 2023-2024 and `existing` from 2025 onward has an `"existing"`
    row *somewhere* in that whole input, so the old check saw a status mismatch
    and excluded the plant's data for **all** of its years — including the
    genuinely all-`proposed` 2023-2024 months. The fix adds a `report_year` column
    and keys the uniformity check on `(plant_id_eia, report_year)` instead, so
    2023, 2024, and 2025 are each evaluated independently: 2023 and 2024 pass
    (all-`proposed` within that year), and 2025 correctly falls outside this
    function's scope entirely. This "`report_year`-scoping" fix is identical code
    on both the `proposed` and `retired` sides
    (`_identify_entirely_transitioned_plants` in `allocate_gen_fuel.py`).
    """)
    return


@app.cell
def _(matplotx, os, plt):
    plt.style.use(matplotx.styles.onedark)
    plt.rcParams["figure.dpi"] = 100
    FIGSIZE_WIDE = (20, 8)  # 2000px wide @ 100dpi
    FIGSIZE_TALL = (20, 11)

    TABLE_NAME = "out_eia923__monthly_generation_fuel_by_generator_energy_source"
    LOCAL_PARQUET_DIR = (
        os.environ.get("PUDL_OUTPUT", "/Users/zane/code/catalyst/pudl-output")
        + "/parquet"
    )
    NIGHTLY_BUCKET = "pudl.catalyst.coop/nightly"
    return FIGSIZE_TALL, LOCAL_PARQUET_DIR, NIGHTLY_BUCKET, TABLE_NAME


@app.cell
def _(mo):
    mo.md("""
    ## Load data
    """)
    return


@app.cell
def _(LOCAL_PARQUET_DIR, TABLE_NAME, pl):
    local_alloc = pl.read_parquet(f"{LOCAL_PARQUET_DIR}/{TABLE_NAME}.parquet")
    local_alloc.shape
    return (local_alloc,)


@app.cell
def _(NIGHTLY_BUCKET, TABLE_NAME, pl, s3fs):
    _fs = s3fs.S3FileSystem(anon=True)
    with _fs.open(f"{NIGHTLY_BUCKET}/{TABLE_NAME}.parquet", "rb") as _f:
        nightly_alloc = pl.read_parquet(_f)
    nightly_alloc.shape
    return (nightly_alloc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Classify generator-years by operational status

    Tag every `(plant_id_eia, generator_id, year)` with the `operational_status`
    reported that year in the **local** `core_eia860__scd_generators` table — the
    same signal `identify_retired_plants`/`identify_proposed_plants` key off of.
    Used to tag *both* the local and nightly allocated rows below, so the
    comparison isolates how the allocation logic treats a given generator-year's
    status, not any difference in the status data itself.

    Also pull `generator_operating_date` from `core_eia__entity_generators`, and
    note whether a generator is *ever* observed as `"existing"` in any year of
    `core_eia860__scd_generators`. `generator_operating_date` is a single,
    harvested-once value on the generator **entity** table (constant across all
    years) — unlike `generator_retirement_date`, which lives on the
    annually-refreshed **SCD** table. A generator that eventually goes into
    service has its operating date backfilled onto *all* its historical rows,
    including years it was still labeled `"proposed"` — so a null
    `generator_operating_date` means "never yet observed as existing," not "date
    temporarily unknown."
    """)
    return


@app.cell
def _(LOCAL_PARQUET_DIR, pl):
    _scd_status = pl.read_parquet(
        f"{LOCAL_PARQUET_DIR}/core_eia860__scd_generators.parquet",
        columns=["plant_id_eia", "generator_id", "report_date", "operational_status"],
    )

    _status_category = (
        pl.when(pl.col("operational_status") == "retired")
        .then(pl.lit("retired"))
        .when(pl.col("operational_status") == "proposed")
        .then(pl.lit("proposed"))
        .otherwise(pl.lit("other"))
    )

    gen_status = (
        _scd_status.with_columns(
            pl.col("report_date").dt.year().alias("year"),
            _status_category.alias("status_category"),
        )
        .select(
            [
                "plant_id_eia",
                "generator_id",
                "year",
                "operational_status",
                "status_category",
            ]
        )
        # a generator can appear more than once per year in the SCD table if its status
        # or other tracked attributes changed mid-year; keep the first (earliest) record.
        .unique(subset=["plant_id_eia", "generator_id", "year"], keep="first")
    )

    _ever_existing = (
        _scd_status.filter(pl.col("operational_status") == "existing")
        .select(["plant_id_eia", "generator_id"])
        .unique()
        .with_columns(pl.lit(True).alias("ever_existing"))
    )

    gen_meta = (
        pl.read_parquet(
            f"{LOCAL_PARQUET_DIR}/core_eia__entity_generators.parquet",
            columns=["plant_id_eia", "generator_id", "generator_operating_date"],
        )
        .join(_ever_existing, on=["plant_id_eia", "generator_id"], how="left")
        .with_columns(pl.col("ever_existing").fill_null(False))
    )

    gen_status["status_category"].value_counts()
    return gen_meta, gen_status


@app.cell
def _(mo):
    mo.md("""
    ## Headline comparison: local vs. nightly, by generator status category
    """)
    return


@app.cell
def _(gen_status, local_alloc, nightly_alloc, pl):
    def _tag_status(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("report_date").dt.year().alias("year"))
            .join(gen_status, on=["plant_id_eia", "generator_id", "year"], how="left")
            .with_columns(pl.col("status_category").fill_null("other"))
        )

    local_tagged = _tag_status(local_alloc).with_columns(
        pl.lit("local").alias("source")
    )
    nightly_tagged = _tag_status(nightly_alloc).with_columns(
        pl.lit("nightly").alias("source")
    )
    combined = pl.concat([local_tagged, nightly_tagged], how="vertical_relaxed")

    yearly_summary = (
        combined.group_by(["source", "status_category", "year"])
        .agg(
            pl.len().alias("n_rows"),
            pl.col("net_generation_mwh").sum().alias("net_generation_mwh"),
            pl.col("fuel_consumed_mmbtu").sum().alias("fuel_consumed_mmbtu"),
        )
        .sort(["status_category", "year", "source"])
    )
    yearly_summary
    return (yearly_summary,)


@app.cell
def _(FIGSIZE_TALL, pl, plt, yearly_summary):
    _metrics = [
        ("n_rows", "Row count"),
        ("net_generation_mwh", "Net generation (MWh)"),
        ("fuel_consumed_mmbtu", "Fuel consumed (MMBTU)"),
    ]
    _categories = ["proposed", "retired"]

    fig_rescued, _axes = plt.subplots(
        len(_categories), len(_metrics), figsize=FIGSIZE_TALL, squeeze=False
    )

    for _row, _cat in enumerate(_categories):
        for _col, (_metric, _label) in enumerate(_metrics):
            _ax = _axes[_row][_col]
            for _source, _marker in [("local", "o"), ("nightly", "x")]:
                _sub = yearly_summary.filter(
                    (pl.col("status_category") == _cat) & (pl.col("source") == _source)
                ).sort("year")
                _ax.plot(
                    _sub["year"],
                    _sub[_metric],
                    marker=_marker,
                    label=_source,
                    linewidth=2,
                )
            _ax.set_title(f"{_cat}: {_label}")
            _ax.set_xlabel("report year")
            if _col == 0:
                _ax.set_ylabel(_label)
            _ax.legend()

    fig_rescued.suptitle(
        "Local (this branch) vs. nightly (main) allocated gen/fuel, "
        "by generator status category",
        fontsize=16,
    )
    fig_rescued.tight_layout()
    fig_rescued
    return


@app.cell
def _(mo):
    mo.md(r"""
    The panels above share a y-axis scale set by the larger series, so a real but
    proportionally small difference on the `retired` side is easy to miss.
    Plotted below: the local-minus-nightly delta directly, on its own scale per
    panel, so both effects are visible regardless of their relative size.
    """)
    return


@app.cell
def _(FIGSIZE_TALL, pl, plt, yearly_summary):
    _delta_wide = (
        yearly_summary.filter(pl.col("status_category") != "other")
        .pivot(
            values=["n_rows", "net_generation_mwh", "fuel_consumed_mmbtu"],
            index=["status_category", "year"],
            on="source",
        )
        .fill_null(0)
        .with_columns(
            (pl.col("n_rows_local") - pl.col("n_rows_nightly")).alias("delta_n_rows"),
            (
                pl.col("net_generation_mwh_local")
                - pl.col("net_generation_mwh_nightly")
            ).alias("delta_net_generation_mwh"),
            (
                pl.col("fuel_consumed_mmbtu_local")
                - pl.col("fuel_consumed_mmbtu_nightly")
            ).alias("delta_fuel_consumed_mmbtu"),
        )
        .sort("status_category", "year")
    )

    _delta_metrics = [
        ("delta_n_rows", "Δ row count (local − nightly)"),
        ("delta_net_generation_mwh", "Δ net generation (MWh)"),
        ("delta_fuel_consumed_mmbtu", "Δ fuel consumed (MMBTU)"),
    ]
    _categories = ["proposed", "retired"]

    fig_delta, _axes = plt.subplots(2, 3, figsize=FIGSIZE_TALL, squeeze=False)

    for _row, _cat in enumerate(_categories):
        for _col, (_metric, _label) in enumerate(_delta_metrics):
            _ax = _axes[_row][_col]
            _sub = _delta_wide.filter(pl.col("status_category") == _cat).sort("year")
            _ax.bar(_sub["year"], _sub[_metric])
            _ax.axhline(0, linewidth=1)
            _ax.set_title(f"{_cat}: {_label}")
            _ax.set_xlabel("report year")
            if _col == 0:
                _ax.set_ylabel(_label)

    fig_delta.suptitle(
        "Local minus nightly, by generator status category (own scale per row)",
        fontsize=16,
    )
    fig_delta.tight_layout()
    fig_delta
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Sanity check: does local ever *lose* data nightly has?

    This PR's fixes should only ever add previously-dropped rows back in, never
    remove or change existing ones directly. Diff the two tables on the full row
    key (`plant_id_eia`, `generator_id`, `report_date`, `energy_source_code`,
    `prime_mover_code`).
    """)
    return


@app.cell
def _(local_alloc, mo, nightly_alloc, pl):
    _key = [
        "plant_id_eia",
        "generator_id",
        "report_date",
        "energy_source_code",
        "prime_mover_code",
    ]

    nightly_only = nightly_alloc.join(local_alloc, on=_key, how="anti")
    local_only = local_alloc.join(nightly_alloc, on=_key, how="anti")

    _both = local_alloc.join(nightly_alloc, on=_key, how="inner", suffix="_nightly")
    changed_values = _both.filter(
        (pl.col("net_generation_mwh") != pl.col("net_generation_mwh_nightly"))
        | (pl.col("fuel_consumed_mmbtu") != pl.col("fuel_consumed_mmbtu_nightly"))
    )

    mo.md(
        f"""
        - Rows only in **nightly**: **{len(nightly_only):,}**
        - Rows only in **local** (rescued by this PR): **{len(local_only):,}**
        - Rows in both, with **changed values**: **{len(changed_values):,}**
        """
    )
    return changed_values, local_only


@app.cell
def _(mo):
    mo.md(r"""
    The 24 `nightly_only` rows are **not** local dropping data — the row key still
    exists in local, but its `prime_mover_code` differs (`CA` locally vs. `ST` in
    nightly) for plant 50973 / generator GN27, report_date 2011. This is a known,
    already-fixed issue on the separate `tiebreaker` branch
    (`89617c90b8`, "Fix non-deterministic tiebreaks in EIA entity resolution and
    plant parts"), which reproduces this exact plant/generator/date:
    `occurrence_consistency()` picked a winner among exactly-tied
    `prime_mover_code` candidates using incidental pandas sort order rather than an
    explicit tiebreak, so local (macOS) and nightly (Linux) builds resolved the tie
    differently. It's unrelated to this PR and will disappear once `tiebreaker`
    merges into `main` and nightly rebuilds from it.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Reallocation ripple effect

    The 6,264 `changed_values` rows are expected, not a bug. Rescuing a
    previously-dropped generator changes the pool of generators that a plant's
    `gf`-table (less granular) generation/fuel gets proportionally allocated
    across for any shared PM/ESC combo. Bringing a retired/proposed generator's
    data back in can shift the *allocated* values for other generators at the
    same plant, even in rows that existed in both builds. Distribution of the
    size of those shifts:
    """)
    return


@app.cell
def _(changed_values, pl):
    _diffs = changed_values.with_columns(
        (
            pl.col("net_generation_mwh").fill_null(0)
            - pl.col("net_generation_mwh_nightly").fill_null(0)
        )
        .abs()
        .alias("abs_diff_mwh"),
        (
            pl.col("fuel_consumed_mmbtu").fill_null(0)
            - pl.col("fuel_consumed_mmbtu_nightly").fill_null(0)
        )
        .abs()
        .alias("abs_diff_mmbtu"),
    )
    _diffs.select(["abs_diff_mwh", "abs_diff_mmbtu"]).describe()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Why does the proposed side rescue so much more than the retired side?

    The `report_year`-scoping fix is identical code on both directions, but the
    headline comparison above shows a large asymmetry: far more rows and MWh/MMBTU
    get rescued on the `proposed` side than the `retired` side. Two mechanisms
    could plausibly explain that, both built into `_identify_entirely_transitioned_plants`:

    1. **The null-transition-date fallback.** A generator with no known transition
       date can't be *disproven* anomalous, so it's treated as a candidate for
       rescue too, rather than excluded by default. If null dates are far more
       common on the proposed side, this alone could explain the gap.
    2. **Different real-world incidence of the pattern the fix protects.** The
       `report_year`-scoping fix only matters for plants that are genuinely
       uniform-status in some years and mixed-status across their full history.
       If that pattern is just far more common among proposed-then-built plants
       than among retired-then-reactivated ones, that would explain the gap
       instead — with an identical fix on both sides.

    Check each against the data, starting with the null-date fallback.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Testing the null-date fallback

    Split every rescued `proposed`-status row (present in local, absent from
    nightly) by whether its generator ever reaches `"existing"` status, and
    whether its `generator_operating_date` is null:
    """)
    return


@app.cell
def _(gen_meta, gen_status, local_only, pl):
    rescued_proposed = (
        local_only.with_columns(pl.col("report_date").dt.year().alias("year"))
        .join(
            gen_status.filter(pl.col("status_category") == "proposed").select(
                ["plant_id_eia", "generator_id", "year"]
            ),
            on=["plant_id_eia", "generator_id", "year"],
            how="inner",
        )
        .join(gen_meta, on=["plant_id_eia", "generator_id"], how="left")
        .with_columns(
            pl.col("generator_operating_date").is_null().alias("null_operating_date")
        )
    )

    rescued_proposed_attribution = (
        rescued_proposed.group_by(["ever_existing", "null_operating_date"])
        .agg(
            pl.len().alias("n_rescued_rows"),
            pl.col("net_generation_mwh").sum(),
            pl.col("fuel_consumed_mmbtu").sum(),
        )
        .sort(["ever_existing", "null_operating_date"])
    )
    rescued_proposed_attribution
    return (rescued_proposed,)


@app.cell
def _(mo):
    mo.md(r"""
    Of the 5,007 rescued `proposed`-status rows:

    - **4,247 (85%)** belong to generators that *do* eventually reach `"existing"`
      status and already have a known `generator_operating_date` (0 nulls in this
      group). Rescued by the `report_year`-scoping fix plus the ordinary
      known-date anomalous-report comparison — **not** the null-date fallback.
    - **244 (4.9%)** genuinely rely on the null-date fallback: still-`"proposed"`
      generators with no operating date on record at all, that nonetheless report
      real gf-table generation. This is the case
      `test_identify_plants_unknown_transition_date` locks in as a regression
      test.
    - **516 (10%)** have a *known* operating date but have never (yet) been
      observed as `"existing"`. EIA's `operational_status` reporting is known to
      lag actual commissioning by months, so these are most likely generators
      that already have a confirmed operating date on record but whose status
      field hasn't caught up to `"existing"` in this data snapshot.

    The null-date fallback is necessary — it's what rescues a permanently-proposed
    generator with no recorded operating date that nonetheless reports real
    generation — but it accounts for only ~5% of rescued rows and ~15% of rescued
    MWh/MMBTU. It does not explain the proposed-vs-retired asymmetry: 85% of the
    proposed-side rescue runs through the ordinary known-date path, the same path
    available on the retired side. The first candidate explanation is ruled out.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Testing plant-lifecycle incidence

    Compute the analogous rescued-row set for the `retired` side, then compare how
    many *distinct plants* are actually rescued on each side, and how many of
    those plants are ones that eventually reach `"existing"` status at all.
    """)
    return


@app.cell
def _(LOCAL_PARQUET_DIR, gen_status, local_only, pl, rescued_proposed):
    rescued_retired = local_only.with_columns(
        pl.col("report_date").dt.year().alias("year")
    ).join(
        gen_status.filter(pl.col("status_category") == "retired").select(
            ["plant_id_eia", "generator_id", "year"]
        ),
        on=["plant_id_eia", "generator_id", "year"],
        how="inner",
    )

    rescued_proposed_plants = rescued_proposed.select("plant_id_eia").unique()
    rescued_retired_plants = rescued_retired.select("plant_id_eia").unique()

    _scd_status = pl.read_parquet(
        f"{LOCAL_PARQUET_DIR}/core_eia860__scd_generators.parquet",
        columns=["plant_id_eia", "operational_status"],
    )
    plants_ever_existing = (
        _scd_status.filter(pl.col("operational_status") == "existing")
        .select("plant_id_eia")
        .unique()
    )

    plant_incidence = pl.DataFrame(
        {
            "direction": ["proposed", "retired"],
            "rescued_rows": [rescued_proposed.height, rescued_retired.height],
            "distinct_plants_rescued": [
                rescued_proposed_plants.height,
                rescued_retired_plants.height,
            ],
            "of_which_ever_existing": [
                rescued_proposed_plants.join(
                    plants_ever_existing, on="plant_id_eia", how="inner"
                ).height,
                rescued_retired_plants.join(
                    plants_ever_existing, on="plant_id_eia", how="inner"
                ).height,
            ],
        }
    )
    plant_incidence
    return


@app.cell
def _(mo):
    mo.md(r"""
    **174 distinct plants** are rescued on the proposed side, vs. only **8** on
    the retired side — a ~20x gap in how many real plants trip each direction's
    version of the identical bug. Of the 174 proposed-side plants, **170**
    eventually reach `"existing"` status somewhere in their history; all 8
    retired-side plants do too.

    This confirms the second candidate: a plant reported `proposed` that later
    becomes `existing` is the near-universal life story of every power plant that
    actually gets built — practically every proposed project that goes on to
    report real generation data eventually flips to `existing`. A plant reported
    `retired` that later has `existing` generators show up again at the same
    `plant_id_eia` is a much rarer event — repowering, a rebuilt site, or a plant
    ID being reused for genuinely new units after the old ones retired. The
    `report_year`-scoping fix is byte-for-byte identical on both sides; it's the
    real-world frequency of the pattern it protects that differs by ~20x.

    One reconciliation note: 291 retired rows are rescued locally, not the 267
    shown in the delta charts above. The 24-row gap is exactly the plant
    50973/GN27 `nightly_only` rows described in the sanity-check section — a
    `tiebreaker`-branch tiebreak fix, not something this PR loses.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Overall totals rescued (all years, proposed + retired combined)
    """)
    return


@app.cell
def _(pl, yearly_summary):
    overall_totals = (
        yearly_summary.filter(pl.col("status_category") != "other")
        .group_by("source")
        .agg(
            pl.col("n_rows").sum(),
            pl.col("net_generation_mwh").sum(),
            pl.col("fuel_consumed_mmbtu").sum(),
        )
        .sort("source")
    )
    overall_totals
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Takeaways for discussion with @grgmiller

    - Local rescues **5,007 proposed rows** and **291 retired rows** (net +267
      after the one unrelated `tiebreaker`-fixed discrepancy), totaling roughly
      **+8.25M MWh / +76.7M MMBTU** net over nightly.
    - The ~20x row-count gap between the two directions is about how often the
      underlying pattern actually occurs in the real data, not about any
      asymmetry in the fix itself: 174 distinct plants trip the proposed-side bug
      vs. only 8 on the retired side, because "proposed becomes existing" is the
      near-universal fate of a real power plant, while "retired plant reports
      existing generators again" is rare (repowering, rebuilds, reused plant
      IDs). The null-date fallback is a real but minor contributor (~5% of the
      proposed-side rescue).
    - No row present in nightly is missing from local under normal circumstances.
      The one exception (plant 50973 / GN27) is a known, already-fixed
      non-determinism issue on the separate `tiebreaker` branch, unrelated to
      this PR, and will resolve itself once `tiebreaker` reaches `main` and
      nightly rebuilds.
    - Bringing back a retired/proposed generator's data can ripple into the
      allocated values of its plant-mates that share a PM/ESC combo, even for
      rows that existed in both builds — expected, given how `gf`-table
      generation gets proportionally allocated, but worth calling out explicitly
      so it doesn't look like a surprise regression during review.
    """)
    return


if __name__ == "__main__":
    app.run()
