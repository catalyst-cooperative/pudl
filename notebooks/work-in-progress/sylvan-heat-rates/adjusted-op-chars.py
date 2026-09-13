import pathlib

import marimo

# Marimo cells are functions; docstrings, cell-scoped SCREAMING_SNAKE_CASE
# parameters/config constants, and a trailing bare expression (the cell's
# display value) are idiomatic here, not real problems. Cyclomatic complexity
# limits also don't fit: a single cell often bundles several small helpers
# together, inflating the count without reflecting real complexity.
# ruff: noqa: D100, D103, N803, N806, B018, C901

__generated_with = "0.23.16"
app = marimo.App(width="columns")


@app.cell
def _():
    import importlib
    import os
    from unittest import mock

    import dagster as dg
    import marimo as mo
    import matplotlib.pyplot as plt
    import matplotx
    import numpy as np
    import polars as pl

    import pudl.analysis.operational_characteristics as ops_mod
    from pudl.dagster.build import build_defs

    PUDL_OUTPUT = os.environ["PUDL_OUTPUT"]

    # Large-format style for full-column / 4K-legible plots: onedark theme
    # plus text sized for a 3000px+-wide figure rather than a default-size one.
    PLOT_STYLE = [
        matplotx.styles.onedark,
        {
            "font.size": 22,
            # Figure suptitle and per-axes titles pinned to the same absolute
            # size (26.4pt -- what figure.titlesize's default 'large' keyword
            # happened to resolve to) rather than axes.titlesize's previous,
            # larger 32pt, so the two title tiers read as the same size.
            "axes.titlesize": 26.4,
            "figure.titlesize": 26.4,
            "axes.labelsize": 26,
            "xtick.labelsize": 20,
            "ytick.labelsize": 20,
            "legend.fontsize": 20,
        },
    ]

    # Fixed hue order for prime_mover_code, most- to least-common in the CEMS
    # fleet, so a series always gets the same color across charts/filters. Nulls
    # (no EIA generator match) get a neutral gray rather than a 7th categorical hue.
    PRIME_MOVER_COLORS = {
        "GT": "#61afef",  # gas turbine
        "ST": "#d49f6e",  # steam turbine
        "CA": "#98c379",  # combined cycle steam part
        "CT": "#e06c75",  # combined cycle combustion turbine part
        "CS": "#c678dd",  # combined cycle single shaft
        "IC": "#e5c07b",  # internal combustion
        "Unknown": "#5c6370",
    }

    importlib.reload(ops_mod)
    defs = build_defs()

    def fmt_df(df, float_decimals: int = 4, **table_kwargs):
        """Display a Polars/pandas dataframe with notebook-wide number formatting.

        Integers with no thousands separator, floats rounded to `float_decimals`
        places. marimo's default table renderer formats raw numeric JSON values
        itself (commas, full float precision) independent of pl.Config, so this
        goes through mo.ui.table's format_mapping instead, which we build
        automatically from the frame's own dtypes.
        """
        schema = df.schema if isinstance(df, pl.DataFrame) else df.dtypes
        mapping = {}
        for name, dtype in schema.items():
            if isinstance(dtype, pl.DataType) and dtype.is_float():
                mapping[name] = lambda v, _d=float_decimals: (
                    "" if v is None else f"{v:.{_d}f}"
                )
            elif isinstance(dtype, pl.DataType) and dtype.is_integer():
                mapping[name] = lambda v: "" if v is None else str(v)
        return mo.ui.table(df, format_mapping=mapping, **table_kwargs)

    return (
        PLOT_STYLE,
        PRIME_MOVER_COLORS,
        PUDL_OUTPUT,
        defs,
        dg,
        fmt_df,
        mo,
        mock,
        np,
        ops_mod,
        os,
        pl,
        plt,
    )


@app.cell
def existing_title(mo):
    mo.md("""
    ## `out_epacems__yearly_operational_characteristics` (existing, gross-load)
    """)
    return


@app.cell
def existing_chars(PUDL_OUTPUT, fmt_df, pl):
    existing_chars = pl.read_parquet(
        f"{PUDL_OUTPUT}/parquet/out_epacems__yearly_operational_characteristics.parquet"
    )
    fmt_df(existing_chars)
    return (existing_chars,)


@app.cell
def adjusted_title(mo):
    mo.md("""
    ## `_out_epacems__yearly_operational_characteristics_adjusted` (dev, net-generation)

    "
        "Not persisted to Parquet yet -- materialized here on demand for a small "
        "subset of states, in-process, using the real Dagster resources/config.
    """)
    return


@app.cell
def state_picker(mo, ops_mod):
    state_picker = mo.ui.multiselect(
        options=sorted(ops_mod.EPACEMS_STATES),
        value=["ID", "MT"],
        label="States to materialize (kept small -- each one re-scans the full hourly CEMS window)",
    )
    state_picker
    return (state_picker,)


@app.cell
def crosswalk_bug_note(mo):
    mo.callout(
        mo.md(
            "**Found a real bug while pairing**: the asset defaults "
            "`eia_epa_mapping_year` to CEMS's `max_full_year`, but "
            "`core_epa__assn_eia_epacamd` (the EPA/EIA crosswalk) lags behind -- "
            "its most recent `report_year` is **2024**, not 2025. Filtering to a "
            "year the crosswalk doesn't have returns an empty mapping, which "
            "silently nulls out `max_cap_mw` for every CEMS row (via "
            "`plant_unit`), which nulls `load_factor_adjusted_cems`, which zeroes "
            "`load_factor_nunique`, which empties `valid_cems` -- cascading into "
            "an all-null result with no error raised anywhere. Overriding the "
            "mapping year below to match what the crosswalk actually has fixes it."
        ),
        kind="warn",
    )
    return


@app.cell
def eia_config(mo):
    eia_report_date_input = mo.ui.text(
        value="2025-12-01", label="EIA generator snapshot report_date"
    )
    eia_epa_mapping_year_input = mo.ui.number(
        start=2000, stop=2030, value=2024, label="EPA/EIA crosswalk report_year"
    )
    mo.hstack([eia_report_date_input, eia_epa_mapping_year_input])
    return eia_epa_mapping_year_input, eia_report_date_input


@app.cell
def adjusted_chars(
    defs,
    dg,
    eia_epa_mapping_year_input,
    eia_report_date_input,
    fmt_df,
    mo,
    mock,
    ops_mod,
    pl,
    state_picker,
):
    def _materialize_adjusted(
        states: list[str], eia_report_date: str, eia_epa_mapping_year: int
    ) -> pl.DataFrame:
        target_key = dg.AssetKey(
            ["_out_epacems__yearly_operational_characteristics_adjusted"]
        )
        with mock.patch.object(ops_mod, "EPACEMS_STATES", set(states)):
            job = defs.get_implicit_global_asset_job_def()
            result = job.execute_in_process(
                asset_selection=[target_key],
                run_config={
                    "ops": {
                        "_out_epacems__yearly_operational_characteristics_adjusted": {
                            "config": {
                                "num_years": 3,
                                "min_stable_consecutive_hours": 8,
                                "eia_report_date": eia_report_date,
                                "eia_epa_mapping_year": eia_epa_mapping_year,
                            }
                        }
                    }
                },
            )
        pdf = result.output_for_node(
            "_out_epacems__yearly_operational_characteristics_adjusted"
        )
        return pl.from_pandas(pdf)

    with mo.status.spinner(
        title="Materializing adjusted characteristics..."
    ) as _spinner:
        adjusted_chars = _materialize_adjusted(
            state_picker.value,
            eia_report_date_input.value,
            eia_epa_mapping_year_input.value,
        )
    fmt_df(adjusted_chars)
    return (adjusted_chars,)


@app.cell
def comparison_title(mo):
    mo.md("""
    ## Side-by-side comparison (gross-load vs. net-generation-adjusted)
    """)
    return


@app.cell
def comparison(adjusted_chars, existing_chars, fmt_df):
    unit_cols = ["plant_id_epa", "emissions_unit_id_epa"]
    compare_cols = [
        "min_stable_load_factor",
        "min_up_time_hours",
        "min_down_time_hours",
        "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        "heat_rate_at_min_stable_load_factor_mmbtu_per_mwh",
        "ramp_up_rate_per_min",
        "ramp_down_rate_per_min",
    ]

    comparison = existing_chars.join(
        adjusted_chars,
        on=unit_cols,
        how="inner",
        suffix="_adjusted",
        validate="1:1",
    ).select(
        unit_cols
        + ["plant_id_eia", "state"]
        + [c for pair in compare_cols for c in (pair, f"{pair}_adjusted")]
    )
    fmt_df(comparison)
    return compare_cols, unit_cols


@app.cell
def national_bug_note(mo):
    mo.callout(
        mo.md(
            "**Fixed and re-materialized.** Both tables below are now the "
            "full national outputs, loaded fresh from disk after the "
            "`LATEST_EPACAMD_CROSSWALK_YEAR` fix."
        ),
        kind="success",
    )
    return


@app.cell
def national_adjusted_chars(mo, os, pl):
    import pickle

    with pathlib.Path(
        os.path.join(
            os.environ["DAGSTER_HOME"],
            "storage",
            "_out_epacems__yearly_operational_characteristics_adjusted",
        )
    ).open("rb") as _f:
        # Trusted local Dagster pickle-IO-manager output, not untrusted input.
        national_adjusted_chars = pl.from_pandas(pickle.load(_f))  # noqa: S301

    mo.hstack(
        [
            mo.stat(national_adjusted_chars.height, label="rows"),
            mo.stat(
                f"{national_adjusted_chars['max_cap_mw'].null_count() / national_adjusted_chars.height:.0%}",
                label="max_cap_mw null rate",
            ),
        ]
    )
    return (national_adjusted_chars,)


@app.cell
def national_comparison_title(mo):
    mo.md("""
    ## National side-by-side comparison (gross-load vs. adjusted)
    """)
    return


@app.cell
def national_comparison(
    compare_cols,
    existing_chars,
    fmt_df,
    mo,
    national_adjusted_chars,
    unit_cols,
):
    national_comparison = existing_chars.join(
        national_adjusted_chars,
        on=unit_cols,
        how="inner",
        suffix="_adjusted",
        validate="1:1",
    ).select(
        unit_cols
        + ["plant_id_eia", "state"]
        + [c for pair in compare_cols for c in (pair, f"{pair}_adjusted")]
    )

    mo.vstack(
        [
            mo.hstack(
                [
                    mo.stat(existing_chars.height, label="existing rows"),
                    mo.stat(national_adjusted_chars.height, label="adjusted rows"),
                    mo.stat(national_comparison.height, label="matched rows"),
                ]
            ),
            fmt_df(national_comparison),
        ]
    )
    return (national_comparison,)


@app.cell
def unit_prime_movers(PUDL_OUTPUT, fmt_df, ops_mod, pl):
    _epacamd = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/core_epa__assn_eia_epacamd.parquet"
    )
    _gens = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/out_eia__monthly_generators.parquet"
    )

    _mapping = (
        _epacamd.filter(pl.col("report_year") == ops_mod.LATEST_EPACAMD_CROSSWALK_YEAR)
        .select(
            ["plant_id_epa", "emissions_unit_id_epa", "plant_id_eia", "generator_id"]
        )
        .unique()
    )
    _gen_pm = _gens.filter(
        pl.col("report_date") == pl.date(ops_mod.LATEST_EPACAMD_CROSSWALK_YEAR, 12, 1)
    ).select(["plant_id_eia", "generator_id", "prime_mover_code"])

    # A unit can map to more than one generator_id in the crosswalk (see the
    # fan-out caveat on summarize_eia_generators); take the modal prime mover
    # per unit so every point in the scatter gets exactly one color.
    unit_prime_movers = (
        _mapping.join(_gen_pm, on=["plant_id_eia", "generator_id"], how="left")
        .group_by(["plant_id_epa", "emissions_unit_id_epa"])
        .agg(pl.col("prime_mover_code").mode().first())
        .with_columns(pl.col("prime_mover_code").fill_null("Unknown"))
        .collect()
    )
    fmt_df(
        unit_prime_movers["prime_mover_code"]
        .value_counts()
        .sort("count", descending=True)
    )
    return (unit_prime_movers,)


@app.cell
def unit_fuel_types(PUDL_OUTPUT, fmt_df, ops_mod, pl):
    _epacamd2 = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/core_epa__assn_eia_epacamd.parquet"
    )
    _gens2 = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/out_eia__monthly_generators.parquet"
    )

    _mapping2 = (
        _epacamd2.filter(pl.col("report_year") == ops_mod.LATEST_EPACAMD_CROSSWALK_YEAR)
        .select(
            ["plant_id_epa", "emissions_unit_id_epa", "plant_id_eia", "generator_id"]
        )
        .unique()
    )
    _gen_fuel = _gens2.filter(
        pl.col("report_date") == pl.date(ops_mod.LATEST_EPACAMD_CROSSWALK_YEAR, 12, 1)
    ).select(["plant_id_eia", "generator_id", "fuel_type_code_pudl", "capacity_mw"])

    # Capacity-weighted fuel mix per unit: a unit can map to more than one
    # generator_id (same crosswalk fan-out as prime movers), so classify by the
    # share of mapped capacity in each fuel, not just a majority vote.
    _unit_fuel_capacity = (
        _mapping2.join(_gen_fuel, on=["plant_id_eia", "generator_id"], how="left")
        .group_by(["plant_id_epa", "emissions_unit_id_epa", "fuel_type_code_pudl"])
        .agg(pl.col("capacity_mw").sum())
        .collect()
    )

    OVERWHELMING_FUEL_SHARE_THRESHOLD = 0.9

    _unit_fuel_shares = _unit_fuel_capacity.with_columns(
        (
            pl.col("capacity_mw")
            / pl.col("capacity_mw")
            .sum()
            .over(["plant_id_epa", "emissions_unit_id_epa"])
        ).alias("fuel_share")
    )

    # "Overwhelmingly" coal/gas fired: >=90% of mapped generator capacity on
    # that unit is one fuel. Units without a clear majority fuel are dropped
    # from both groups (not "gas" or "coal").
    unit_fuel_types = (
        _unit_fuel_shares.filter(
            pl.col("fuel_type_code_pudl").is_in(["coal", "gas"])
            & (pl.col("fuel_share") >= OVERWHELMING_FUEL_SHARE_THRESHOLD)
        )
        .select(["plant_id_epa", "emissions_unit_id_epa", "fuel_type_code_pudl"])
        .rename({"fuel_type_code_pudl": "fuel_type"})
    )
    fmt_df(unit_fuel_types["fuel_type"].value_counts())
    return (unit_fuel_types,)


@app.cell
def unit_annual_generation(
    PUDL_OUTPUT,
    eia_epa_mapping_year_input,
    eia_report_date_input,
    mo,
    ops_mod,
    pl,
):
    # Same final year the characteristics tables themselves report (see
    # existing_chars/national_adjusted_chars "report_year").
    GENERATION_WEIGHT_YEAR = 2024

    _cems_lf = pl.scan_parquet(
        f"{PUDL_OUTPUT}/parquet/core_epacems__hourly_emissions.parquet"
    ).filter(pl.col("year") == GENERATION_WEIGHT_YEAR)

    # Annual gross generation per unit -- a plain reduction, no per-unit binning
    # or window functions needed, so this is cheap even at national scale.
    gross_gen_by_unit = (
        _cems_lf.group_by(["plant_id_epa", "emissions_unit_id_epa"])
        .agg(
            (pl.col("gross_load_mw") * pl.col("operating_time_hours"))
            .sum()
            .alias("gross_generation_mwh")
        )
        .collect()
    )

    # Annual net (adjusted) generation per unit: not stored anywhere -- it only
    # ever existed inside add_adjusted_net_generation_to_cems's hourly output,
    # which the asset never persists. Recompute the same pipeline
    # _out_epacems__yearly_operational_characteristics_adjusted uses, but stop
    # at a per-unit annual sum instead of running the full characteristics calc.
    _generators_wt = ops_mod.filter_eia_generators_for_heat_rate_analysis(
        pl.scan_parquet(f"{PUDL_OUTPUT}/parquet/out_eia__monthly_generators.parquet"),
        eia_report_date_input.value,
    )
    _eia_epa_mapping_wt = ops_mod.filter_eia_epa_mapping_for_heat_rate_analysis(
        pl.scan_parquet(f"{PUDL_OUTPUT}/parquet/core_epa__assn_eia_epacamd.parquet"),
        eia_epa_mapping_year_input.value,
    )
    _eia_summaries_wt = ops_mod.summarize_eia_generators(
        _generators_wt, _eia_epa_mapping_wt
    )
    _cems_monthly_wt = ops_mod.summarize_cems_monthly_plant_operations(
        _cems_lf, _eia_summaries_wt["plant"]
    )
    _plant_ids_wt = _cems_lf.select("plant_id_eia").unique().collect().to_series()
    _eia923_monthly_wt = ops_mod.summarize_eia923_monthly_plant_fuel(
        pl.scan_parquet(
            f"{PUDL_OUTPUT}/parquet/core_eia923__monthly_generation_fuel.parquet"
        ).filter(pl.col("report_date").dt.year() == GENERATION_WEIGHT_YEAR),
        _eia_summaries_wt["plant"],
        _plant_ids_wt,
        GENERATION_WEIGHT_YEAR,
    )
    _conversion_factors_wt = ops_mod.estimate_gross_to_net_conversion_factors(
        _cems_monthly_wt, _eia923_monthly_wt
    )
    _adjusted_cems_wt = ops_mod.add_adjusted_net_generation_to_cems(
        _cems_lf, _conversion_factors_wt, _eia_summaries_wt["plant_unit"]
    )
    net_gen_by_unit = (
        _adjusted_cems_wt.group_by(["plant_id_epa", "emissions_unit_id_epa"])
        .agg(pl.col("net_generation_mwh_cems").sum().alias("net_generation_mwh"))
        .collect()
    )

    mo.hstack(
        [
            mo.stat(gross_gen_by_unit.height, label="units w/ gross generation"),
            mo.stat(net_gen_by_unit.height, label="units w/ net generation"),
        ]
    )
    return GENERATION_WEIGHT_YEAR, gross_gen_by_unit, net_gen_by_unit


@app.cell
def heat_rate_scatter(
    PLOT_STYLE,
    PRIME_MOVER_COLORS,
    national_comparison,
    pl,
    plt,
    unit_prime_movers,
):
    heat_rate_pair = "heat_rate_at_max_load_factor_mmbtu_per_mwh"
    scatter_df = (
        national_comparison.select(
            "plant_id_epa",
            "emissions_unit_id_epa",
            "state",
            heat_rate_pair,
            f"{heat_rate_pair}_adjusted",
        )
        .join(
            unit_prime_movers, on=["plant_id_epa", "emissions_unit_id_epa"], how="left"
        )
        .with_columns(pl.col("prime_mover_code").fill_null("Unknown"))
        .drop_nulls([heat_rate_pair, f"{heat_rate_pair}_adjusted"])
    )
    x_vals = scatter_df[heat_rate_pair].to_numpy()
    y_vals = scatter_df[f"{heat_rate_pair}_adjusted"].to_numpy()
    pm_vals = scatter_df["prime_mover_code"].to_numpy()

    # 2x2 grid, one panel per top prime mover code, so each cluster is visible
    # on its own rather than buried under the others.
    TOP_PRIME_MOVERS = ["GT", "ST", "CA", "CT"]

    # Shared square axis range/scale across all four panels, fixed to 0-50 so
    # a perfect 1:1 correlation is still a true 45-degree diagonal and panels
    # stay directly comparable to each other. Points outside this range (a few
    # outlier units) are simply not shown.
    lims = (0, 50)

    with plt.style.context(PLOT_STYLE):
        fig, axes = plt.subplots(
            2, 2, figsize=(22, 22), dpi=160, sharex=True, sharey=True
        )  # ~3520x3520px total
        for _pm_code, _ax in zip(TOP_PRIME_MOVERS, axes.flat, strict=True):
            _mask = pm_vals == _pm_code
            _color = PRIME_MOVER_COLORS[_pm_code]
            _ax.scatter(
                x_vals[_mask],
                y_vals[_mask],
                s=40,
                alpha=0.45,
                linewidths=0,
                color=_color,
            )
            _ax.plot(lims, lims, linestyle="--", linewidth=2, color="#abb2bf")
            _ax.set_xlim(lims)
            _ax.set_ylim(lims)
            _ax.set_aspect("equal", adjustable="box")
            _ax.set_title(f"{_pm_code} (n={_mask.sum()})")
            _ax.grid(True, alpha=0.25)

        for _ax in axes[1, :]:
            _ax.set_xlabel("Gross-load heat rate (MMBtu/MWh)")
        for _ax in axes[:, 0]:
            _ax.set_ylabel("Adjusted (net-generation) heat rate (MMBtu/MWh)")

        fig.suptitle(
            "Heat rate at max load factor: gross-load vs. adjusted, by prime mover"
        )
        fig.tight_layout()

    fig
    return


@app.cell
def heat_rate_histograms(
    GENERATION_WEIGHT_YEAR,
    PLOT_STYLE,
    existing_chars,
    gross_gen_by_unit,
    national_adjusted_chars,
    net_gen_by_unit,
    np,
    pl,
    plt,
    unit_fuel_types,
):
    gross_hr_df = (
        existing_chars.select(
            "plant_id_epa",
            "emissions_unit_id_epa",
            "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        )
        .drop_nulls()
        .join(unit_fuel_types, on=["plant_id_epa", "emissions_unit_id_epa"], how="left")
        .join(
            gross_gen_by_unit, on=["plant_id_epa", "emissions_unit_id_epa"], how="left"
        )
        .with_columns(pl.col("gross_generation_mwh").fill_null(0.0))
    )
    net_hr_df = (
        national_adjusted_chars.select(
            "plant_id_epa",
            "emissions_unit_id_epa",
            "heat_rate_at_max_load_factor_mmbtu_per_mwh",
        )
        .drop_nulls()
        .join(unit_fuel_types, on=["plant_id_epa", "emissions_unit_id_epa"], how="left")
        .join(net_gen_by_unit, on=["plant_id_epa", "emissions_unit_id_epa"], how="left")
        .with_columns(pl.col("net_generation_mwh").fill_null(0.0))
    )

    FUEL_COLORS = {"coal": "#e06c75", "gas": "#61afef"}

    # Shared bin edges across every histogram in the figure, fixed to 4-14 so
    # all 100 bins fall within the visible range instead of being spent on the
    # outlier tails. Points outside 4-14 are excluded from the bin counts, same
    # as points being clipped from view in the scatter.
    HR_XLIM = (4, 14)
    hr_bin_edges = np.linspace(*HR_XLIM, 51)

    with plt.style.context(PLOT_STYLE):
        # Square overall figure, 2 rows x 1 col -> each panel ~2x as wide as tall.
        hist_fig, (ax_gross, ax_net) = plt.subplots(
            2, 1, figsize=(18, 18), dpi=170, sharex=True, sharey=True
        )  # ~3060x3060px

        # (axes, dataframe, weight column, panel label) -- weight is annual
        # generation (MWh) for that unit, so bin height is total energy
        # generated at that heat rate, not a count of units.
        _hist_panels = [
            (ax_gross, gross_hr_df, "gross_generation_mwh", "Gross-load"),
            (ax_net, net_hr_df, "net_generation_mwh", "Adjusted (net-generation)"),
        ]

        for _ax, _df, _weight_col, _panel_label in _hist_panels:
            _col = "heat_rate_at_max_load_factor_mmbtu_per_mwh"
            for _fuel, _color in FUEL_COLORS.items():
                _fuel_df = _df.filter(pl.col("fuel_type") == _fuel)
                _vals = _fuel_df[_col].to_numpy()
                _weights = _fuel_df[_weight_col].to_numpy()
                _ax.hist(
                    _vals,
                    bins=hr_bin_edges,
                    weights=_weights,
                    color=_color,
                    alpha=0.55,
                    label=f"{_fuel} ({_weights.sum() / 1e6:.1f} TWh)",
                )
            _n_shown = _df.filter(pl.col(_col).is_between(*HR_XLIM)).height
            _ax.set_title(
                f"{_panel_label} heat rate at max load factor, weighted by "
                f"{GENERATION_WEIGHT_YEAR} generation\n"
                f"(n={_n_shown} of {_df.height}, "
                f"{_df.height - _n_shown} outside {HR_XLIM} not shown)"
            )
            _ax.set_ylabel("Generation (MWh)")
            _ax.grid(True, alpha=0.25)
            _ax.legend(frameon=False, loc="upper right")

        ax_net.set_xlabel("Heat rate (MMBtu/MWh)")
        ax_gross.set_xlim(*HR_XLIM)
        ax_net.set_xlim(*HR_XLIM)

        hist_fig.suptitle(
            "Generation-weighted heat rate distributions: gross vs. adjusted, coal vs. gas"
        )
        hist_fig.tight_layout()

    hist_fig
    return


if __name__ == "__main__":
    app.run()
