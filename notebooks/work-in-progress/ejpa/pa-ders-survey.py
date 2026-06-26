import marimo

__generated_with = "0.23.10"
app = marimo.App(width="medium")


@app.cell
def imports():
    import os
    import marimo as mo
    import polars as pl
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import matplotx
    import numpy as np
    from pathlib import Path
    from upath import UPath

    matplotlib.style.use(matplotx.styles.onedark)
    plt.rcParams.update({"font.size": 16, "axes.titlesize": 22, "axes.labelsize": 18})
    return Path, UPath, mo, mticker, np, os, pl, plt


@app.cell
def parquet_dir(Path, UPath, os):
    _local = os.environ.get("PUDL_OUTPUT")
    if _local and Path(_local, "parquet").is_dir():
        parquet_dir = UPath(_local) / "parquet"
        parquet_storage_options = {}
    else:
        parquet_dir = UPath("s3://pudl.catalyst.coop/nightly")
        parquet_storage_options = {"region": "us-west-2"}
    return parquet_dir, parquet_storage_options


@app.cell
def net_metering_customer_fuel_class(
    parquet_dir,
    parquet_storage_options,
    pl,
    selected_state,
):
    net_metering_customer_fuel_class = (
        pl.scan_parquet(
            str(parquet_dir / "core_eia861__yearly_net_metering_customer_fuel_class.parquet")
            , storage_options=parquet_storage_options
        )
        .filter(pl.col("state") == selected_state)
        .collect()
    )
    return (net_metering_customer_fuel_class,)


@app.cell
def non_net_metering_customer_fuel_class(
    parquet_dir,
    parquet_storage_options,
    pl,
    selected_state,
):
    non_net_metering_customer_fuel_class = (
        pl.scan_parquet(
            str(parquet_dir / "core_eia861__yearly_non_net_metering_customer_fuel_class.parquet")
            , storage_options=parquet_storage_options
        )
        .filter(pl.col("state") == selected_state)
        .collect()
    )
    return (non_net_metering_customer_fuel_class,)


@app.cell
def gats_registrations(Path, pl, selected_state):
    _gats_path = Path(__file__).parent / "pjm_gats.csv"
    gats_registrations = (
        pl.read_csv(
            _gats_path,
            encoding="latin1",
            infer_schema_length=5000,
        )
        .filter(pl.col("State") == selected_state)
        .rename({"Nameplate": "nameplate_mw"})
        .with_columns(
            pl.col("Date Online").str.to_date("%m/%d/%Y").alias("date_online"),
        )
    )
    return (gats_registrations,)


@app.cell
def pa_generators_eia860(
    parquet_dir,
    parquet_storage_options,
    pl,
    selected_state,
):
    _gen = pl.scan_parquet(str(parquet_dir / "out_eia__yearly_generators.parquet"), storage_options=parquet_storage_options).filter(
        pl.col("state") == selected_state
    )

    solar_generators_eia860 = _gen.filter(
        (pl.col("technology_description") == "Solar Photovoltaic")
        & (pl.col("operational_status") == "existing")
    ).collect()

    battery_generators_eia860 = _gen.filter(
        (pl.col("technology_description") == "Batteries")
        & (pl.col("operational_status") == "existing")
    ).collect()
    return battery_generators_eia860, solar_generators_eia860


@app.cell(hide_code=True)
def pa_dsm_eia861(parquet_dir, parquet_storage_options, pl, selected_state):
    demand_response_eia861 = (
        pl.scan_parquet(str(parquet_dir / "core_eia861__yearly_demand_response.parquet"), storage_options=parquet_storage_options)
        .filter(pl.col("state") == selected_state)
        .collect()
    )

    energy_efficiency_eia861 = (
        pl.scan_parquet(str(parquet_dir / "core_eia861__yearly_energy_efficiency.parquet"), storage_options=parquet_storage_options)
        .filter(pl.col("state") == selected_state)
        .collect()
    )

    ami_eia861 = (
        pl.scan_parquet(
            str(parquet_dir / "core_eia861__yearly_advanced_metering_infrastructure.parquet")
        )
        .filter(pl.col("state") == selected_state)
        .collect()
    )
    return ami_eia861, demand_response_eia861, energy_efficiency_eia861


@app.cell
def tech_labels():
    tech_label_map = {
        "pv": "Solar PV",
        "storage_pv": "PV + Battery Storage",
        "storage_nonpv": "Non-PV Storage",
        "wind": "Wind",
        "chp_cogen": "CHP / Cogen",
        "other": "Other",
        "virtual_pv": "Virtual / Community PV",
    }
    tech_order = [
        "Solar PV",
        "PV + Battery Storage",
        "Non-PV Storage",
        "Wind",
        "CHP / Cogen",
        "Other",
        "Virtual / Community PV",
    ]
    tech_colors = {
        "Solar PV": "#f4a020",
        "PV + Battery Storage": "#2166ac",
        "Non-PV Storage": "#74add1",
        "Wind": "#4dac26",
        "CHP / Cogen": "#b8860b",
        "Other": "#aaaaaa",
        "Virtual / Community PV": "#f7c97e",
    }
    return tech_colors, tech_label_map, tech_order


@app.cell
def size_bin_helpers(pl):
    capacity_bin_edges = [
        (-4.0, -3.5, "0.1–0.3 kW"),
        (-3.5, -3.0, "0.3–1 kW"),
        (-3.0, -2.5, "1–3 kW"),
        (-2.5, -2.0, "3–10 kW"),
        (-2.0, -1.5, "10–32 kW"),
        (-1.5, -1.0, "32–100 kW"),
        (-1.0, -0.5, "100–316 kW"),
        (-0.5,  0.0, "316 kW–1 MW"),
        ( 0.0,  0.5, "1–3 MW"),
        ( 0.5,  1.0, "3–10 MW"),
        ( 1.0,  1.5, "10–32 MW"),
        ( 1.5,  2.0, "32–100 MW"),
        ( 2.0,  2.5, "100–316 MW"),
    ]
    capacity_bin_order = [e[2] for e in capacity_bin_edges]
    capacity_bin_breaks = [e[1] for e in capacity_bin_edges[:-1]]


    def capacity_hist(
        df: pl.DataFrame,
        col: str,
        breaks: list[float],
        labels: list[str],
    ) -> pl.DataFrame:
        """Aggregate a capacity column into log-spaced bins (count and total MW).

        Args:
            df: Input DataFrame.
            col: Name of the capacity column (must be in MW).
            breaks: Log10 break points between bins (right edges, left-closed).
            labels: Bin labels in order; len(labels) == len(breaks) + 1.

        Returns columns: bin (Categorical), count (u32), capacity_mw (f64/f32).
        """
        return (
            df.filter(pl.col(col) > 0)
            .with_columns(pl.col(col).log(base=10).alias("_log10"))
            .with_columns(
                pl.col("_log10").cut(breaks, labels=labels, left_closed=True).alias("bin")
            )
            .group_by("bin")
            .agg(
                pl.len().alias("count"),
                pl.col(col).sum().alias("capacity_mw"),
            )
            .sort("bin")
        )

    return capacity_bin_breaks, capacity_bin_order, capacity_hist


@app.cell
def plot_helpers(mticker):
    FIGSIZE = (16, 10)
    YEAR_XLIM = (2001.5, 2026.5)

    CLASS_COLORS = {
        "commercial": "#61afef",
        "residential": "#e06c75",
        "industrial": "#98c379",
        "transportation": "#c678dd",
    }

    _UNIT_FACTOR = {"": 1.0, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}


    def style_ax(ax):
        ax.set_axisbelow(True)
        ax.grid(axis="y", linewidth=0.5)


    def metric_formatter(max_value: float, base_unit: str = "") -> tuple[str, float]:
        """Pick a display prefix and divisor so displayed numbers stay in 1–1000.

        `max_value` is in units of `base_unit` (e.g. base_unit="M" for megawatts).
        Returns (display_prefix, divisor) where displayed = max_value / divisor.
        """
        import math
        if not max_value or not math.isfinite(max_value) or max_value <= 0:
            return (base_unit, 1.0)
        base_factor = _UNIT_FACTOR.get(base_unit, 1.0)
        abs_watts = max_value * base_factor
        if abs_watts >= 1e12:
            return ("T", 1e12 / base_factor)
        if abs_watts >= 1e9:
            return ("G", 1e9 / base_factor)
        if abs_watts >= 1e6:
            return ("M", 1e6 / base_factor)
        if abs_watts >= 1e3:
            return ("k", 1e3 / base_factor)
        return ("", 1.0)


    def metric_tick_formatter(prefix: str, divisor: float):
        """Format ticks with the given prefix/divisor, dropping decimals for integers."""
        def _fmt(x, _):
            v = x / divisor
            if v == 0:
                return "0"
            if abs(v - round(v)) < 1e-9:
                return f"{round(v):,.0f}"
            return f"{v:,.1f}"
        return mticker.FuncFormatter(_fmt)


    def prefixed_unit(prefix: str, base_unit: str = "W") -> str:
        """Combine a metric prefix with a unit symbol, e.g. ('G', 'W') -> 'GW'."""
        return f"{prefix}{base_unit}" if prefix else base_unit


    def count_formatter(max_value: float) -> tuple[str, float, str]:
        """Pick a suffix/divisor for entity-count axes (e.g. 1500 -> ('K', 1e3, 'thousands')).

        Returns (suffix, divisor, label_word) where label_word is '' / 'thousands' /
        'millions' for use in axis labels like "Customers (thousands)".
        """
        import math
        if not max_value or not math.isfinite(max_value) or max_value <= 0:
            return ("", 1.0, "")
        if max_value >= 1e6:
            return ("M", 1e6, "millions")
        if max_value >= 1e3:
            return ("K", 1e3, "thousands")
        return ("", 1.0, "")


    def count_tick_formatter(suffix: str, divisor: float):
        """Format count ticks with K/M suffix, dropping decimals for integers."""
        def _fmt(x, _):
            v = x / divisor
            if v == 0:
                return "0"
            if abs(v - round(v)) < 1e-9:
                return f"{round(v):,.0f}{suffix}"
            return f"{v:,.1f}{suffix}"
        return mticker.FuncFormatter(_fmt)

    return (
        CLASS_COLORS,
        FIGSIZE,
        YEAR_XLIM,
        count_formatter,
        count_tick_formatter,
        metric_formatter,
        metric_tick_formatter,
        prefixed_unit,
        style_ax,
    )


@app.cell
def net_metering_by_tech(net_metering_customer_fuel_class, pl):
    net_metering_by_tech = (
        net_metering_customer_fuel_class.filter(~pl.col("tech_class").is_in(["total"]))
        .group_by(
            pl.col("report_date").dt.year().alias("year"),
            "tech_class",
        )
        .agg(
            pl.col("capacity_mw").sum(),
            pl.col("energy_capacity_mwh").sum(),
            pl.col("sold_to_utility_mwh").sum(),
            pl.col("customers").sum(),
        )
        .sort("year", "tech_class")
    )
    return (net_metering_by_tech,)


@app.cell
def non_net_metering_by_tech(non_net_metering_customer_fuel_class, pl):
    non_net_metering_by_tech = (
        non_net_metering_customer_fuel_class.filter(
            (pl.col("customer_class") == "total") & (pl.col("tech_class") != "total")
        )
        .group_by(
            pl.col("report_date").dt.year().alias("year"),
            "tech_class",
        )
        .agg(pl.col("capacity_mw").sum(), pl.col("energy_capacity_mwh").sum())
        .sort("year", "tech_class")
    )
    return (non_net_metering_by_tech,)


@app.cell
def net_metering_data(net_metering_by_tech, pl, tech_label_map, tech_order):
    net_metering_data = net_metering_by_tech.with_columns(
        pl.col("tech_class").replace(tech_label_map).alias("technology"),
    ).filter(pl.col("technology").is_in(tech_order))
    return (net_metering_data,)


@app.cell
def gats_annual(gats_registrations, pl):
    _gats = gats_registrations.with_columns(
        pl.col("Primary Fuel Type").str.strip_chars().alias("fuel_type")
    )


    def _gats_cumulative(fuel: str) -> pl.DataFrame:
        return (
            _gats.filter(
                (pl.col("fuel_type") == fuel)
                & (pl.col("date_online").dt.year() >= 2002)
                & (pl.col("date_online").dt.year() <= 2026)
            )
            .group_by(pl.col("date_online").dt.year().cast(pl.Int64).alias("year"))
            .agg(pl.col("nameplate_mw").sum().alias("added_mw"), pl.len().alias("n_systems"))
            .sort("year")
            .with_columns(pl.col("added_mw").cum_sum().alias("cumulative_mw"))
        )


    gats_solar_annual = _gats_cumulative("SUN")
    gats_ee_annual = _gats_cumulative("EE")

    _all_years = pl.DataFrame({"year": list(range(2002, 2027))})
    gats_solar_annual = _all_years.join(
        gats_solar_annual, on="year", how="left"
    ).with_columns(pl.col("cumulative_mw").forward_fill().fill_null(0))
    gats_ee_annual = _all_years.join(gats_ee_annual, on="year", how="left").with_columns(
        pl.col("cumulative_mw").forward_fill().fill_null(0)
    )
    return gats_ee_annual, gats_solar_annual


@app.cell
def generators_annual_eia860(
    battery_generators_eia860,
    pl,
    solar_generators_eia860,
):
    def _annual_capacity(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by(pl.col("report_date").dt.year().alias("year"))
            .agg(pl.col("capacity_mw").sum().alias("total_mw"))
            .sort("year")
        )


    def _annual_additions(df: pl.DataFrame) -> pl.DataFrame:
        _latest = (
            df.sort("report_date", descending=True)
            .unique(subset=["plant_id_eia", "generator_id"], keep="first")
            .filter(pl.col("generator_operating_date").is_not_null())
            # Generators that change plant_id share the same generator_id, operating_date,
            # AND capacity; adding capacity_mw prevents merging distinct generators that
            # happen to share generator_id + operating_date at different plants.
            .unique(
                subset=["generator_id", "generator_operating_date", "capacity_mw"],
                keep="first",
            )
        )
        _all_years = pl.DataFrame(
            {"year": list(range(2008, 2027))}, schema={"year": pl.Int32}
        )
        _adds = (
            _latest.group_by(
                pl.col("generator_operating_date").dt.year().cast(pl.Int32).alias("year")
            )
            .agg(
                pl.col("capacity_mw").sum().alias("added_mw"),
                pl.len().alias("n_generators"),
            )
            .sort("year")
        )
        return _all_years.join(_adds, on="year", how="left").with_columns(
            pl.col("added_mw").fill_null(0),
            pl.col("n_generators").fill_null(0),
        )


    solar_capacity_annual = _annual_capacity(solar_generators_eia860)
    solar_additions_annual = _annual_additions(solar_generators_eia860)
    battery_capacity_annual = _annual_capacity(battery_generators_eia860)
    battery_additions_annual = _annual_additions(battery_generators_eia860)
    return (
        battery_additions_annual,
        solar_additions_annual,
        solar_capacity_annual,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Initial Thoughts on PA DERs
    - There's ~1 GW of net-metered Solar PV reported in EIA-861.
    - There's basically no PV reported as non-net-meterd in EIA-861.
    - Almost none of the net-metered Solar PV is reported as having battery storage associated with it.
    - There's almost 3x that much PV reported in GATS as of early 2026.
    - EIA-861 net-metered PV and GATS PV were almost identical to each other through 2019, and still very close through 2022. From 2023 on GATS has grown very rapidly.
    - About 2/3 of the GATS projects are larger than you'd expect for residential. There's almost 1 GW of projets > 10MW in size. I imagine that these big projects took off in 2023 (maybe IRA inspired?) and account for the 2/3 of projects that aren't showing up in the EIA-861 Net Metering data. We could look at how project size has evolved over time in the GATS data.
    - EE projects reported in GATS are only 10s of MW.
    - EE projects reported in EIA-861 are like 200 MW. Currently dominated by commercial.
    - Residential DR programs are almost nonexistent. 40 MW and only one utility reporting to EIA-861 (PECO discontinued in 2021)
    - Industrial DR programs **were** ~600 MW in 2020, but the requirements became much more stringent in 2021 and everybody bailed.
    - Utility scale solar PV from EIA-860 is ~1GW, which seems to line up fairly well with the GATS data.
    - There's basically no battery storage reported in PA at utility scale either.
    - Bi-directional AMI is basically universal according to EIA-861.

    ## Policies?
    - Seems like there's lots of room for adding cheap batteries to existing solar to allow peak shifting and displace gas peakers.
    - At least based on the EIA-861 data, it also seems like there's a lot of room for modern demand response programs, especially given universal bi-directional AMI.
    """)
    return


@app.cell(hide_code=True)
def chart_capacity(
    FIGSIZE,
    YEAR_XLIM,
    metric_formatter,
    metric_tick_formatter,
    net_metering_data,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
    tech_colors,
    tech_order,
):
    # NOTE: Virtual/Community PV shows 397 customers and 32.9 MW in 2022 but
    # zero in 2023-2024. PA utilities appear to have stopped reporting this
    # category separately; capacity may have been absorbed into plain "pv".
    _techs = tech_order
    _pivot = (
        net_metering_data.filter(pl.col("technology").is_in(_techs))
        .sort("year")
        .pivot(
            on="technology", index="year", values="capacity_mw", aggregate_function="sum"
        )
        .fill_null(0)
        .sort("year")
    )
    _years = _pivot["year"].to_list()

    _fig, _ax = plt.subplots(figsize=FIGSIZE)
    _bottom = [0.0] * len(_years)
    for _t in _techs:
        _vals = _pivot[_t].to_list()
        _ax.bar(_years, _vals, bottom=_bottom, label=_t, color=tech_colors[_t], width=0.8)
        _bottom = [b + v for b, v in zip(_bottom, _vals)]

    _ax.set_title(f"{state_name} Net-Metered DER Capacity by Technology (EIA-861)")
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Installed Capacity (MW)")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _stack_max = max(_bottom) if _bottom else 0
    _pfx, _div = metric_formatter(_stack_max, base_unit="M")
    _ax.set_ylim(0)
    _ax.yaxis.set_major_formatter(metric_tick_formatter(_pfx, _div))
    _ax.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx, 'W')})")
    style_ax(_ax)
    plt.tight_layout()
    chart_capacity = _fig
    chart_capacity
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Net-metered capacity is growing and PV dominated
    * PA net metered capacity has been growing rapidly since around 2016, and is dominated by solar PV.
    * As of 2024 (the most recent EIA-861 data) the total net-metered capacity was about 1 GW.
    * Starting in 2023 it seems that Community Solar maybe have been absorbed into the Solar PV category.
    * Solar with storage is reported, but is a miniscule amount of capacity. Hardly visible on the chart.
    """)
    return


@app.cell(hide_code=True)
def chart_non_net_metered_capacity(
    FIGSIZE,
    YEAR_XLIM,
    metric_formatter,
    metric_tick_formatter,
    non_net_metering_by_tech,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _nnm_label_map = {
        "internal_combustion": "Internal Combustion",
        "combustion_turbine": "Combustion Turbine",
        "fuel_cell": "Fuel Cell",
        "hydro": "Hydro",
        "steam": "Steam",
        "all_storage": "Storage",
        "pv": "Solar PV",
        "wind": "Wind",
        "other": "Other",
    }
    _nnm_tech_order = [
        "Solar PV",
        "Wind",
        "Hydro",
        "Storage",
        "Fuel Cell",
        "Internal Combustion",
        "Combustion Turbine",
        "Steam",
        "Other",
    ]
    _nnm_colors = {
        "Solar PV": "#f4a020",
        "Wind": "#4dac26",
        "Hydro": "#1f78b4",
        "Storage": "#74add1",
        "Fuel Cell": "#9b59b6",
        "Internal Combustion": "#c0392b",
        "Combustion Turbine": "#e67e22",
        "Steam": "#795548",
        "Other": "#aaaaaa",
    }

    _nnm_pivot = (
        non_net_metering_by_tech.with_columns(
            pl.col("tech_class").replace(_nnm_label_map).alias("technology")
        )
        .group_by("year", "technology")
        .agg(pl.col("capacity_mw").sum())
        .pivot(
            on="technology", index="year", values="capacity_mw", aggregate_function="sum"
        )
        .fill_null(0)
        .sort("year")
    )
    _nnm_years = _nnm_pivot["year"].to_list()
    _nnm_techs = [t for t in _nnm_tech_order if t in _nnm_pivot.columns]

    _fig, _ax = plt.subplots(figsize=FIGSIZE)
    _bottom = [0.0] * len(_nnm_years)
    for _t in _nnm_techs:
        _vals = _nnm_pivot[_t].to_list()
        _ax.bar(
            _nnm_years,
            _vals,
            bottom=_bottom,
            label=_t,
            color=_nnm_colors[_t],
            width=0.8,
        )
        _bottom = [b + v for b, v in zip(_bottom, _vals)]

    _ax.set_title(f"{state_name} Non-Net-Metered BTM DER Capacity by Technology (EIA-861)")
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Installed Capacity")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _stack_max = max(_bottom) if _bottom else 0
    _pfx, _div = metric_formatter(_stack_max, base_unit="M")
    _ax.set_ylim(0)
    _ax.yaxis.set_major_formatter(metric_tick_formatter(_pfx, _div))
    _ax.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx, 'W')})")
    style_ax(_ax)
    plt.tight_layout()
    chart_non_net_metered_capacity = _fig
    chart_non_net_metered_capacity
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Non-net metered behind the meter capacity is minimal
    * Only a couple of MW, and virtually all of it is backup generators.
    * At least within the EIA-861 reporting scheme, substantially all the DERs appear to be enrolled in net-metering.
    """)
    return


@app.cell(hide_code=True)
def chart_customers(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    count_formatter,
    count_tick_formatter,
    net_metering_customer_fuel_class,
    pl,
    plt,
    state_name,
    style_ax,
):
    _classes = ["residential", "commercial", "industrial", "transportation"]

    _cust_pivot = (
        net_metering_customer_fuel_class.filter(pl.col("tech_class") == "total")
        .group_by([pl.col("report_date").dt.year().alias("year"), "customer_class"])
        .agg(pl.col("customers").sum())
        .pivot(
            on="customer_class", index="year", values="customers", aggregate_function="sum"
        )
        .fill_null(0)
        .sort("year")
    )
    _years = _cust_pivot["year"].to_list()

    _fig, _ax = plt.subplots(figsize=FIGSIZE)
    _bottom = [0.0] * len(_years)
    for _cls in _classes:
        if _cls in _cust_pivot.columns:
            _vals = _cust_pivot[_cls].to_list()
            _ax.bar(
                _years,
                _vals,
                bottom=_bottom,
                label=_cls.capitalize(),
                color=CLASS_COLORS[_cls],
                width=0.8,
            )
            _bottom = [b + v for b, v in zip(_bottom, _vals)]

    _ax.set_title(f"{state_name} Net Metering Customer Count by Customer Class (EIA-861)")
    _ax.set_xlabel("Year")

    _stack_max = max(_bottom) if _bottom else 0
    _csfx, _cdiv, _cword = count_formatter(_stack_max)
    _ax.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
    _label_suffix = f" ({_cword})" if _cword else ""
    _ax.set_ylabel(f"Net Metering Customers{_label_suffix}")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    style_ax(_ax)
    plt.tight_layout()
    chart_customers = _fig
    chart_customers
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Net metering customer counts growing, dominated by residential
    * Also starting around 2016, the number of net metering customers started growing rapidly, and has especially taken off since 2022.
    * By number of customers, residential is the overwhelmin majority, with a small number of commercial and a tiny handful of industrial net metering customers.
    """)
    return


@app.cell(hide_code=True)
def chart_storage(
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    net_metering_data,
    pl,
    plt,
    state_name,
    style_ax,
):
    _pa_s = net_metering_data.filter(pl.col("technology") == "PV + Battery Storage").sort(
        "year"
    )
    _years = _pa_s["year"].to_list()

    import math
    _cap_max = float(_pa_s["capacity_mw"].max() or 0)
    # Round up to the next even integer for clean tick alignment with step=2.
    _cap_step = 2
    _y_cap = math.ceil((_cap_max + 1) / _cap_step) * _cap_step
    _y_cust = _y_cap * 100

    _fig, _ax1 = plt.subplots(figsize=FIGSIZE)
    _ax2 = _ax1.twinx()

    (_l1,) = _ax1.plot(
        _years,
        _pa_s["capacity_mw"],
        color="#2166ac",
        linewidth=5,
        marker="o",
        markersize=10,
        label="Inverter Capacity (MW)",
    )
    (_l2,) = _ax2.plot(
        _years,
        _pa_s["customers"],
        color="#74add1",
        linewidth=5,
        marker="o",
        markersize=10,
        label="Customers",
    )

    _ax1.set_ylim(0, _y_cap)
    _ax2.set_ylim(0, _y_cust)
    _ax1.yaxis.set_major_locator(mticker.MultipleLocator(_cap_step))
    _ax2.yaxis.set_major_locator(mticker.MultipleLocator(_cap_step * 100))
    _ax1.set_xlabel("Year")
    _ax1.set_ylabel("Inverter Capacity (MW)", color="#2166ac")
    _ax2.set_ylabel("Customers", color="#74add1")
    _ax1.tick_params(axis="y", labelcolor="#2166ac")
    _ax2.tick_params(axis="y", labelcolor="#74add1")
    _ax1.set_title(f"{state_name} Net-Metered PV+Battery Storage, Co-Located (EIA-861)")
    _ax1.legend(handles=[_l1, _l2], loc="upper left")
    _ax1.set_xlim(*YEAR_XLIM)
    style_ax(_ax1)
    plt.tight_layout()
    chart_storage_combined = _fig
    chart_storage_combined
    return


@app.cell(hide_code=True)
def _data_notes(mo):
    mo.md(r"""
    ## Solar + Storage Capacity is Minimal

    * Only ~1,300 of the more than 80,000 net-metering customers report having associated storage.
    * The total reported **inverter** capacity is only on the order of ~10 MW.
    * Energy storage capacity (MWh) of the associated batteries is not reported.

    **Note on generation data:** EIA-861 collects `sold_to_utility_mwh` (energy sold
    back to the grid) but reporting is voluntary and sparse for Pennsylvania — only
    a handful of utilities reported values in select years (2010–2021), with no data
    for 2022–2024. This column is **not representative of state-wide generation**
    and is omitted from the charts above.

    **Note on battery storage capacity:** The storage chart above shows
    `capacity_mw` for the `storage_pv` tech class, which is the **inverter/charger
    power capacity (MW)** of batteries co-located with PV, not the actual energy
    storage capacity (MWh). The `energy_capacity_mwh` field is nearly absent for PA
    (only two records from a single utility in 2024). In any case, the fraction of
    net-metering PV systems reported as having battery storage is so small that it
    can probably be ignored for policy purposes.
    """)
    return


@app.cell(hide_code=True)
def chart_gats_solar(
    FIGSIZE,
    YEAR_XLIM,
    gats_solar_annual,
    metric_formatter,
    metric_tick_formatter,
    net_metering_by_tech,
    non_net_metering_by_tech,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _eia_nm = (
        net_metering_by_tech.filter(pl.col("tech_class") == "pv")
        .select([pl.col("year").cast(pl.Int64), pl.col("capacity_mw").cast(pl.Float64)])
        .sort("year")
    )
    _eia_nnm = (
        non_net_metering_by_tech.filter(pl.col("tech_class") == "pv")
        .select([pl.col("year").cast(pl.Int64), pl.col("capacity_mw").cast(pl.Float64)])
        .sort("year")
    )
    _gats = gats_solar_annual.select(
        ["year", pl.col("cumulative_mw").alias("capacity_mw")]
    ).sort("year")

    _fig, _ax = plt.subplots(figsize=FIGSIZE)
    _ax.plot(
        _gats["year"],
        _gats["capacity_mw"],
        color="#f4a020",
        linewidth=5,
        marker="o",
        markersize=10,
        label="GATS: All Registered Solar",
    )
    _ax.plot(
        _eia_nm["year"],
        _eia_nm["capacity_mw"],
        color="#d62728",
        linewidth=5,
        marker="o",
        markersize=10,
        label="EIA-861: Net-Metered Solar",
    )
    _ax.plot(
        _eia_nnm["year"],
        _eia_nnm["capacity_mw"],
        color="#8c564b",
        linewidth=5,
        marker="o",
        markersize=10,
        label="EIA-861: Non-Net-Metered Solar",
    )
    _ax.set_title(
        f"{state_name} Solar PV — GATS All Registered vs. EIA-861 Net-Metered vs. Non-Net-Metered"
    )
    _ax.set_xlabel("Year")

    _ymax = float(max(_gats["capacity_mw"].max(), _eia_nm["capacity_mw"].max(), _eia_nnm["capacity_mw"].max()) or 0)
    _pfx, _div = metric_formatter(_ymax, base_unit="M")
    _ax.yaxis.set_major_formatter(metric_tick_formatter(_pfx, _div))
    _ax.set_ylabel(f"Cumulative Capacity ({prefixed_unit(_pfx, 'W')})")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    style_ax(_ax)
    plt.tight_layout()
    chart_gats_solar = _fig
    chart_gats_solar
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Reported GATS Solar PV is much larger than EIA-861
    * The two are nearly identical up through 2018.
    * GATS pulls slightly ahead through 2022.
    * In 2023-2024 GATS Solar PV jumps dramatically, climbing to more than 2x EIA-861 net-metered solar.
    * By 2026 GATS PV is close to 3 GW while 2024 EIA-861 PV is only 1 GW.
    """)
    return


@app.cell(hide_code=True)
def chart_gats_additions(
    FIGSIZE,
    YEAR_XLIM,
    gats_solar_annual,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _gadd = gats_solar_annual.filter(pl.col("added_mw").is_not_null()).sort("year")
    _gcum = gats_solar_annual.sort("year")

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_gadd["year"], _gadd["added_mw"], color="#f4a020", width=0.8)
    _add_max = float(_gadd["added_mw"].max() or 0)
    _pfx1, _div1 = metric_formatter(_add_max, base_unit="M")
    _ax1.set_title(f"{state_name} Solar PV — Annual Capacity Additions (GATS)")
    _ax1.set_ylabel(f"Capacity Added ({prefixed_unit(_pfx1, 'W')})")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(metric_tick_formatter(_pfx1, _div1))
    style_ax(_ax1)

    _ax2.plot(
        _gcum["year"],
        _gcum["cumulative_mw"],
        color="#f4a020",
        linewidth=5,
        marker="o",
        markersize=10,
    )
    _ax2.fill_between(_gcum["year"], _gcum["cumulative_mw"], alpha=0.25, color="#f4a020")
    _cum_max = float(_gcum["cumulative_mw"].max() or 0)
    _pfx2, _div2 = metric_formatter(_cum_max, base_unit="M")
    _ax2.set_title(f"{state_name} Solar PV — Cumulative GATS-Registered Capacity")
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel(f"Cumulative Capacity ({prefixed_unit(_pfx2, 'W')})")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    style_ax(_ax2)

    plt.tight_layout()
    chart_gats_additions = _fig
    chart_gats_additions
    return


@app.cell(hide_code=True)
def chart_gats_size_hist(
    FIGSIZE,
    capacity_bin_breaks,
    capacity_bin_order,
    capacity_hist,
    count_formatter,
    count_tick_formatter,
    gats_registrations,
    metric_formatter,
    metric_tick_formatter,
    np,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _sun_all = gats_registrations.filter(pl.col("Primary Fuel Type") == "SUN")
    _hist_sun = capacity_hist(_sun_all, 'nameplate_mw', capacity_bin_breaks, capacity_bin_order)
    _n_sun = _sun_all.height

    _sun_x = np.arange(len(capacity_bin_order))
    _sun_counts = [_hist_sun.filter(pl.col("bin") == b)["count"].sum() for b in capacity_bin_order]
    _sun_mw = [_hist_sun.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in capacity_bin_order]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_sun_x, _sun_counts, color="#f4a020")
    _ax1.set_title(
        f"{state_name} Solar PV — System Count by Size, All Online Systems (GATS, n={_n_sun:,})"
    )
    _ax1.set_ylabel("Number of Systems")
    _ax1.set_xticks(_sun_x)
    _ax1.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max = max(_sun_counts) if _sun_counts else 0
    _csfx, _cdiv, _cword = count_formatter(_mw_max)
    _ax1.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
    style_ax(_ax1)

    _ax2.bar(_sun_x, _sun_mw, color="#f4a020")
    _ax2.set_title(
        f"{state_name} Solar PV — Installed Capacity (MW) by Size, All Online Systems (GATS)"
    )

    _ax2.set_xlabel("System Size")
    _ax2.set_xticks(_sun_x)
    _ax2.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max2 = max(_sun_mw) if _sun_mw else 0
    _pfx2, _div2 = metric_formatter(_mw_max2, base_unit="M")
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    style_ax(_ax2)

    plt.tight_layout()
    chart_gats_size_hist = _fig
    chart_gats_size_hist
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## GATS Solar PV includes and is dominated by larger systems
    * The number of PV systems reported in GATS is similar to those in EIA-861, with a big peak between 3-30kW likely corresponding to residential rooftop systems.
    * However, the GATS data has a long tail of larger systems, with nearly 1.5 GW of capacity in systems larrger than 10 MW.
    * Given EIA-860 reporting requirements we'd expect to see most of the GATS capacity show up there, with only the 30kW-1MW C&I systems (maybe 400 MW total capacity) missing from both EIA-860 and EIA-861.
    """)
    return


@app.cell(hide_code=True)
def chart_ee_size_hist(
    FIGSIZE,
    capacity_bin_breaks,
    capacity_bin_order,
    capacity_hist,
    count_formatter,
    count_tick_formatter,
    gats_registrations,
    metric_formatter,
    metric_tick_formatter,
    np,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _ee_all = gats_registrations.filter(
        pl.col("Primary Fuel Type").str.strip_chars() == "EE"
    )
    _hist_ee = capacity_hist(_ee_all, 'nameplate_mw', capacity_bin_breaks, capacity_bin_order)
    _n_ee = _ee_all.height

    _ee_x = np.arange(len(capacity_bin_order))
    _ee_counts = [_hist_ee.filter(pl.col("bin") == b)["count"].sum() for b in capacity_bin_order]
    _ee_mw = [_hist_ee.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in capacity_bin_order]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_ee_x, _ee_counts, color="#2ca02c")
    _ax1.set_title(
        f"{state_name} Energy Efficiency — Registration Count by Size, All Systems (GATS, n={_n_ee:,})"
    )
    _ax1.set_ylabel("Number of EE Registrations")
    _ax1.set_xticks(_ee_x)
    _ax1.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max = max(_ee_counts) if _ee_counts else 0
    _csfx, _cdiv, _cword = count_formatter(_mw_max)
    _ax1.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
    style_ax(_ax1)

    _ax2.bar(_ee_x, _ee_mw, color="#2ca02c")
    _ax2.set_title(
        f"{state_name} Energy Efficiency — Registered Capacity (MW) by Size, All Systems (GATS)"
    )

    _ax2.set_xlabel("System Size")
    _ax2.set_xticks(_ee_x)
    _ax2.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max2 = max(_ee_mw) if _ee_mw else 0
    _pfx2, _div2 = metric_formatter(_mw_max2, base_unit="M")
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    style_ax(_ax2)

    plt.tight_layout()
    chart_ee_size_hist = _fig
    chart_ee_size_hist
    return


@app.cell(hide_code=True)
def chart_ee_cumulative(
    FIGSIZE,
    YEAR_XLIM,
    gats_ee_annual,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _ee_add = gats_ee_annual.filter(pl.col("added_mw").is_not_null()).sort("year")
    _ee_cum = gats_ee_annual.sort("year")

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_ee_add["year"], _ee_add["added_mw"], color="#2ca02c", width=0.8)
    _add_max = float(_ee_add["added_mw"].max() or 0)
    _pfx1, _div1 = metric_formatter(_add_max, base_unit="M")
    _ax1.set_title(f"{state_name} Energy Efficiency Resources — Annual Additions (GATS)")
    _ax1.set_ylabel(f"Capacity Added ({prefixed_unit(_pfx1, 'W')})")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(metric_tick_formatter(_pfx1, _div1))
    style_ax(_ax1)

    _ax2.plot(
        _ee_cum["year"],
        _ee_cum["cumulative_mw"],
        color="#2ca02c",
        linewidth=5,
        marker="o",
        markersize=10,
    )
    _ax2.fill_between(
        _ee_cum["year"], _ee_cum["cumulative_mw"], alpha=0.25, color="#2ca02c"
    )
    _cum_max = float(_ee_cum["cumulative_mw"].max() or 0)
    _pfx2, _div2 = metric_formatter(_cum_max, base_unit="M")
    _ax2.set_title(
        f"{state_name} Energy Efficiency Resources — Cumulative GATS-Registered Capacity"
    )
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel(f"Cumulative Capacity ({prefixed_unit(_pfx2, 'W')})")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    style_ax(_ax2)

    plt.tight_layout()
    chart_ee_cumulative = _fig
    chart_ee_cumulative
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## GATS energy efficiency is modest, dominated by large projects
    * Like the Solar PV, the GATS EE projects show a large number of small systems, but a small number or large systems end up dominating the overall capacity.
    * Total capacity of all registered EE systems is only ~100MW.
    """)
    return


@app.cell(hide_code=True)
def chart_eia860_solar_hist(
    FIGSIZE,
    capacity_bin_breaks,
    capacity_bin_order,
    capacity_hist,
    count_formatter,
    count_tick_formatter,
    metric_formatter,
    metric_tick_formatter,
    np,
    pl,
    plt,
    prefixed_unit,
    solar_generators_eia860,
    state_name,
    style_ax,
):
    _solar_snap = solar_generators_eia860.sort("report_date", descending=True).unique(
        subset=["plant_id_eia", "generator_id"], keep="first"
    )
    _hist_solar = capacity_hist(_solar_snap, 'capacity_mw', capacity_bin_breaks, capacity_bin_order)
    _n_solar = _solar_snap.height
    _sx = np.arange(len(capacity_bin_order))
    _solar_counts = [
        _hist_solar.filter(pl.col("bin") == b)["count"].sum() for b in capacity_bin_order
    ]
    _solar_mw = [
        _hist_solar.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in capacity_bin_order
    ]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_sx, _solar_counts, color="#f4a020")
    _ax1.set_title(
        f"{state_name} Utility-Scale Solar PV — Generator Count by Size (EIA-860, n={_n_solar})"
    )
    _ax1.set_ylabel("Number of Generators")
    _ax1.set_xticks(_sx)
    _ax1.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max = max(_solar_counts) if _solar_counts else 0
    _csfx, _cdiv, _cword = count_formatter(_mw_max)
    _ax1.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
    style_ax(_ax1)

    _ax2.bar(_sx, _solar_mw, color="#f4a020")
    _ax2.set_title(f"{state_name} Utility-Scale Solar PV — Installed Capacity (MW) by Size (EIA-860)")

    _ax2.set_xlabel("Generator Size")
    _ax2.set_xticks(_sx)
    _ax2.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max2 = max(_solar_mw) if _solar_mw else 0
    _pfx2, _div2 = metric_formatter(_mw_max2, base_unit="M")
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_solar_hist = _fig
    chart_eia860_solar_hist
    return


@app.cell(hide_code=True)
def chart_eia860_solar_capacity(
    FIGSIZE,
    YEAR_XLIM,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    solar_additions_annual,
    solar_capacity_annual,
    state_name,
    style_ax,
):
    _sadd = solar_additions_annual.filter(pl.col("year") <= 2026)
    _sadd_years = _sadd["year"].to_list()
    _sadd_mw = _sadd["added_mw"].to_list()

    _scum = solar_capacity_annual.filter(pl.col("year") <= 2026)
    _scum_years = _scum["year"].to_list()
    _scum_mw = _scum["total_mw"].to_list()  # MW

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_sadd_years, _sadd_mw, color="#f4a020")
    _add_max = float(_sadd["added_mw"].max() or 0)
    _pfx1, _div1 = metric_formatter(_add_max, base_unit="M")
    _ax1.set_title(f"{state_name} Utility-Scale Solar PV — Annual Capacity Additions (EIA-860)")
    _ax1.set_ylabel(f"Capacity Added ({prefixed_unit(_pfx1, 'W')})")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(metric_tick_formatter(_pfx1, _div1))
    style_ax(_ax1)

    _ax2.plot(
        _scum_years, _scum_mw, color="#f4a020", linewidth=5, marker="o", markersize=10
    )
    _ax2.fill_between(_scum_years, _scum_mw, alpha=0.25, color="#f4a020")
    _ax2.set_title(f"{state_name} Utility-Scale Solar PV — Cumulative Installed Capacity (EIA-860)")
    _ax2.set_xlabel("Year")
    _cum_max = float(max(_scum_mw) or 0)  # MW
    _pfx2, _div2 = metric_formatter(_cum_max, base_unit="M")
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_solar_capacity = _fig
    chart_eia860_solar_capacity
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Utility scale solar is comparable to rooftop in scale
    * Based on the EIA-860 reporting, there's only slightly more utility-scale solar than rooftop solar by capacity. Both are around 1 GW.
    * Like rooftop solar, utility-scale solar has grown rapidly since 2022.
    """)
    return


@app.cell(hide_code=True)
def chart_eia860_batt_hist(
    FIGSIZE,
    battery_generators_eia860,
    capacity_bin_breaks,
    capacity_bin_order,
    capacity_hist,
    count_formatter,
    count_tick_formatter,
    metric_formatter,
    metric_tick_formatter,
    np,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _batt_snap = battery_generators_eia860.sort("report_date", descending=True).unique(
        subset=["plant_id_eia", "generator_id"], keep="first"
    )
    _hist_batt = capacity_hist(_batt_snap, 'capacity_mw', capacity_bin_breaks, capacity_bin_order)
    _n_batt = _batt_snap.height
    _bx = np.arange(len(capacity_bin_order))
    _batt_counts = [
        _hist_batt.filter(pl.col("bin") == b)["count"].sum() for b in capacity_bin_order
    ]
    _batt_mw = [
        _hist_batt.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in capacity_bin_order
    ]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_bx, _batt_counts, color="#5ba4cf")
    _ax1.set_title(
        f"{state_name} Utility-Scale Battery Storage — Generator Count by Size (EIA-860, n={_n_batt})"
    )
    _ax1.set_ylabel("Number of Generators")
    _ax1.set_xticks(_bx)
    _ax1.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max = max(_batt_counts) if _batt_counts else 0
    _csfx, _cdiv, _cword = count_formatter(_mw_max)
    _ax1.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
    style_ax(_ax1)

    _ax2.bar(_bx, _batt_mw, color="#5ba4cf")
    _ax2.set_title(f"{state_name} Utility-Scale Battery Storage — Installed Capacity (MW) by Size (EIA-860)")

    _ax2.set_xlabel("Generator Size")
    _ax2.set_xticks(_bx)
    _ax2.set_xticklabels(capacity_bin_order, rotation=30, ha="right")
    _mw_max2 = max(_batt_mw) if _batt_mw else 0
    _pfx2, _div2 = metric_formatter(_mw_max2, base_unit="M")
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_batt_hist = _fig
    chart_eia860_batt_hist
    return


@app.cell(hide_code=True)
def chart_eia860_batt_capacity(
    FIGSIZE,
    YEAR_XLIM,
    battery_additions_annual,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _badd = battery_additions_annual.filter(pl.col("year") <= 2025)
    _badd_years = _badd["year"].to_list()
    _badd_mw = _badd["added_mw"].to_list()

    # Derive cumulative from additions (operating-date basis) so both panels are consistent
    _bcum = battery_additions_annual.filter(pl.col("year") <= 2025).with_columns(
        pl.col("added_mw").cum_sum().alias("cumulative_mw")
    )
    _bcum_years = _bcum["year"].to_list()
    _bcum_mw = _bcum["cumulative_mw"].to_list()

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_badd_years, _badd_mw, color="#5ba4cf")
    _add_max = float(_badd["added_mw"].max() or 0)
    _pfx1, _div1 = metric_formatter(_add_max, base_unit="M")
    _ax1.set_title(f"{state_name} Utility-Scale Battery Storage — Annual Capacity Additions (EIA-860)")
    _ax1.set_ylabel(f"Capacity Added ({prefixed_unit(_pfx1, 'W')})")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(metric_tick_formatter(_pfx1, _div1))
    style_ax(_ax1)

    _ax2.plot(
        _bcum_years, _bcum_mw, color="#5ba4cf", linewidth=5, marker="o", markersize=10
    )
    _ax2.fill_between(_bcum_years, _bcum_mw, alpha=0.25, color="#5ba4cf")
    _ax2.set_title(
        f"{state_name} Utility-Scale Battery Storage — Cumulative Installed Capacity (EIA-860)"
    )
    _ax2.set_xlabel("Year")
    _cum_max = float(_bcum["cumulative_mw"].max() or 0)
    _pfx2, _div2 = metric_formatter(_cum_max, base_unit="M")
    _ax2.set_ylabel(f"Installed Capacity ({prefixed_unit(_pfx2, 'W')})")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_batt_capacity = _fig
    chart_eia860_batt_capacity
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Utility scale battery storage is almost non-existent
    * As with net-metering customers, there's virtually no utility scale battery storage in PA.
    * Half a dozen systems, with most capacity (oddly) installed in 2015-2016, adding up to just over 35MW.
    """)
    return


@app.cell(hide_code=True)
def chart_ee_by_class(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    energy_efficiency_eia861,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _classes = ["commercial", "residential", "industrial"]

    _ee_pivot = (
        energy_efficiency_eia861.filter(pl.col("customer_class").is_in(_classes))
        .group_by([pl.col("report_date").dt.year().alias("year"), "customer_class"])
        .agg(pl.col("incremental_peak_reduction_mw").sum())
        .sort("year")
        .pivot(
            on="customer_class",
            index="year",
            values="incremental_peak_reduction_mw",
            aggregate_function="sum",
        )
        .fill_null(0)
        .sort("year")
    )
    _years = _ee_pivot["year"].to_list()

    _fig, _ax = plt.subplots(figsize=FIGSIZE)
    _bottom = [0.0] * len(_years)
    for _cls in _classes:
        _vals = _ee_pivot[_cls].to_list()
        _ax.bar(
            _years, _vals, bottom=_bottom, label=_cls.capitalize(), color=CLASS_COLORS[_cls]
        )
        _bottom = [b + v for b, v in zip(_bottom, _vals)]

    _ax.set_title(
        f"{state_name} Energy Efficiency Programs — Incremental Peak Reduction by Customer Class (EIA-861)"
    )
    _ax.set_xlabel("Year")

    _stack_max = max(_bottom) if _bottom else 0
    _pfx, _div = metric_formatter(_stack_max, base_unit="M")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    _ax.legend(loc="upper left")
    _ax.yaxis.set_major_formatter(metric_tick_formatter(_pfx, _div))
    _ax.set_ylabel(f"Incremental Peak Reduction ({prefixed_unit(_pfx, 'W')})")
    style_ax(_ax)
    plt.tight_layout()
    chart_ee_by_class = _fig
    chart_ee_by_class
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Energy efficiency peak reductions are modest, spread across customer classes
    * Incremental peak reduction has varied substantially over the years, with more than a 2x swing between high and low years.
    * Residential and Commercial customes have historically contributed comparable amounts.
    * Industrial energy efficiency programs have typically been much less impactful.
    * However, in the most recent (2024) data, industrial efficiency contributions roughly doubled, after being constant for many years.
    * Total peak reduction across all customer classes reported to EIA-861 is only 100-250 MW.
    """)
    return


@app.cell(hide_code=True)
def chart_dr_by_class(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    demand_response_eia861,
    metric_formatter,
    metric_tick_formatter,
    pl,
    plt,
    prefixed_unit,
    state_name,
    style_ax,
):
    _dr = (
        demand_response_eia861.filter(
            ~pl.col("customer_class").is_in(["total", "transportation"])
        )
        .group_by([pl.col("report_date").dt.year().alias("year"), "customer_class"])
        .agg(
            pl.col("potential_peak_demand_savings_mw").sum().alias("potential_mw"),
            pl.col("actual_peak_demand_savings_mw").sum().alias("actual_mw"),
            pl.col("customers").sum(),
        )
        .sort("year")
    )


    def _pivot_dr(df, value_col):
        return (
            df.pivot(
                on="customer_class",
                index="year",
                values=value_col,
                aggregate_function="sum",
            )
            .fill_null(0)
            .sort("year")
        )


    _pot = _pivot_dr(_dr, "potential_mw")
    _act = _pivot_dr(_dr, "actual_mw")
    _years = _pot["year"].to_list()

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _res_max = max(
        float(_pot["residential"].max() or 0),
        float(_act["residential"].max() or 0),
    )
    _pfx1, _div1 = metric_formatter(_res_max, base_unit="M")

    _ax1.bar(
        _years,
        _pot["residential"].to_list(),
        color=CLASS_COLORS["residential"],
        label="Potential",
    )
    _ax1.plot(
        _years,
        _act["residential"].to_list(),
        color="white",
        linewidth=5,
        linestyle="--",
        marker="o",
        markersize=10,
        label="Actual",
        zorder=5,
    )
    _ax1.set_title(
        f"{state_name} Demand Response — Residential: Potential vs. Actual Peak Savings (EIA-861)"
    )
    _ax1.set_ylabel(f"Peak Demand Savings ({prefixed_unit(_pfx1, 'W')})")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.set_ylim(0)
    _ax1.legend(loc="upper right")
    _ax1.yaxis.set_major_formatter(metric_tick_formatter(_pfx1, _div1))
    style_ax(_ax1)

    _ci_classes = ["commercial", "industrial"]
    _bottom = [0.0] * len(_years)
    for _cls in _ci_classes:
        if _cls in _pot.columns:
            _vals = _pot[_cls].to_list()
            _ax2.bar(
                _years,
                _vals,
                bottom=_bottom,
                label=f"{_cls.capitalize()} (potential)",
                color=CLASS_COLORS[_cls],
            )
            _bottom = [b + v for b, v in zip(_bottom, _vals)]
    _ci_actual = [
        sum(_act[c][i] for c in _ci_classes if c in _act.columns)
        for i in range(len(_years))
    ]
    _ci_max = max(max(_bottom) if _bottom else 0, max(_ci_actual) if _ci_actual else 0)
    _pfx2, _div2 = metric_formatter(_ci_max, base_unit="M")
    _ax2.plot(
        _years,
        _ci_actual,
        color="white",
        linewidth=5,
        linestyle="--",
        marker="o",
        markersize=10,
        label="Actual (all C&I)",
        zorder=5,
    )
    _ax2.set_title(
        f"{state_name} Demand Response — Commercial & Industrial: Potential vs. Actual Peak Savings (EIA-861)"
    )
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel(f"Peak Demand Savings ({prefixed_unit(_pfx2, 'W')})")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.legend(loc="upper right")
    _ax2.yaxis.set_major_formatter(metric_tick_formatter(_pfx2, _div2))
    style_ax(_ax2)

    plt.tight_layout()
    chart_dr_by_class = _fig
    chart_dr_by_class
    return


@app.cell(hide_code=True)
def _dr_notes(mo):
    mo.md("""
    ## Notes on PA Demand Response Data (EIA-861, 2013–2024)

    ### Why commercial & industrial DR shows a large spike in 2017–2020

    The EIA-861 demand response schedule was introduced in 2013 after EIA split its legacy
    Demand-Side Management form into separate energy efficiency and demand response tables.
    Through 2016, only a small number of PA utilities — primarily PECO — actively reported DR
    programs. Starting in 2017, four additional major PA distribution utilities (PPL Electric,
    West Penn Power, Metropolitan Edison, and Pennsylvania Power) began reporting, bringing
    with them large blocks of commercial and industrial load enrolled in
    **PJM's wholesale capacity-market demand response programs**.

    These wholesale DR commitments — curtailable industrial and large commercial loads
    contracted directly in PJM's capacity market — were reported here because the distribution
    utilities aggregated and submitted them. The numbers are plausible: ~630 MW potential in
    2017–2020 with only a few hundred industrial customers implies a few MW per
    customer on average, consistent with small to medium-sized industrial facilities.

    ### Why C&I demand response collapsed after 2020

    PJM operated several legacy DR products — the **Limited**, **Extended Summer**, and
    **Annual** programs — that were popular with large industrial customers because they
    required availability only during summer peak hours. These programs were
    **discontinued effective June 1, 2018**, replaced by **Base Capacity** (available only
    for the 2018/19 and 2019/20 delivery years) and **Capacity Performance (CP)**.

    CP requires year-round, around-the-clock availability and imposes severe financial
    penalties for non-performance. Many large industrial customers found the CP requirements
    incompatible with their operations and exited. **Base Capacity was retired after the
    2019/20 season**, making CP the only available PJM emergency capacity DR product from
    summer 2020 onward. The utilities subsequently stopped reporting wholesale industrial DR
    through the EIA-861 form.

    * **[CPower: PJM Capacity Demand Response Changes](https://cpowerenergy.com/help/pjm-dr-changes/)**
    * **[PJM Demand Response](https://www.pjm.com/markets-and-operations/demand-response.aspx)**

    ### What the residential panel shows

    The residential panel has two distinct phases. From 2013–2020, PECO reported roughly
    50,000–72,000 residential customers enrolled in AC cycling and water heater direct load
    control programs, contributing 35–72 MW of potential peak savings. Allegheny Electric
    Cooperative began reporting a separate ~45,000-customer (~45 MW) residential program in
    2017, growing total residential DR to ~93 MW by 2020.

    PECO discontinued its **Smart A/C Saver** program in May 2021. This accounts for
    the abrupt ~50 MW drop visible in the residential chart in 2021. PECO filed no
    demand response data for any customer class in 2021 or any subsequent year.

    Currently only Allegheny Electric Cooperative continues to report residential DR
    — for 42,000–45,000 customers and ~42 MW of potential peak savings.

    * **[Philadelphia Inquirer: Peco pulls the plug on Smart A/C program (Nov 2020)](https://www.inquirer.com/business/peco-discontinues-smart-a-c-saver-energy-conservation-program-20201118.html)**
    """)
    return


@app.cell(hide_code=True)
def chart_ami_penetration(
    CLASS_COLORS,
    YEAR_XLIM,
    ami_eia861,
    count_formatter,
    count_tick_formatter,
    mticker,
    pl,
    plt,
    state_name,
    style_ax,
):
    _classes = ["commercial", "residential", "industrial"]

    _totals_2013 = (
        ami_eia861.filter(
            pl.col("customer_class").is_in(_classes),
            pl.col("report_date").dt.year() == 2013,
        )
        .group_by("customer_class")
        .agg(
            pl.col("advanced_metering_infrastructure").sum().alias("ami"),
            pl.col("automated_meter_reading").sum().alias("amr"),
            pl.col("non_amr_ami").sum().alias("standard"),
        )
        .with_columns(
            (pl.col("ami") + pl.col("amr") + pl.col("standard")).alias("total_2013")
        )
        .select(["customer_class", "total_2013"])
        .with_columns(pl.col("customer_class").cast(pl.String))
    )
    _total_2013_map = {
        r["customer_class"]: r["total_2013"] for r in _totals_2013.iter_rows(named=True)
    }

    _ami_pct = (
        ami_eia861.filter(pl.col("customer_class").is_in(_classes))
        .group_by([pl.col("report_date").dt.year().alias("year"), "customer_class"])
        .agg(
            pl.col("advanced_metering_infrastructure").sum().alias("ami"),
            pl.col("automated_meter_reading").sum().alias("amr"),
            pl.col("non_amr_ami").sum().alias("standard"),
        )
        .with_columns(pl.col("customer_class").cast(pl.String))
        .join(_totals_2013, on="customer_class")
        .with_columns(
            _total=pl.when(pl.col("year") < 2013)
            .then(pl.col("total_2013"))
            .otherwise(pl.col("ami") + pl.col("amr") + pl.col("standard"))
        )
        .with_columns((pl.col("ami") / pl.col("_total") * 100).alias("ami_pct"))
        .sort("year", "customer_class")
    )


    def _meter_inventory(customer_class: str) -> pl.DataFrame:
        fixed_total = _total_2013_map[customer_class]
        return (
            ami_eia861.filter(pl.col("customer_class") == customer_class)
            .group_by(pl.col("report_date").dt.year().alias("year"))
            .agg(
                pl.col("advanced_metering_infrastructure").sum().alias("ami"),
                pl.col("automated_meter_reading").sum().alias("amr"),
                pl.col("non_amr_ami").sum().alias("standard"),
            )
            .with_columns(
                pl.when(pl.col("year") < 2013)
                .then(fixed_total - pl.col("ami") - pl.col("amr"))
                .otherwise(0)
                .alias("standard_est")
            )
            .sort("year")
        )


    def _stacked_bar(ax, df: pl.DataFrame) -> None:
        _yrs = df["year"].to_list()
        _ami = df["ami"].to_list()
        _amr = df["amr"].to_list()
        _std = df["standard"].to_list()
        _est = df["standard_est"].to_list()
        _b1 = _ami
        _b2 = [a + b for a, b in zip(_ami, _amr)]
        _b3 = _b2
        ax.bar(
            _yrs, _ami, label="AMI (two-way)", color=CLASS_COLORS["residential"], width=0.8
        )
        ax.bar(_yrs, _amr, bottom=_b1, label="AMR (one-way)", color="#d19a66", width=0.8)
        ax.bar(_yrs, _std, bottom=_b2, label="Standard", color="#5c6370", width=0.8)
        ax.bar(
            _yrs,
            _est,
            bottom=_b3,
            label="Standard (est.)",
            color="#aeb3ba",
            hatch="//",
            width=0.8,
        )
        _stack_max = max(_b2[i] + _std[i] + _est[i] for i in range(len(_yrs))) if _yrs else 0
        _csfx, _cdiv, _cword = count_formatter(_stack_max)
        ax.set_xlim(*YEAR_XLIM)
        ax.set_ylim(0)
        ax.yaxis.set_major_formatter(count_tick_formatter(_csfx, _cdiv))
        _label_suffix = f" ({_cword})" if _cword else ""
        ax.set_ylabel(f"Meters{_label_suffix}")
        style_ax(ax)


    _fig, (_ax1, _ax2, _ax3, _ax4) = plt.subplots(4, 1, figsize=(16, 22))

    for _cls in _classes:
        _sub = _ami_pct.filter(pl.col("customer_class") == _cls).sort("year")
        _ax1.plot(
            _sub["year"].to_list(),
            _sub["ami_pct"].to_list(),
            label=_cls.capitalize(),
            color=CLASS_COLORS[_cls],
            linewidth=5,
            marker="o",
            markersize=10,
        )
    _ax1.set_title(
        f"{state_name} Advanced Metering Infrastructure — AMI Penetration by Customer Class (EIA-861)"
    )
    _ax1.set_ylabel("Meters with AMI (%)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.set_ylim(0, 105)
    _ax1.legend(loc="upper left")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    _ax1.text(
        0.98,
        0.04,
        "Note: Includes AMI (bi-directional) only.",
        transform=_ax1.transAxes,
        ha="right",
        va="bottom",
        fontsize=11,
        color="#9aa0a6",
        style="italic",
    )
    style_ax(_ax1)

    _stacked_bar(_ax2, _meter_inventory("residential"))
    _ax2.set_title(f"{state_name} Residential Meter Inventory by Type (EIA-861)")
    _ax2.legend(loc="upper left")

    _stacked_bar(_ax3, _meter_inventory("commercial"))
    _ax3.set_title(f"{state_name} Commercial Meter Inventory by Type (EIA-861)")
    _ax3.legend(loc="upper left")

    _stacked_bar(_ax4, _meter_inventory("industrial"))
    _ax4.set_title(f"{state_name} Industrial Meter Inventory by Type (EIA-861)")
    _ax4.set_xlabel("Year")
    _ax4.legend(loc="upper left")

    plt.tight_layout()
    chart_ami_penetration = _fig
    chart_ami_penetration
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## AMI is essentially universal in PA
    * Starting in 2016 the rollout of bidirectional AMI accelerated rapidly.
    * Since 2022 all customer classes have had close to 100% penetration for bidirectional AMI.
    * Reporting changes or data issues make the most recent Industrial AMI data a little fishy. Would need to dig in more to understand what's going on there.
    """)
    return


@app.cell
def _ann_industrial_standard_2017(mo):
    mo.md("""
    ### 2017: standard meter count drops sharply

    In 2016, four FirstEnergy Pennsylvania distribution utilities collectively reported
    approximately 13,400 non-AMR/non-AMI ("standard") industrial meters. In 2017 that
    count fell to near zero. West Penn Power alone accounted for a drop of 11,629
    meters (11,629 to 0); Pennsylvania Electric Co dropped from 891 to 1; Metropolitan
    Edison Co from 770 to 7; and Pennsylvania Power Co from 94 to 0. No commensurate
    increase in AMI or AMR was reported by these utilities, so the total PA industrial
    meter inventory fell from approximately 25,800 in 2016 to approximately 12,700 in
    2017.
    """)
    return


@app.cell
def _tbl_industrial_standard_2017(ami_eia861, pl):
    (
        ami_eia861.filter(
            pl.col("customer_class") == "industrial",
            pl.col("report_date").dt.year().is_in([2015, 2016, 2017, 2018]),
        )
        .with_columns(pl.col("report_date").dt.year().cast(pl.String).alias("year"))
        .group_by("utility_name_eia", "year")
        .agg(pl.col("non_amr_ami").sum())
        .pivot(
            on="year",
            index="utility_name_eia",
            values="non_amr_ami",
            aggregate_function="sum",
        )
        .filter(pl.col("2016").fill_null(0) > 0)
        .sort("2016", descending=True)
        .select(["utility_name_eia", "2015", "2016", "2017", "2018"])
        .head(10)
    )
    return


@app.cell
def _ann_peco_2024(mo):
    mo.md("""
    ### 2024: PECO industrial AMI count increases sharply

    PECO Energy Co reported 162,913 industrial AMI meters in 2024, up from 3,730 in
    2023, an increase of 159,183. In the same filing year PECO's commercial AMI count
    fell from 146,555 to 97,714, a decrease of 48,841. The two changes do not offset
    one another.
    """)
    return


@app.cell
def _tbl_peco_2024(ami_eia861, pl):
    (
        ami_eia861.filter(
            pl.col("utility_name_eia") == "PECO Energy Co",
            pl.col("customer_class").is_in(["commercial", "industrial"]),
            pl.col("report_date").dt.year().is_in([2022, 2023, 2024]),
        )
        .select(
            [
                pl.col("report_date").dt.year().alias("year"),
                "customer_class",
                pl.col("advanced_metering_infrastructure").alias("ami"),
                pl.col("automated_meter_reading").alias("amr"),
                pl.col("non_amr_ami").alias("standard"),
            ]
        )
        .sort("customer_class", "year")
    )
    return


@app.cell
def _(mo):
    # State selector — appears in a persistent left sidebar.
    # Add more entries here as pjm_gats.csv grows to cover more PJM states.
    _state_options = {
        "Pennsylvania": "PA",
        "New Jersey": "NJ",
    }

    state_selector = mo.ui.dropdown(
        options=_state_options,
        value="Pennsylvania",
        label="## Select a state",
        searchable=True,
    )

    mo.sidebar(state_selector)
    return (state_selector,)


@app.cell
def _(state_selector):
    selected_state = state_selector.value

    STATE_NAMES = {
        "PA": "Pennsylvania",
        "NJ": "New Jersey",
    }
    state_name = STATE_NAMES.get(selected_state, selected_state)
    return selected_state, state_name


if __name__ == "__main__":
    app.run()
