import marimo

__generated_with = "0.23.9"
app = marimo.App(width="full")


@app.cell
def _():
    return


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

    matplotlib.style.use(matplotx.styles.onedark)
    plt.rcParams.update({"font.size": 16, "axes.titlesize": 22, "axes.labelsize": 18})
    return Path, mo, mticker, np, os, pl, plt


@app.cell
def parquet_dir(Path, os):
    parquet_dir = Path(os.environ["PUDL_OUTPUT"]) / "parquet"
    return (parquet_dir,)


@app.cell
def net_metering_customer_fuel_class(parquet_dir, pl):
    net_metering_customer_fuel_class = (
        pl.scan_parquet(
            parquet_dir / "core_eia861__yearly_net_metering_customer_fuel_class.parquet"
        )
        .filter(pl.col("state") == "PA")
        .collect()
    )
    return (net_metering_customer_fuel_class,)


@app.cell
def net_metering_misc(parquet_dir, pl):
    net_metering_misc = (
        pl.scan_parquet(parquet_dir / "core_eia861__yearly_net_metering_misc.parquet")
        .filter(pl.col("state") == "PA")
        .collect()
    )
    return


@app.cell
def non_net_metering_customer_fuel_class(parquet_dir, pl):
    non_net_metering_customer_fuel_class = (
        pl.scan_parquet(
            parquet_dir / "core_eia861__yearly_non_net_metering_customer_fuel_class.parquet"
        )
        .filter(pl.col("state") == "PA")
        .collect()
    )
    return (non_net_metering_customer_fuel_class,)


@app.cell
def non_net_metering_misc(parquet_dir, pl):
    non_net_metering_misc = (
        pl.scan_parquet(parquet_dir / "core_eia861__yearly_non_net_metering_misc.parquet")
        .filter(pl.col("state") == "PA")
        .collect()
    )
    return


@app.cell
def gats_registrations(Path, pl):
    _gats_path = Path(__file__).parent / "pjm_gats.csv"
    gats_registrations = pl.read_csv(
        _gats_path,
        encoding="latin1",
        infer_schema_length=5000,
    ).with_columns(
        pl.col("Date Online").str.to_date("%m/%d/%Y").alias("date_online"),
    )
    return (gats_registrations,)


@app.cell(hide_code=True)
def pa_generators_eia860(parquet_dir, pl):
    _gen = pl.scan_parquet(parquet_dir / "out_eia__yearly_generators.parquet").filter(
        pl.col("state") == "PA"
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
def pa_dsm_eia861(parquet_dir, pl):
    demand_response_eia861 = (
        pl.scan_parquet(parquet_dir / "core_eia861__yearly_demand_response.parquet")
        .filter(pl.col("state") == "PA")
        .collect()
    )

    energy_efficiency_eia861 = (
        pl.scan_parquet(parquet_dir / "core_eia861__yearly_energy_efficiency.parquet")
        .filter(pl.col("state") == "PA")
        .collect()
    )

    ami_eia861 = (
        pl.scan_parquet(
            parquet_dir / "core_eia861__yearly_advanced_metering_infrastructure.parquet"
        )
        .filter(pl.col("state") == "PA")
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
    bin_edges = [
        (0, 0.5, "1–3 kW"),
        (0.5, 1.0, "3–10 kW"),
        (1.0, 1.5, "10–32 kW"),
        (1.5, 2.0, "32–100 kW"),
        (2.0, 2.5, "100–316 kW"),
        (2.5, 3.0, "316 kW–1 MW"),
        (3.0, 3.5, "1–3 MW"),
        (3.5, 4.0, "3–10 MW"),
        (4.0, 5.0, ">10 MW"),
    ]
    bin_order = [e[2] for e in bin_edges]


    def size_hist(df: pl.DataFrame) -> pl.DataFrame:
        """Pre-aggregate system sizes into log-spaced labeled bins (count and MW)."""
        _d = df.filter(pl.col("Nameplate") > 0).with_columns(
            (pl.col("Nameplate") * 1000).log(base=10).alias("log10_kw")
        )
        rows = []
        for lo, hi, label in bin_edges:
            _bin = _d.filter((pl.col("log10_kw") >= lo) & (pl.col("log10_kw") < hi))
            rows.append(
                {
                    "bin": label,
                    "count": _bin.height,
                    "capacity_mw": float(_bin["Nameplate"].sum()),
                }
            )
        return pl.DataFrame(rows)


    # MW-scale bins for EIA-860 utility/commercial generators (capacity_mw column)
    mw_bin_edges = [
        (-0.5, 0.0, "<1 MW"),
        (0.0, 0.7, "1–5 MW"),
        (0.7, 1.3, "5–20 MW"),
        (1.3, 1.7, "20–50 MW"),
        (1.7, 2.5, "50–300 MW"),
    ]
    mw_bin_order = [e[2] for e in mw_bin_edges]


    def mw_size_hist(df: pl.DataFrame) -> pl.DataFrame:
        """Pre-aggregate generator capacities into log-spaced MW bins (count and MW)."""
        _d = df.filter(pl.col("capacity_mw") > 0).with_columns(
            pl.col("capacity_mw").log(base=10).alias("log10_mw")
        )
        rows = []
        for lo, hi, label in mw_bin_edges:
            _bin = _d.filter((pl.col("log10_mw") >= lo) & (pl.col("log10_mw") < hi))
            rows.append(
                {
                    "bin": label,
                    "count": _bin.height,
                    "capacity_mw": float(_bin["capacity_mw"].sum()),
                }
            )
        return pl.DataFrame(rows)

    return bin_order, mw_bin_order, mw_size_hist, size_hist


@app.cell
def plot_helpers():
    FIGSIZE = (16, 10)
    YEAR_XLIM = (2001.5, 2026.5)

    CLASS_COLORS = {
        "commercial": "#61afef",
        "residential": "#e06c75",
        "industrial": "#98c379",
        "transportation": "#c678dd",
    }


    def style_ax(ax):
        ax.set_axisbelow(True)
        ax.grid(axis="y", linewidth=0.5)

    return CLASS_COLORS, FIGSIZE, YEAR_XLIM, style_ax


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
            .agg(pl.col("Nameplate").sum().alias("added_mw"), pl.len().alias("n_systems"))
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


@app.cell(hide_code=True)
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


@app.cell
def chart_capacity(
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    net_metering_data,
    pl,
    plt,
    style_ax,
    tech_colors,
    tech_order,
):
    _techs = [
        t
        for t in tech_order
        if t not in ("Virtual / Community PV", "CHP / Cogen", "Non-PV Storage")
    ]
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

    _ax.set_title("Pennsylvania Net-Metered DER Capacity by Technology (EIA-861)")
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Installed Capacity (MW)")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    _ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax)
    plt.tight_layout()
    chart_capacity = _fig
    chart_capacity
    return


@app.cell
def chart_non_net_metered_capacity(
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    non_net_metering_by_tech,
    pl,
    plt,
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

    _ax.set_title("Pennsylvania Non-Net-Metered BTM DER Capacity by Technology (EIA-861)")
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Installed Capacity (MW)")
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    _ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.2f}"))
    style_ax(_ax)
    plt.tight_layout()
    chart_non_net_metered_capacity = _fig
    chart_non_net_metered_capacity
    return


@app.cell
def chart_customers(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    net_metering_customer_fuel_class,
    pl,
    plt,
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

    _ax.set_title("Pennsylvania Net Metering Customer Count by Customer Class (EIA-861)")
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Net Metering Customers")
    _ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    style_ax(_ax)
    plt.tight_layout()
    chart_customers = _fig
    chart_customers
    return


@app.cell
def chart_storage(
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    net_metering_data,
    pl,
    plt,
    style_ax,
):
    _pa_s = net_metering_data.filter(pl.col("technology") == "PV + Battery Storage").sort(
        "year"
    )
    _years = _pa_s["year"].to_list()

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

    _ax1.set_ylim(0, 14)
    _ax2.set_ylim(0, 1400)
    _ax1.yaxis.set_major_locator(mticker.MultipleLocator(2))
    _ax2.yaxis.set_major_locator(mticker.MultipleLocator(200))
    _ax1.set_xlabel("Year")
    _ax1.set_ylabel("Inverter Capacity (MW)", color="#2166ac")
    _ax2.set_ylabel("Customers", color="#74add1")
    _ax1.tick_params(axis="y", labelcolor="#2166ac")
    _ax2.tick_params(axis="y", labelcolor="#74add1")
    _ax1.set_title("Pennsylvania Net-Metered PV+Battery Storage, Co-Located (EIA-861)")
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


@app.cell
def chart_gats_solar(
    FIGSIZE,
    YEAR_XLIM,
    gats_solar_annual,
    mticker,
    net_metering_by_tech,
    non_net_metering_by_tech,
    pl,
    plt,
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
        "Pennsylvania Solar PV — GATS All Registered vs. EIA-861 Net-Metered vs. Non-Net-Metered"
    )
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Cumulative Capacity (MW)")
    _ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    _ax.legend(loc="upper left")
    _ax.set_xlim(*YEAR_XLIM)
    style_ax(_ax)
    plt.tight_layout()
    chart_gats_solar = _fig
    chart_gats_solar
    return


@app.cell
def chart_gats_additions(
    FIGSIZE,
    YEAR_XLIM,
    gats_solar_annual,
    mticker,
    pl,
    plt,
    style_ax,
):
    _gadd = gats_solar_annual.filter(pl.col("added_mw").is_not_null()).sort("year")
    _gcum = gats_solar_annual.sort("year")

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_gadd["year"], _gadd["added_mw"], color="#f4a020", width=0.8)
    _ax1.set_title("Pennsylvania Solar PV — Annual Capacity Additions (GATS)")
    _ax1.set_ylabel("Capacity Added (MW)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
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
    _ax2.set_title("Pennsylvania Solar PV — Cumulative GATS-Registered Capacity")
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Cumulative Capacity (MW)")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_gats_additions = _fig
    chart_gats_additions
    return


@app.cell
def chart_gats_size_hist(
    FIGSIZE,
    bin_order,
    gats_registrations,
    mticker,
    np,
    pl,
    plt,
    size_hist,
    style_ax,
):
    _sun_all = gats_registrations.filter(pl.col("Primary Fuel Type") == "SUN")
    _hist_sun = size_hist(_sun_all)
    _n_sun = _sun_all.height

    _sun_x = np.arange(len(bin_order))
    _sun_counts = [_hist_sun.filter(pl.col("bin") == b)["count"].sum() for b in bin_order]
    _sun_mw = [_hist_sun.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in bin_order]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_sun_x, _sun_counts, color="#f4a020")
    _ax1.set_title(
        f"PA Solar PV — System Count by Size, All Online Systems (GATS, n={_n_sun:,})"
    )
    _ax1.set_ylabel("Number of Systems")
    _ax1.set_xticks(_sun_x)
    _ax1.set_xticklabels(bin_order, rotation=30, ha="right")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.bar(_sun_x, _sun_mw, color="#f4a020")
    _ax2.set_title(
        "PA Solar PV — Installed Capacity (MW) by Size, All Online Systems (GATS)"
    )
    _ax2.set_ylabel("Installed Capacity (MW)")
    _ax2.set_xlabel("System Size")
    _ax2.set_xticks(_sun_x)
    _ax2.set_xticklabels(bin_order, rotation=30, ha="right")
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_gats_size_hist = _fig
    chart_gats_size_hist
    return


@app.cell
def chart_ee_size_hist(
    FIGSIZE,
    bin_order,
    gats_registrations,
    mticker,
    np,
    pl,
    plt,
    size_hist,
    style_ax,
):
    _ee_all = gats_registrations.filter(
        pl.col("Primary Fuel Type").str.strip_chars() == "EE"
    )
    _hist_ee = size_hist(_ee_all)
    _n_ee = _ee_all.height

    _ee_x = np.arange(len(bin_order))
    _ee_counts = [_hist_ee.filter(pl.col("bin") == b)["count"].sum() for b in bin_order]
    _ee_mw = [_hist_ee.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in bin_order]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_ee_x, _ee_counts, color="#2ca02c")
    _ax1.set_title(
        f"PA Energy Efficiency — Registration Count by Size, All Systems (GATS, n={_n_ee:,})"
    )
    _ax1.set_ylabel("Number of EE Registrations")
    _ax1.set_xticks(_ee_x)
    _ax1.set_xticklabels(bin_order, rotation=30, ha="right")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.bar(_ee_x, _ee_mw, color="#2ca02c")
    _ax2.set_title(
        "PA Energy Efficiency — Registered Capacity (MW) by Size, All Systems (GATS)"
    )
    _ax2.set_ylabel("Registered Capacity (MW)")
    _ax2.set_xlabel("System Size")
    _ax2.set_xticks(_ee_x)
    _ax2.set_xticklabels(bin_order, rotation=30, ha="right")
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.2f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_ee_size_hist = _fig
    chart_ee_size_hist
    return


@app.cell
def chart_ee_cumulative(
    FIGSIZE,
    YEAR_XLIM,
    gats_ee_annual,
    mticker,
    pl,
    plt,
    style_ax,
):
    _ee_add = gats_ee_annual.filter(pl.col("added_mw").is_not_null()).sort("year")
    _ee_cum = gats_ee_annual.sort("year")

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_ee_add["year"], _ee_add["added_mw"], color="#2ca02c", width=0.8)
    _ax1.set_title("Pennsylvania Energy Efficiency Resources — Annual Additions (GATS)")
    _ax1.set_ylabel("Capacity Added (MW)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}"))
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
    _ax2.set_title(
        "Pennsylvania Energy Efficiency Resources — Cumulative GATS-Registered Capacity"
    )
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Cumulative Capacity (MW)")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_ee_cumulative = _fig
    chart_ee_cumulative
    return


@app.cell
def chart_eia860_solar_hist(
    FIGSIZE,
    mticker,
    mw_bin_order,
    mw_size_hist,
    np,
    pl,
    plt,
    solar_generators_eia860,
    style_ax,
):
    _solar_snap = solar_generators_eia860.sort("report_date", descending=True).unique(
        subset=["plant_id_eia", "generator_id"], keep="first"
    )
    _hist_solar = mw_size_hist(_solar_snap)
    _n_solar = _solar_snap.height
    _sx = np.arange(len(mw_bin_order))
    _solar_counts = [
        _hist_solar.filter(pl.col("bin") == b)["count"].sum() for b in mw_bin_order
    ]
    _solar_mw = [
        _hist_solar.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in mw_bin_order
    ]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_sx, _solar_counts, color="#f4a020")
    _ax1.set_title(
        f"PA Utility-Scale Solar PV — Generator Count by Size (EIA-860, n={_n_solar})"
    )
    _ax1.set_ylabel("Number of Generators")
    _ax1.set_xticks(_sx)
    _ax1.set_xticklabels(mw_bin_order, rotation=30, ha="right")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.bar(_sx, _solar_mw, color="#f4a020")
    _ax2.set_title("PA Utility-Scale Solar PV — Installed Capacity (MW) by Size (EIA-860)")
    _ax2.set_ylabel("Installed Capacity (MW)")
    _ax2.set_xlabel("Generator Size")
    _ax2.set_xticks(_sx)
    _ax2.set_xticklabels(mw_bin_order, rotation=30, ha="right")
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_solar_hist = _fig
    chart_eia860_solar_hist
    return


@app.cell
def chart_eia860_solar_capacity(
    FIGSIZE,
    YEAR_XLIM,
    mticker,
    pl,
    plt,
    solar_additions_annual,
    solar_capacity_annual,
    style_ax,
):
    _sadd = solar_additions_annual.filter(pl.col("year") <= 2025)
    _sadd_years = _sadd["year"].to_list()
    _sadd_mw = _sadd["added_mw"].to_list()

    _scum = solar_capacity_annual.filter(pl.col("year") <= 2025)
    _scum_years = _scum["year"].to_list()
    _scum_mw = [v / 1000 for v in _scum["total_mw"].to_list()]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)

    _ax1.bar(_sadd_years, _sadd_mw, color="#f4a020")
    _ax1.set_title("PA Utility-Scale Solar PV — Annual Capacity Additions (EIA-860)")
    _ax1.set_ylabel("Capacity Added (MW)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.plot(
        _scum_years, _scum_mw, color="#f4a020", linewidth=5, marker="o", markersize=10
    )
    _ax2.fill_between(_scum_years, _scum_mw, alpha=0.25, color="#f4a020")
    _ax2.set_title("PA Utility-Scale Solar PV — Cumulative Installed Capacity (EIA-860)")
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Installed Capacity (GW)")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.2f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_solar_capacity = _fig
    chart_eia860_solar_capacity
    return


@app.cell
def chart_eia860_batt_hist(
    FIGSIZE,
    battery_generators_eia860,
    mticker,
    mw_bin_order,
    mw_size_hist,
    np,
    pl,
    plt,
    style_ax,
):
    _batt_snap = battery_generators_eia860.sort("report_date", descending=True).unique(
        subset=["plant_id_eia", "generator_id"], keep="first"
    )
    _hist_batt = mw_size_hist(_batt_snap)
    _n_batt = _batt_snap.height
    _bx = np.arange(len(mw_bin_order))
    _batt_counts = [
        _hist_batt.filter(pl.col("bin") == b)["count"].sum() for b in mw_bin_order
    ]
    _batt_mw = [
        _hist_batt.filter(pl.col("bin") == b)["capacity_mw"].sum() for b in mw_bin_order
    ]

    _fig, (_ax1, _ax2) = plt.subplots(2, 1, figsize=FIGSIZE)
    _ax1.bar(_bx, _batt_counts, color="#5ba4cf")
    _ax1.set_title(
        f"PA Utility-Scale Battery Storage — Generator Count by Size (EIA-860, n={_n_batt})"
    )
    _ax1.set_ylabel("Number of Generators")
    _ax1.set_xticks(_bx)
    _ax1.set_xticklabels(mw_bin_order, rotation=30, ha="right")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.bar(_bx, _batt_mw, color="#5ba4cf")
    _ax2.set_title(
        "PA Utility-Scale Battery Storage — Installed Capacity (MW) by Size (EIA-860)"
    )
    _ax2.set_ylabel("Installed Capacity (MW)")
    _ax2.set_xlabel("Generator Size")
    _ax2.set_xticks(_bx)
    _ax2.set_xticklabels(mw_bin_order, rotation=30, ha="right")
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_batt_hist = _fig
    chart_eia860_batt_hist
    return


@app.cell
def chart_eia860_batt_capacity(
    FIGSIZE,
    YEAR_XLIM,
    battery_additions_annual,
    mticker,
    pl,
    plt,
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
    _ax1.set_title("PA Utility-Scale Battery Storage — Annual Capacity Additions (EIA-860)")
    _ax1.set_ylabel("Capacity Added (MW)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax1)

    _ax2.plot(
        _bcum_years, _bcum_mw, color="#5ba4cf", linewidth=5, marker="o", markersize=10
    )
    _ax2.fill_between(_bcum_years, _bcum_mw, alpha=0.25, color="#5ba4cf")
    _ax2.set_title(
        "PA Utility-Scale Battery Storage — Cumulative Installed Capacity (EIA-860)"
    )
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Installed Capacity (MW)")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax2)

    plt.tight_layout()
    chart_eia860_batt_capacity = _fig
    chart_eia860_batt_capacity
    return


@app.cell
def chart_ee_by_class(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    energy_efficiency_eia861,
    mticker,
    pl,
    plt,
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
        "PA Energy Efficiency Programs — Incremental Peak Reduction by Customer Class (EIA-861)"
    )
    _ax.set_xlabel("Year")
    _ax.set_ylabel("Incremental Peak Reduction (MW)")
    _ax.set_xlim(*YEAR_XLIM)
    _ax.set_ylim(0)
    _ax.legend(loc="upper left")
    _ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    style_ax(_ax)
    plt.tight_layout()
    chart_ee_by_class = _fig
    chart_ee_by_class
    return


@app.cell
def chart_dr_by_class(
    CLASS_COLORS,
    FIGSIZE,
    YEAR_XLIM,
    demand_response_eia861,
    mticker,
    pl,
    plt,
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
        "PA Demand Response — Residential: Potential vs. Actual Peak Savings (EIA-861)"
    )
    _ax1.set_ylabel("Peak Demand Savings (MW)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.set_ylim(0)
    _ax1.legend(loc="upper right")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
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
        "PA Demand Response — Commercial & Industrial: Potential vs. Actual Peak Savings (EIA-861)"
    )
    _ax2.set_xlabel("Year")
    _ax2.set_ylabel("Peak Demand Savings (MW)")
    _ax2.set_xlim(*YEAR_XLIM)
    _ax2.set_ylim(0)
    _ax2.legend(loc="upper right")
    _ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
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


@app.cell
def chart_ami_penetration(
    CLASS_COLORS,
    YEAR_XLIM,
    ami_eia861,
    mticker,
    pl,
    plt,
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


    def _stacked_bar(ax, df: pl.DataFrame, scale: float, unit: str) -> None:
        _yrs = df["year"].to_list()
        _ami = [v / scale for v in df["ami"].to_list()]
        _amr = [v / scale for v in df["amr"].to_list()]
        _std = [v / scale for v in df["standard"].to_list()]
        _est = [v / scale for v in df["standard_est"].to_list()]
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
        ax.set_xlim(*YEAR_XLIM)
        ax.set_ylim(0)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.1f}{unit}"))
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
        "PA Advanced Metering Infrastructure — AMI Penetration by Customer Class (EIA-861)"
    )
    _ax1.set_ylabel("Meters with AMI (%)")
    _ax1.set_xlim(*YEAR_XLIM)
    _ax1.set_ylim(0, 105)
    _ax1.legend(loc="upper left")
    _ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    style_ax(_ax1)

    _stacked_bar(_ax2, _meter_inventory("residential"), 1e6, "M")
    _ax2.set_title("PA Residential Meter Inventory by Type (EIA-861)")
    _ax2.set_ylabel("Meters (millions)")
    _ax2.legend(loc="upper left")

    _stacked_bar(_ax3, _meter_inventory("commercial"), 1e3, "K")
    _ax3.set_title("PA Commercial Meter Inventory by Type (EIA-861)")
    _ax3.set_ylabel("Meters (thousands)")
    _ax3.legend(loc="upper left")

    _stacked_bar(_ax4, _meter_inventory("industrial"), 1e3, "K")
    _ax4.set_title("PA Industrial Meter Inventory by Type (EIA-861)")
    _ax4.set_xlabel("Year")
    _ax4.set_ylabel("Meters (thousands)")
    _ax4.legend(loc="upper left")

    plt.tight_layout()
    chart_ami_penetration = _fig
    chart_ami_penetration
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


if __name__ == "__main__":
    app.run()
