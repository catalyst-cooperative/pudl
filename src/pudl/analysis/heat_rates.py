"""Use EPA CEMS and EIA data to estimate generator operational characteristics."""

from typing import Any

import numpy as np
import pandas as pd
from dagster import AssetIn, Field, asset

HEAT_RATE_ANALYSIS_CONFIG_SCHEMA = {
    "final_year": Field(
        int,
        default_value=2024,  # derive from dataset settings instead of hard coding?
        description=(
            "Final EPA CEMS year to include in the operational characteristics "
            "analysis."
        ),
    ),
    "num_years": Field(
        int,
        default_value=3,
        description=(
            "Number of historical EPA CEMS years to include, counting backward from "
            "the configured final year."
        ),
    ),
    "min_stable_level_consecutive_hours": Field(
        int,
        default_value=8,
        description=(
            "Minimum number of consecutive operating hours in a load-factor bin "
            "required for that bin to be considered a stable operating level."
        ),
    ),
    "states": Field(
        [str],
        default_value=[],
        description=(
            "Optional list of two-letter state abbreviations to include. If empty, "
            "all available states are included."
        ),
    ),
    "eia_monthly_generators_report_date": Field(
        str,
        default_value="2024-01-01",
        description=(
            "Report date from out_eia__monthly_generators to use in later analysis "
            "steps. The current output asset only uses EPA CEMS inputs."
        ),
    ),
    "eia_epa_mapping_year": Field(
        int,
        default_value=2024,  # derive from dataset settings instead of hard coding?
        description=(
            "Report year from core_epa__assn_eia_epacamd to use in later analysis "
            "steps. The current output asset only uses EPA CEMS inputs."
        ),
    ),
}


def _get_heat_rate_analysis_config(context) -> dict[str, Any]:
    """Extract heat rate analysis settings from Dagster asset config."""
    return {
        "final_year": context.op_config["final_year"],
        "num_years": context.op_config["num_years"],
        "min_stable_level_consecutive_hours": context.op_config[
            "min_stable_level_consecutive_hours"
        ],
        "states": context.op_config["states"],
        "eia_monthly_generators_report_date": context.op_config[
            "eia_monthly_generators_report_date"
        ],
        "eia_epa_mapping_year": context.op_config["eia_epa_mapping_year"],
    }


def filter_cems_for_heat_rate_analysis(
    core_epacems__hourly_emissions: pd.DataFrame,
    final_year: int,
    num_years: int,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Filter hourly EPA CEMS records to the configured analysis window.

    Args:
        core_epacems__hourly_emissions: Hourly CEMS emissions and gross load data.
        final_year: Final EPA CEMS year to include in the analysis.
        num_years: Number of historical years to include, counting backward from
            ``final_year``.
        states: Optional list of two-letter state abbreviations to include.

    Returns:
        Hourly EPA CEMS records filtered to the requested years and states.
    """
    start_year = final_year - num_years
    cems = core_epacems__hourly_emissions[
        core_epacems__hourly_emissions["year"].between(start_year, final_year)
    ].copy()

    if states:
        cems = cems[cems["state"].isin(states)].copy()

    cems["operating_datetime_utc"] = pd.to_datetime(cems["operating_datetime_utc"])
    return cems


def filter_eia_generators_for_heat_rate_analysis(
    out_eia__monthly_generators: pd.DataFrame,
    report_date: str,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Filter monthly EIA generator records to the configured snapshot.

    Args:
        out_eia__monthly_generators: Monthly EIA generator attributes.
        report_date: Report date to use as the EIA generator snapshot.
        states: Optional list of two-letter state abbreviations to include.

    Returns:
        Monthly generator records filtered to the requested snapshot and states.
    """
    report_timestamp = pd.Timestamp(report_date)
    generators = out_eia__monthly_generators[
        pd.to_datetime(out_eia__monthly_generators["report_date"]) == report_timestamp
    ].copy()

    if states:
        generators = generators[generators["state"].isin(states)].copy()

    return generators


def filter_eia_epa_mapping_for_heat_rate_analysis(
    core_epa__assn_eia_epacamd: pd.DataFrame,
    eia_epa_mapping_year: int,
) -> pd.DataFrame:
    """Filter the EPA/EIA crosswalk to one configured report year.

    Args:
        core_epa__assn_eia_epacamd: EPA/EIA crosswalk table.
        eia_epa_mapping_year: Report year to use when mapping EPA units to EIA
            generators.

    Returns:
        Unique EPA unit to EIA generator mappings for the requested report year.
    """
    return (
        core_epa__assn_eia_epacamd[
            core_epa__assn_eia_epacamd["report_year"] == eia_epa_mapping_year
        ][["plant_id_epa", "emissions_unit_id_epa", "plant_id_eia", "generator_id"]]
        .drop_duplicates()
        .copy()
    )


def summarize_eia_generators(
    generators: pd.DataFrame,
    eia_epa_mapping: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Summarize EIA generator capacity at plant, generator, and EPA unit levels.

    Args:
        generators: Filtered monthly EIA generator records.
        eia_epa_mapping: Filtered EPA/EIA crosswalk records.

    Returns:
        Dictionary containing plant-generator, plant, and plant-unit summaries.
    """
    generator_cols = [
        "plant_id_eia",
        "generator_id",
        "report_date",
        "prime_mover_code",
        "capacity_mw",
        "summer_capacity_mw",
        "winter_capacity_mw",
        "latitude",
        "longitude",
    ]
    plant_gen = generators.loc[:, generator_cols].copy()
    plant_gen["max_cap_mw"] = plant_gen[
        ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
    ].max(axis=1)
    plant_gen = plant_gen.merge(
        eia_epa_mapping,
        on=["plant_id_eia", "generator_id"],
        how="left",
    )

    plant = (
        generators.groupby(["plant_id_eia", "report_date"], as_index=False)[
            ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
        ]
        .sum()
        .assign(
            max_cap_mw=lambda x: x[
                ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
            ].max(axis=1),
        )
    )
    plant["max_mwh"] = plant["max_cap_mw"] * 24 * 30

    plant_unit = (
        plant_gen.groupby(["plant_id_eia", "emissions_unit_id_epa"], as_index=False)[
            ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
        ]
        .sum()
        .assign(
            max_cap_mw=lambda x: x[
                ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
            ].max(axis=1),
        )
    )

    return {
        "plant_gen": plant_gen,
        "plant": plant,
        "plant_unit": plant_unit,
    }


def summarize_eia923_monthly_plant_fuel(
    core_eia923__monthly_generation_fuel: pd.DataFrame,
    eia_plant_summary: pd.DataFrame,
    plant_ids_eia: pd.Series,
    start_year: int,
) -> pd.DataFrame:
    """Summarize monthly EIA 923 plant generation and fuel consumption.

    Args:
        core_eia923__monthly_generation_fuel: Monthly plant fuel and generation
            records.
        eia_plant_summary: Plant-level generator capacity summary.
        plant_ids_eia: EIA plant IDs to include.
        start_year: First report year to include.

    Returns:
        Monthly plant-level EIA 923 generation, fuel, heat rate, and load factor.
    """
    eia923 = core_eia923__monthly_generation_fuel.copy()
    eia923["report_date"] = pd.to_datetime(eia923["report_date"])
    eia923["year"] = eia923["report_date"].dt.year
    eia923["month"] = eia923["report_date"].dt.month
    eia923 = eia923[
        (eia923["year"] >= start_year)
        & (eia923["data_maturity"] == "final")
        & (eia923["plant_id_eia"].isin(plant_ids_eia.dropna().unique()))
    ].copy()

    monthly_plant = (
        eia923.groupby(["plant_id_eia", "year", "month"], as_index=False)[
            [
                "net_generation_mwh",
                "fuel_consumed_mmbtu",
                "fuel_consumed_for_electricity_mmbtu",
            ]
        ]
        .sum()
        .merge(eia_plant_summary, on="plant_id_eia", how="left")
    )
    monthly_plant["heat_rate_mmbtu_per_mwh_net_generation"] = (
        monthly_plant["fuel_consumed_for_electricity_mmbtu"]
        / monthly_plant["net_generation_mwh"]
    )
    monthly_plant["load_factor_net_generation"] = (
        monthly_plant["net_generation_mwh"] / monthly_plant["max_mwh"]
    )

    return monthly_plant


def summarize_cems_monthly_plant_operations(
    cems: pd.DataFrame,
    eia_plant_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize monthly EPA CEMS plant gross load and fuel consumption.

    Args:
        cems: Filtered hourly EPA CEMS records.
        eia_plant_summary: Plant-level generator capacity summary.

    Returns:
        Monthly plant-level CEMS gross load, fuel, heat rate, and load factor.
    """
    cems_working = cems.copy()
    cems_working["month"] = cems_working["operating_datetime_utc"].dt.month
    monthly_plant = (
        cems_working.groupby(["plant_id_eia", "year", "month"], as_index=False)[
            ["gross_load_mw", "heat_content_mmbtu"]
        ]
        .sum()
        .merge(eia_plant_summary, on="plant_id_eia", how="left")
    )
    monthly_plant["heat_rate_mmbtu_per_mwh_gross_load"] = (
        monthly_plant["heat_content_mmbtu"] / monthly_plant["gross_load_mw"]
    )
    monthly_plant["load_factor_gross_load"] = (
        monthly_plant["gross_load_mw"] / monthly_plant["max_mwh"]
    )

    return monthly_plant


def constant_fit(x: np.ndarray, y: np.ndarray) -> dict[str, float | str]:
    """Estimate a constant gross-to-net conversion factor.

    Args:
        x: Observed load factors.
        y: Observed gross-to-net generation conversion factors.

    Returns:
        Fitted conversion factor metadata.
    """
    a0 = float(np.mean(y))
    min_obs_lf = float(np.min(x))
    max_obs_lf = float(np.max(x))
    return {
        "a1": 0.0,
        "a0": a0,
        "fit_type": "constant",
        "min_obs_lf": min_obs_lf,
        "max_obs_lf": max_obs_lf,
        "gen_cems_to_net_gen_conversion_factor_at_min_load_factor": a0,
        "gen_cems_to_net_gen_conversion_factor_at_max_load_factor": a0,
    }


def estimate_gross_to_net_conversion_factors(
    cems_monthly_plant_summary: pd.DataFrame,
    eia923_monthly_plant_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Estimate plant-level conversion factors from CEMS gross load to net generation.

    Args:
        cems_monthly_plant_summary: Monthly plant-level CEMS summary.
        eia923_monthly_plant_summary: Monthly plant-level EIA 923 summary.

    Returns:
        Plant-level conversion factor estimates and supporting fit metadata.
    """
    conversion = cems_monthly_plant_summary.merge(
        eia923_monthly_plant_summary,
        on=[
            "plant_id_eia",
            "year",
            "month",
            "report_date",
            "capacity_mw",
            "summer_capacity_mw",
            "winter_capacity_mw",
            "max_cap_mw",
            "max_mwh",
        ],
        how="left",
        suffixes=("_cems", "_eia923"),
    )
    conversion["gen_cems_to_net_gen_conversion_factor"] = (
        conversion["net_generation_mwh"] / conversion["gross_load_mw"]
    )
    conversion["fuel_cems_to_eia923_conversion_factor"] = (
        conversion["fuel_consumed_for_electricity_mmbtu"]
        / conversion["heat_content_mmbtu"]
    )
    conversion = (
        conversion.replace([np.inf, -np.inf], np.nan)
        .dropna(
            subset=[
                "plant_id_eia",
                "load_factor_gross_load",
                "gen_cems_to_net_gen_conversion_factor",
            ]
        )
        .query(
            "0 <= load_factor_gross_load <= 1 "
            "and 0 <= gen_cems_to_net_gen_conversion_factor <= 1"
        )
    )

    plant_fits = []
    for plant_id_eia, plant_df in conversion.groupby("plant_id_eia"):
        x = plant_df["load_factor_gross_load"].to_numpy()
        y = plant_df["gen_cems_to_net_gen_conversion_factor"].to_numpy()
        fit = constant_fit(x, y)
        plant_fits.append(
            {
                "plant_id_eia": plant_id_eia,
                **fit,
                "n_obs": len(x),
                "fuel_cems_to_eia923_conversion_factor": plant_df[
                    "fuel_cems_to_eia923_conversion_factor"
                ].mean(),
            }
        )

    return pd.DataFrame(plant_fits)


def add_adjusted_net_generation_to_cems(
    cems: pd.DataFrame,
    conversion_factors: pd.DataFrame,
    eia_plant_unit_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Add estimated net generation and adjusted heat rates to hourly CEMS records.

    Args:
        cems: Filtered hourly EPA CEMS records.
        conversion_factors: Plant-level gross-to-net and fuel conversion factors.
        eia_plant_unit_summary: EIA capacity summary by plant and EPA emissions unit.

    Returns:
        Hourly CEMS records with estimated net generation, adjusted fuel, adjusted heat
        rates, and adjusted load factors.
    """
    cems_adjusted = (
        cems.merge(conversion_factors, on="plant_id_eia", how="left")
        .merge(
            eia_plant_unit_summary[
                ["plant_id_eia", "emissions_unit_id_epa", "capacity_mw", "max_cap_mw"]
            ],
            on=["plant_id_eia", "emissions_unit_id_epa"],
            how="left",
        )
        .copy()
    )
    cems_adjusted["net_generation_mwh_cems"] = (
        cems_adjusted["gross_load_mw"]
        * cems_adjusted["gen_cems_to_net_gen_conversion_factor_at_max_load_factor"]
    )
    cems_adjusted["fuel_consumed_for_electricity_mmbtu_cems"] = (
        cems_adjusted["heat_content_mmbtu"]
        * cems_adjusted["fuel_cems_to_eia923_conversion_factor"]
    )
    cems_adjusted["heat_rate_net_generation_cems"] = (
        cems_adjusted["fuel_consumed_for_electricity_mmbtu_cems"]
        / cems_adjusted["net_generation_mwh_cems"]
    )
    cems_adjusted["load_factor_adjusted_cems"] = (
        cems_adjusted["net_generation_mwh_cems"] / cems_adjusted["max_cap_mw"]
    )

    return cems_adjusted


def _consecutive_run_ids(datetimes: pd.Series) -> pd.Series:
    """Identify consecutive hourly runs within a datetime series."""
    return datetimes.diff().dt.total_seconds().div(3600).ne(1).cumsum()


def min_stable_level(
    hourly_plant_unit: pd.DataFrame,
    consecutive_hours: int,
    load_factor_bin_col: str,
) -> tuple[pd.Interval | float, float]:
    """Find the smallest load-factor bin with a sufficiently long operating run."""
    stable_level = hourly_plant_unit.sort_values("operating_datetime_utc").copy()
    bins = sorted(stable_level[load_factor_bin_col].dropna().unique())

    for candidate_bin in bins[1:]:
        candidate = stable_level[stable_level[load_factor_bin_col] == candidate_bin]
        run_ids = _consecutive_run_ids(candidate["operating_datetime_utc"])
        has_stable_run = (
            not candidate.empty
            and candidate.groupby(run_ids).size().max() >= consecutive_hours
        )
        if has_stable_run:
            return candidate_bin, candidate_bin.left

    return np.nan, np.nan


def min_up_down_times(
    hourly_plant_unit: pd.DataFrame,
    min_stable_level_bin: pd.Interval,
    load_factor_col: str,
    load_factor_bin_col: str,
) -> tuple[float, float, Any, Any]:
    """Estimate minimum up and down times from hourly plant-unit observations."""
    if pd.isna(min_stable_level_bin):
        return np.nan, np.nan, pd.NaT, pd.NaT

    up_down = hourly_plant_unit.sort_values("operating_datetime_utc").copy()
    up = up_down[up_down[load_factor_bin_col] >= min_stable_level_bin].copy()
    down = up_down[up_down[load_factor_col].isna()].copy()

    min_up_time = np.nan
    min_up_datetime_utc = pd.NaT
    if not up.empty:
        up_run_ids = _consecutive_run_ids(up["operating_datetime_utc"])
        up_run_sizes = up.groupby(up_run_ids).size()
        min_up_time = up_run_sizes.min()
        min_up_run_id = up_run_sizes.idxmin()
        min_up_datetime_utc = (
            up.groupby(up_run_ids)["operating_datetime_utc"].min().loc[min_up_run_id]
        )

    min_down_time = np.nan
    min_down_datetime_utc = pd.NaT
    if not down.empty:
        down_run_ids = _consecutive_run_ids(down["operating_datetime_utc"])
        down_run_sizes = down.groupby(down_run_ids).size()
        min_down_time = down_run_sizes.min()
        min_down_run_id = down_run_sizes.idxmin()
        min_down_datetime_utc = (
            down.groupby(down_run_ids)["operating_datetime_utc"]
            .min()
            .loc[min_down_run_id]
        )

    return min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc


def heat_rate(
    hourly_plant_unit: pd.DataFrame,
    min_stable_level_bin: pd.Interval,
    load_factor_col: str,
    load_factor_bin_col: str,
    heat_rate_col: str,
) -> tuple[float, float]:
    """Estimate median heat rates at maximum load and minimum stable load."""
    heat_rates = hourly_plant_unit.dropna(subset=[load_factor_col, heat_rate_col])
    if heat_rates.empty or pd.isna(min_stable_level_bin):
        return np.nan, np.nan

    max_load_factor_bin = heat_rates[load_factor_bin_col].max()
    max_load_heat_rate = heat_rates.loc[
        heat_rates[load_factor_bin_col] == max_load_factor_bin, heat_rate_col
    ].median()
    min_stable_heat_rate = heat_rates.loc[
        heat_rates[load_factor_bin_col] == min_stable_level_bin, heat_rate_col
    ].median()

    return max_load_heat_rate, min_stable_heat_rate


def ramp_rate(
    hourly_plant_unit: pd.DataFrame,
    generation_col: str,
) -> tuple[float, float]:
    """Estimate ramp-up and ramp-down rates from hourly generation changes."""
    ramp = hourly_plant_unit.sort_values("operating_datetime_utc").copy()
    ramp["time_delta"] = (
        ramp["operating_datetime_utc"].diff().dt.total_seconds().div(3600)
    )
    ramp["generation_delta"] = ramp[generation_col].diff()
    ramp["ramp_rate"] = ramp["generation_delta"] / ramp["time_delta"]
    ramp = ramp.replace([np.inf, -np.inf], np.nan).dropna(subset=["ramp_rate"])

    if ramp.empty:
        return np.nan, np.nan

    ramp["ramp_rate_bin"] = pd.qcut(
        ramp["ramp_rate"],
        q=min(20, ramp["ramp_rate"].nunique()),
        duplicates="drop",
    )
    low_bin = ramp["ramp_rate_bin"].min()
    high_bin = ramp["ramp_rate_bin"].max()
    ramp_down_rate = ramp.loc[ramp["ramp_rate_bin"] == low_bin, "ramp_rate"].median()
    ramp_up_rate = ramp.loc[ramp["ramp_rate_bin"] == high_bin, "ramp_rate"].median()

    return ramp_up_rate, ramp_down_rate


def estimate_unit_operational_characteristics(
    hourly_plant_unit: pd.DataFrame,
    consecutive_hours: int,
    load_factor_col: str,
    generation_col: str,
    heat_rate_col: str,
    max_load_col: str,
    output_max_load_col: str,
    output_ramp_rate_col_suffix: str,
) -> pd.DataFrame:
    """Estimate operational characteristics for one EPA CEMS plant-unit pair."""
    plant_unit = hourly_plant_unit.sort_values("operating_datetime_utc").copy()
    plant_id_epa = plant_unit["plant_id_epa"].iloc[0]
    emissions_unit_id_epa = plant_unit["emissions_unit_id_epa"].iloc[0]
    max_load = plant_unit[max_load_col].max()
    load_factor_bin_col = f"{load_factor_col}_bin"

    valid_load_factors = plant_unit[load_factor_col].dropna()
    if valid_load_factors.nunique() > 1:
        plant_unit[load_factor_bin_col] = pd.cut(
            plant_unit[load_factor_col],
            bins=10,
            right=True,
            include_lowest=False,
        )
        min_stable_level_bin, min_stable_level_value = min_stable_level(
            plant_unit,
            consecutive_hours=consecutive_hours,
            load_factor_bin_col=load_factor_bin_col,
        )
        max_load_heat_rate, min_stable_heat_rate = heat_rate(
            plant_unit,
            min_stable_level_bin=min_stable_level_bin,
            load_factor_col=load_factor_col,
            load_factor_bin_col=load_factor_bin_col,
            heat_rate_col=heat_rate_col,
        )
        min_up_time, min_down_time, _, _ = min_up_down_times(
            plant_unit,
            min_stable_level_bin=min_stable_level_bin,
            load_factor_col=load_factor_col,
            load_factor_bin_col=load_factor_bin_col,
        )
        ramp_up, ramp_down = ramp_rate(plant_unit, generation_col=generation_col)
    else:
        min_stable_level_value = np.nan
        min_up_time = np.nan
        min_down_time = np.nan
        max_load_heat_rate = np.nan
        min_stable_heat_rate = np.nan
        ramp_up = np.nan
        ramp_down = np.nan

    ramp_up_fraction = ramp_up / max_load / 60 if max_load else np.nan
    ramp_down_fraction = ramp_down / max_load / 60 if max_load else np.nan

    output = {
        "plant_id_epa": plant_id_epa,
        "emissions_unit_id_epa": emissions_unit_id_epa,
    }
    if "plant_id_eia" in plant_unit:
        output["plant_id_eia"] = plant_unit["plant_id_eia"].iloc[0]
    output |= {
        output_max_load_col: max_load,
        "min_stable_level": min_stable_level_value,
        "min_up_time_hr": min_up_time,
        "min_down_time_hr": min_down_time,
        "heat_rate_at_max_load_factor_mmbtu_per_mwh": round(max_load_heat_rate, 2),
        "heat_rate_at_min_stable_level_mmbtu_per_mwh": round(min_stable_heat_rate, 2),
        f"ramp_up_rate_fraction_of_{output_ramp_rate_col_suffix}_per_min": round(
            ramp_up_fraction, 2
        ),
        f"ramp_down_rate_fraction_of_{output_ramp_rate_col_suffix}_per_min": round(
            ramp_down_fraction, 2
        ),
    }

    return pd.DataFrame([output])


def estimate_operational_characteristics_by_unit(
    cems: pd.DataFrame,
    consecutive_hours: int,
    adjusted: bool = False,
) -> pd.DataFrame:
    """Estimate operational characteristics for every EPA CEMS plant-unit pair."""
    cems_working = cems.copy()
    if adjusted:
        load_factor_col = "load_factor_adjusted_cems"
        generation_col = "net_generation_mwh_cems"
        heat_rate_col = "heat_rate_net_generation_cems"
        max_load_col = "max_cap_mw"
        output_max_load_col = "max_cap_mw"
        output_ramp_rate_col_suffix = "max_cap_mw"
    else:
        max_gross_load = cems_working.groupby(
            ["plant_id_epa", "emissions_unit_id_epa"]
        )["gross_load_mw"].transform("max")
        cems_working["max_gross_load_mw"] = max_gross_load
        cems_working["load_factor"] = (
            cems_working["gross_load_mw"] / cems_working["max_gross_load_mw"]
        )
        cems_working["gross_load_mwh"] = (
            cems_working["gross_load_mw"] * cems_working["operating_time_hours"]
        )
        cems_working["heat_rate_mmbtu_per_mwh"] = (
            cems_working["heat_content_mmbtu"] / cems_working["gross_load_mwh"]
        )
        load_factor_col = "load_factor"
        generation_col = "gross_load_mwh"
        heat_rate_col = "heat_rate_mmbtu_per_mwh"
        max_load_col = "max_gross_load_mw"
        output_max_load_col = "max_gross_load_mw"
        output_ramp_rate_col_suffix = "max_gross_load"

    outputs = [
        estimate_unit_operational_characteristics(
            hourly_plant_unit=plant_unit,
            consecutive_hours=consecutive_hours,
            load_factor_col=load_factor_col,
            generation_col=generation_col,
            heat_rate_col=heat_rate_col,
            max_load_col=max_load_col,
            output_max_load_col=output_max_load_col,
            output_ramp_rate_col_suffix=output_ramp_rate_col_suffix,
        )
        for _, plant_unit in cems_working.groupby(
            ["plant_id_epa", "emissions_unit_id_epa"]
        )
    ]

    return pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()


def operational_characteristics(
    core_epacems__hourly_emissions: pd.DataFrame,
    final_year: int,
    num_years: int,
    min_stable_level_consecutive_hours: int,
    states: list[str] | None,
) -> pd.DataFrame:
    """Estimate EPA CEMS unit operational characteristics.

    This table corresponds to the script output named ``epa_op_char_output_df.csv``.
    """
    cems = filter_cems_for_heat_rate_analysis(
        core_epacems__hourly_emissions=core_epacems__hourly_emissions,
        final_year=final_year,
        num_years=num_years,
        states=states,
    )
    return estimate_operational_characteristics_by_unit(
        cems=cems,
        consecutive_hours=min_stable_level_consecutive_hours,
    )


@asset(
    name="out_epacems__yearly_operational_characteristics",
    ins={
        "core_epacems__hourly_emissions": AssetIn(input_manager_key="pudl_io_manager"),
    },
    io_manager_key="pudl_io_manager",
    compute_kind="Python",
    config_schema=HEAT_RATE_ANALYSIS_CONFIG_SCHEMA,
    op_tags={"memory-use": "high"},
)
def out_epacems__yearly_operational_characteristics(
    context,
    core_epacems__hourly_emissions: pd.DataFrame,
) -> pd.DataFrame:
    """Estimate EPA CEMS unit operational characteristics."""
    heat_rate_config = _get_heat_rate_analysis_config(context)
    return operational_characteristics(
        core_epacems__hourly_emissions=core_epacems__hourly_emissions,
        final_year=heat_rate_config["final_year"],
        num_years=heat_rate_config["num_years"],
        min_stable_level_consecutive_hours=heat_rate_config[
            "min_stable_level_consecutive_hours"
        ],
        states=heat_rate_config["states"],
    )
