# Copyright 2026 Sylvan Energy Analytics LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Frozen, verbatim reference copy of the original script -- not maintained code.
# ruff: noqa: B007, D100, D103, F821, F841, N802, N812, N816, PD101, RET503, S608

# %% Import libraries

import duckdb as DUCK
import numpy as np
import pandas as pd

# %% User Inputs
final_year = 2024  # final year in EPA CEMS data
num_years = 3  # number of historical years to include in the analysis
min_stbl_lvl_consecutive_hours = (
    8  # number of consecutive hours at a load factor to be considered stable
)
state_filter = True
states = ["CA"]
states_str = ",".join(f"'{s}'" for s in states)
eia_monthly_generators_report_date = "2024-01-01"
eia_epa_mapping_year = 2024


# %% Pudl Imports
def pudl_query(query_str):
    con = DUCK.connect()
    df = con.execute(query_str).df()
    return df


if state_filter:
    epa_cems_str = f"""
    SELECT *
    FROM 's3://pudl.catalyst.coop/nightly/core_epacems__hourly_emissions.parquet'
    WHERE year <= {final_year} AND year >= {final_year - num_years} AND state IN ({states_str})
    """
    eia_860_str = f"""
    SELECT *
    FROM 's3://pudl.catalyst.coop/nightly/out_eia__monthly_generators.parquet'
    WHERE report_date = '{eia_monthly_generators_report_date}' AND state IN ({states_str})
    """

else:
    epa_cems_str = f"""
    SELECT *
    FROM 's3://pudl.catalyst.coop/nightly/core_epacems__hourly_emissions.parquet'
    WHERE year <= {final_year} AND year >= {final_year - num_years}
    """
    eia_860_str = f"""
    SELECT *
    FROM 's3://pudl.catalyst.coop/nightly/out_eia__monthly_generators.parquet'
    WHERE report_date = '{eia_monthly_generators_report_date}'
    """


eia_epa_mapping_str = f"""
SELECT *
FROM 's3://pudl.catalyst.coop/nightly/core_epa__assn_eia_epacamd.parquet'
WHERE report_year = '{eia_epa_mapping_year}'
"""
eia_923_str = """
SELECT *
FROM 's3://pudl.catalyst.coop/nightly/core_eia923__monthly_generation_fuel.parquet'
"""

# %%
epa_cems_df = pudl_query(epa_cems_str)

# %%
eia_860_df = pudl_query(eia_860_str)
# %%
eia_923_df = pudl_query(eia_923_str)
# %%
eia_epa_mapping_df = pudl_query(eia_epa_mapping_str)
# %% Op Char Function Definitions
epa_op_char_output_df = pd.DataFrame([])


# estimate the min stable level for the unit by finding the smallest bin with at least 5 consecutve operating hours
def min_stable_level(hourly_plant_unit_df, consecutive_hours):
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


# estimate min up and down times by finding the smallest bins operating a) at or above min stbl level and b) at 0
def min_up_down_times(hourly_plant_unit_df, min_stbl_lvl):
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
    min_up_run_id = up_run_sizes.idxmin()
    min_up_datetime_utc = (
        up_df.groupby(up_run_id)["operating_datetime_utc"].min().loc[min_up_run_id]
    )

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
    min_down_run_id = down_run_sizes.idxmin()
    min_down_datetime_utc = (
        down_df.groupby(down_run_id)["operating_datetime_utc"]
        .min()
        .loc[min_down_run_id]
    )

    return min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc


# estimate heat rates at min stable level and at max load_factor_bin
def heat_rate(hourly_plant_unit_df, min_stbl_lvl):
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


# estimate ramp rates by adding a ramp_rate column to the dataframe as the MWh delta/h delta, then group these into 20 bins and set the median of the highest bin to be the ramp_up rate and the median of th elowest to be the ramp down rate
def ramp_rate(hourly_plant_unit_df):
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


def build_op_char_df(plant_id, unit_id):
    epa_cems_plant_unit_df = epa_cems_df[
        (epa_cems_df["plant_id_epa"] == plant_id)
        & (epa_cems_df["emissions_unit_id_epa"] == unit_id)
    ]
    max_gross_load_mw = epa_cems_plant_unit_df["gross_load_mw"].max()
    plant_id_eia = epa_cems_plant_unit_df["plant_id_eia"].iloc[0]
    # set load factor and heat rate data to NAN if load_factor is less than .02
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
    epa_cems_plant_unit_df["time_delta"] = (
        epa_cems_plant_unit_df["operating_datetime_utc"]
        .diff()
        .dt.total_seconds()
        .div(3600)
    )
    valid_load_factors = epa_cems_plant_unit_df["load_factor"].dropna()
    if valid_load_factors.nunique() > 1:
        epa_cems_plant_unit_df["load_factor_bin"] = pd.cut(
            epa_cems_plant_unit_df["load_factor"],
            bins=10,
            right=True,
            include_lowest=False,
        )
        min_stbl_lvl_bin, min_stbl_lvl = min_stable_level(
            epa_cems_plant_unit_df, min_stbl_lvl_consecutive_hours
        )
        max_lf_hr, min_stbl_hr = heat_rate(epa_cems_plant_unit_df, min_stbl_lvl_bin)
        min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc = (
            min_up_down_times(epa_cems_plant_unit_df, min_stbl_lvl_bin)
        )
        ramp_up_rate, ramp_down_rate = ramp_rate(epa_cems_plant_unit_df)
    else:
        epa_cems_plant_unit_df["load_factor_bin"] = np.nan
        min_stbl_lvl = np.nan
        max_lf_hr = np.nan
        min_stbl_hr = np.nan
        min_up_time = np.nan
        min_down_time = np.nan
        min_up_datetime_utc = np.nan
        min_down_datetime_utc = np.nan
        ramp_up_rate = np.nan
        ramp_down_rate = np.nan

    plant_unit_op_char_df = pd.DataFrame(
        {
            "plant_id_epa": [plant_id],
            "emissions_unit_id_epa": [unit_id],
            "plant_id_eia": [plant_id_eia],
            "max_gross_load_mw": [max_gross_load_mw],
            "min_stable_level": [min_stbl_lvl],
            "min_up_time_hr": [min_up_time],
            "min_down_time_hr": [min_down_time],
            "heat_rate_at_max_load_factor_mmbtu_per_mwh": [round(max_lf_hr, 2)],
            "heat_rate_at_min_stable_level_mmbtu_per_mwh": [round(min_stbl_hr, 2)],
            "ramp_up_rate_fraction_of_max_gross_load_per_min": [
                round(ramp_up_rate / max_gross_load_mw / 60, 2)
            ],
            "ramp_down_rate_fraction_of_max_gross_load_per_min": [
                round(ramp_down_rate / max_gross_load_mw / 60, 2)
            ],
        }
    )

    return plant_unit_op_char_df


# %% iterate through plant unit pairs and build EPA op char df
epa_plant_unit_pairs = epa_cems_df[
    ["plant_id_epa", "emissions_unit_id_epa"]
].drop_duplicates()

for index, row in epa_plant_unit_pairs.iterrows():
    plant_id = row["plant_id_epa"]
    unit_id = row["emissions_unit_id_epa"]
    plant_unit_op_char_df = build_op_char_df(plant_id, unit_id)
    epa_op_char_output_df = pd.concat([epa_op_char_output_df, plant_unit_op_char_df])

epa_op_char_output_df.to_csv("epa_op_char_output_df.csv", index=False)

# %% drop EPA generator id from mapping df
eia_epa_mapping_working = eia_epa_mapping_df[
    ["plant_id_epa", "emissions_unit_id_epa", "plant_id_eia", "generator_id"]
]
eia_epa_mapping_working = eia_epa_mapping_working.drop_duplicates()

# %% EIA 860 summarys at variety of plant/gen/unit aggreagations
eia_860_working = eia_860_df.copy()

eia_860_plant_gen_summary = eia_860_working[
    [
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
]
eia_860_plant_gen_summary["max_cap_mw"] = eia_860_plant_gen_summary[
    ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
].max(axis=1)
eia_860_plant_gen_summary = pd.merge(
    eia_860_plant_gen_summary,
    eia_epa_mapping_working,
    on=["plant_id_eia", "generator_id"],
    how="left",
)

eia_860_plant_gen_summary.to_csv("eia_860_plant_gen_summary.csv", index=False)

eia_860_plant_summary = (
    eia_860_working.groupby(["plant_id_eia", "report_date"])
    .agg(
        {"capacity_mw": "sum", "summer_capacity_mw": "sum", "winter_capacity_mw": "sum"}
    )
    .reset_index()
)
eia_860_plant_summary["max_cap_mw"] = eia_860_plant_summary[
    ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
].max(axis=1)
eia_860_plant_summary["max_mwh"] = eia_860_plant_summary["max_cap_mw"] * 24 * 30

eia_860_plant_summary.to_csv("eia_860_plant_summary.csv", index=False)

eia_860_plant_unit_summary = (
    eia_860_plant_gen_summary.groupby(["plant_id_eia", "emissions_unit_id_epa"])
    .agg(
        {"capacity_mw": "sum", "summer_capacity_mw": "sum", "winter_capacity_mw": "sum"}
    )
    .reset_index()
)
eia_860_plant_unit_summary["max_cap_mw"] = eia_860_plant_unit_summary[
    ["capacity_mw", "summer_capacity_mw", "winter_capacity_mw"]
].max(axis=1)

eia_860_plant_unit_summary.to_csv("eia_860_plant_unit_summary.csv", index=False)

# %% EIA 923 Monthly Summary
eia_923_working = eia_923_df.copy()
eia_923_working["report_date"] = pd.to_datetime(eia_923_working["report_date"])
eia_923_working["year"] = eia_923_working["report_date"].dt.year
eia_923_working["month"] = eia_923_working["report_date"].dt.month
eia_923_working = eia_923_working[eia_923_working["year"] > 2021]
eia_923_working = eia_923_working[eia_923_working["data_maturity"] == "final"]
eia_923_working = eia_923_working[
    eia_923_working["plant_id_eia"].isin(pd.unique(epa_op_char_merged["plant_id_eia"]))
]
eia_923_monthly_plant_summary = (
    eia_923_working.groupby(["plant_id_eia", "year", "month"])
    .agg(
        {
            "net_generation_mwh": "sum",
            "fuel_consumed_mmbtu": "sum",
            "fuel_consumed_for_electricity_mmbtu": "sum",
        }
    )
    .reset_index()
)
eia_923_monthly_plant_summary["heat_rate_mmbtu_per_mwh_NET_GEN"] = (
    eia_923_monthly_plant_summary["fuel_consumed_for_electricity_mmbtu"]
    / eia_923_monthly_plant_summary["net_generation_mwh"]
)
eia_923_monthly_plant_summary = pd.merge(
    eia_923_monthly_plant_summary,
    eia_860_plant_summary,
    on=["plant_id_eia"],
    how="left",
)
eia_923_monthly_plant_summary["load_factor_NET_GEN"] = (
    eia_923_monthly_plant_summary["net_generation_mwh"]
    / eia_923_monthly_plant_summary["max_mwh"]
)

eia_923_monthly_plant_summary.to_csv("eia_923_monthly_plant_summary.csv", index=False)

# %% EPA CEMS monthly summary
epa_cems_working = epa_cems_df.copy()
epa_cems_working["month"] = pd.to_datetime(
    epa_cems_working["operating_datetime_utc"]
).dt.month
epa_cems_monthly_plant_summary = (
    epa_cems_working.groupby(["plant_id_eia", "year", "month"])
    .agg({"gross_load_mw": "sum", "heat_content_mmbtu": "sum"})
    .reset_index()
)
epa_cems_monthly_plant_summary["heat_rate_mmbtu_per_mwh_GROSS_LOAD"] = (
    epa_cems_monthly_plant_summary["heat_content_mmbtu"]
    / epa_cems_monthly_plant_summary["gross_load_mw"]
)
epa_cems_monthly_plant_summary = pd.merge(
    epa_cems_monthly_plant_summary,
    eia_860_plant_summary,
    on=["plant_id_eia"],
    how="left",
)
epa_cems_monthly_plant_summary["load_factor_GROSS_LOAD"] = (
    epa_cems_monthly_plant_summary["gross_load_mw"]
    / epa_cems_monthly_plant_summary["max_mwh"]
)

epa_cems_monthly_plant_summary.to_csv("epa_cems_monthly_plant_summary.csv", index=False)

# %% calculate monthly conversion factors mapping EPA gross load to net gen
conversion_df = pd.merge(
    epa_cems_monthly_plant_summary,
    eia_923_monthly_plant_summary,
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
)
conversion_df["gen_CEMS_to_net_gen_conversion_factor"] = (
    conversion_df["net_generation_mwh"] / conversion_df["gross_load_mw"]
)
conversion_df["fuel_CEMS_to_923_conversion_factor"] = (
    conversion_df["fuel_consumed_for_electricity_mmbtu"]
    / conversion_df["heat_content_mmbtu"]
)
conversion_df.to_csv("conversion_df.csv", index=False)


# %% Gross Load to Net Gen Converstion Functions
def linear_fit(x, y):
    a1, a0 = np.polyfit(x, y, 1)
    fit_type = "linear"
    min_obs_lf = np.min(x)
    max_obs_lf = np.max(x)
    share_at_min_load_factor = a1 * min_obs_lf + a0
    share_at_max_load_factor = a1 * max_obs_lf + a0
    return (
        a1,
        a0,
        fit_type,
        min_obs_lf,
        max_obs_lf,
        share_at_min_load_factor,
        share_at_max_load_factor,
    )


def constant_fit(x, y):
    a1, a0 = 0, np.mean(y)
    fit_type = "constant"
    min_obs_lf = np.min(x)
    max_obs_lf = np.max(x)
    share_at_min_load_factor = a0
    share_at_max_load_factor = a0
    return (
        a1,
        a0,
        fit_type,
        min_obs_lf,
        max_obs_lf,
        share_at_min_load_factor,
        share_at_max_load_factor,
    )


def conversion_fit(plant_id):
    plant_df = conversion_working[conversion_working["plant_id_eia"] == plant_id]
    x = plant_df["load_factor_GROSS_LOAD"].to_numpy()
    y = plant_df["gen_CEMS_to_net_gen_conversion_factor"].to_numpy()

    (
        a1,
        a0,
        fit_type,
        min_obs_lf,
        max_obs_lf,
        share_at_min_load_factor,
        share_at_max_load_factor,
    ) = constant_fit(x, y)

    fuel_share = plant_df["fuel_CEMS_to_923_conversion_factor"].mean()

    plant_fit_df = pd.DataFrame(
        {
            "plant_id_eia": [plant_id],
            "a1": [a1],
            "a0": [a0],
            "fit_type": [fit_type],
            "n_obs": [len(x)],
            "min_obs_lf": [min_obs_lf],
            "max_obs_lf": [max_obs_lf],
            "gen_CEMS_to_net_gen_conversion_factor_at_min_load_factor": [
                share_at_min_load_factor
            ],
            "gen_CEMS_to_net_gen_conversion_factor_at_max_load_factor": [
                share_at_max_load_factor
            ],
            "fuel_CEMS_to_923_conversion_factor": [fuel_share],
        }
    )
    return plant_fit_df


# %% Gross Load to Net Gen Conversion Iteration on Plant ID
conversion_working = conversion_df.copy()
conversion_working = conversion_working.replace([np.inf, -np.inf], np.nan)
conversion_working = conversion_working.dropna(
    subset=[
        "plant_id_eia",
        "load_factor_GROSS_LOAD",
        "gen_CEMS_to_net_gen_conversion_factor",
    ]
)
conversion_working = conversion_working[
    (conversion_working["load_factor_GROSS_LOAD"] >= 0)
    & (conversion_working["load_factor_GROSS_LOAD"] <= 1)
    & (conversion_working["gen_CEMS_to_net_gen_conversion_factor"] >= 0)
    & (conversion_working["gen_CEMS_to_net_gen_conversion_factor"] <= 1)
]

conversion_fit_df = pd.DataFrame()

for plant_id in pd.unique(conversion_working["plant_id_eia"]):
    plant_fit_df = conversion_fit(plant_id)
    conversion_fit_df = pd.concat([conversion_fit_df, plant_fit_df], ignore_index=True)

conversion_fit_df.to_csv("conversion_fit_df.csv", index=False)

# %%
epa_cems_w_net_gen = pd.merge(
    epa_cems_working, conversion_fit_df, on=["plant_id_eia"], how="left"
)
epa_cems_w_net_gen = pd.merge(
    epa_cems_w_net_gen,
    eia_860_plant_unit_summary[
        ["plant_id_eia", "emissions_unit_id_epa", "capacity_mw", "max_cap_mw"]
    ],
    on=["plant_id_eia", "emissions_unit_id_epa"],
    how="left",
)
epa_cems_w_net_gen["net_generation_mwh_CEMS"] = (
    epa_cems_w_net_gen["gross_load_mw"]
    * epa_cems_w_net_gen["gen_CEMS_to_net_gen_conversion_factor_at_max_load_factor"]
)
epa_cems_w_net_gen["fuel_consumed_for_electricity_mmbtu_CEMS"] = (
    epa_cems_w_net_gen["heat_content_mmbtu"]
    * epa_cems_w_net_gen["fuel_CEMS_to_923_conversion_factor"]
)
epa_cems_w_net_gen["heat_rate_net_gen_CEMS"] = (
    epa_cems_w_net_gen["fuel_consumed_for_electricity_mmbtu_CEMS"]
    / epa_cems_w_net_gen["net_generation_mwh_CEMS"]
)
epa_cems_w_net_gen["load_factor_adjusted_CEMS"] = (
    epa_cems_w_net_gen["net_generation_mwh_CEMS"] / epa_cems_w_net_gen["max_cap_mw"]
)
epa_cems_w_net_gen.to_csv("epa_cems_w_net_gen.csv", index=False)

# %% Op Char Functions for adjusted load factor and heat rates

epa_op_char_output_df_ADJUSTED = pd.DataFrame([])


# estimate the min stable level for the unit by finding the smallest bin with at least 5 consecutve operating hours
def min_stable_level_ADJUSTED(hourly_plant_unit_df, consecutive_hours):
    min_stbl_lvl_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
    bins = sorted(min_stbl_lvl_df["load_factor_bin_adjusted"].dropna().unique())
    for candidate_bin in bins[1:]:
        d = min_stbl_lvl_df[
            min_stbl_lvl_df["load_factor_bin_adjusted"] == candidate_bin
        ].copy()
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


# estimate min up and down times by finding the smallest bins operating a) at or above min stbl level and b) at 0
def min_up_down_times_ADJUSTED(hourly_plant_unit_df, min_stbl_lvl):
    up_down_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()

    up_df = up_down_df[up_down_df["load_factor_bin_adjusted"] >= min_stbl_lvl].copy()
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
    min_up_run_id = up_run_sizes.idxmin()
    min_up_datetime_utc = (
        up_df.groupby(up_run_id)["operating_datetime_utc"].min().loc[min_up_run_id]
    )

    down_df = up_down_df[up_down_df["load_factor_adjusted_CEMS"].isna()].copy()
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
    min_down_run_id = down_run_sizes.idxmin()
    min_down_datetime_utc = (
        down_df.groupby(down_run_id)["operating_datetime_utc"]
        .min()
        .loc[min_down_run_id]
    )

    return min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc


# estimate heat rates at min stable level and at max load_factor_bin
def heat_rate_ADJUSTED(hourly_plant_unit_df, min_stbl_lvl):
    heat_rates_df = hourly_plant_unit_df.copy()
    heat_rates_df = heat_rates_df.dropna(
        subset=["load_factor_adjusted_CEMS", "heat_rate_net_gen_CEMS"]
    )

    max_lf_bin = heat_rates_df["load_factor_bin_adjusted"].max()
    max_lf_df = heat_rates_df[heat_rates_df["load_factor_bin_adjusted"] == max_lf_bin]
    max_lf_hr = max_lf_df["heat_rate_net_gen_CEMS"].median()

    min_stbl_df = heat_rates_df[
        heat_rates_df["load_factor_bin_adjusted"] == min_stbl_lvl
    ]
    min_stbl_hr = min_stbl_df["heat_rate_net_gen_CEMS"].median()

    return max_lf_hr, min_stbl_hr


# estimate ramp rates by adding a ramp_rate column to the dataframe as the MWh delta/h delta, then group these into 20 bins and set the median of the highest bin to be the ramp_up rate and the median of th elowest to be the ramp down rate
def ramp_rate_ADJUSTED(hourly_plant_unit_df):
    ramp_df = hourly_plant_unit_df.sort_values("operating_datetime_utc").copy()
    ramp_df["time_delta"] = (
        ramp_df["operating_datetime_utc"].diff().dt.total_seconds().div(3600)
    )
    ramp_df["mwh_delta"] = ramp_df["net_generation_mwh_CEMS"].diff()
    ramp_df["ramp_rate"] = ramp_df["mwh_delta"] / ramp_df["time_delta"]
    ramp_df = ramp_df.dropna(subset=["ramp_rate"])

    ramp_df["ramp_rate_bin"] = pd.qcut(ramp_df["ramp_rate"], q=20, duplicates="drop")

    low_bin = ramp_df["ramp_rate_bin"].min()
    high_bin = ramp_df["ramp_rate_bin"].max()
    ramp_down_rate = ramp_df[ramp_df["ramp_rate_bin"] == low_bin]["ramp_rate"].median()
    ramp_up_rate = ramp_df[ramp_df["ramp_rate_bin"] == high_bin]["ramp_rate"].median()

    return ramp_up_rate, ramp_down_rate


def build_op_char_df_ADJUSTED(plant_id, unit_id):
    epa_cems_plant_unit_df = epa_cems_w_net_gen[
        (epa_cems_w_net_gen["plant_id_epa"] == plant_id)
        & (epa_cems_w_net_gen["emissions_unit_id_epa"] == unit_id)
    ]
    # set load factor and heat rate data to NAN if load_factor is less than .02
    epa_cems_plant_unit_df["operating_datetime_utc"] = pd.to_datetime(
        epa_cems_plant_unit_df["operating_datetime_utc"]
    )
    epa_cems_plant_unit_df["time_delta"] = (
        epa_cems_plant_unit_df["operating_datetime_utc"]
        .diff()
        .dt.total_seconds()
        .div(3600)
    )
    valid_load_factors = epa_cems_plant_unit_df["load_factor_adjusted_CEMS"].dropna()
    max_mw = epa_cems_plant_unit_df["max_cap_mw"].max()
    if valid_load_factors.nunique() > 1:
        epa_cems_plant_unit_df["load_factor_bin_adjusted"] = pd.cut(
            epa_cems_plant_unit_df["load_factor_adjusted_CEMS"],
            bins=10,
            right=True,
            include_lowest=False,
        )
        min_stbl_lvl_bin, min_stbl_lvl = min_stable_level_ADJUSTED(
            epa_cems_plant_unit_df, min_stbl_lvl_consecutive_hours
        )
        max_lf_hr, min_stbl_hr = heat_rate_ADJUSTED(
            epa_cems_plant_unit_df, min_stbl_lvl_bin
        )
        min_up_time, min_down_time, min_up_datetime_utc, min_down_datetime_utc = (
            min_up_down_times_ADJUSTED(epa_cems_plant_unit_df, min_stbl_lvl_bin)
        )
        ramp_up_rate, ramp_down_rate = ramp_rate_ADJUSTED(epa_cems_plant_unit_df)
    else:
        epa_cems_plant_unit_df["load_factor_bin_adjusted"] = np.nan
        min_stbl_lvl = np.nan
        max_lf_hr = np.nan
        min_stbl_hr = np.nan
        min_up_time = np.nan
        min_down_time = np.nan
        min_up_datetime_utc = np.nan
        min_down_datetime_utc = np.nan
        ramp_up_rate = np.nan
        ramp_down_rate = np.nan

    plant_unit_op_char_df = pd.DataFrame(
        {
            "plant_id_epa": [plant_id],
            "emissions_unit_id_epa": [unit_id],
            "max_cap_mw": [max_mw],
            "min_stable_level": [min_stbl_lvl],
            "min_up_time_hr": [min_up_time],
            "min_down_time_hr": [min_down_time],
            "heat_rate_at_max_load_factor_mmbtu_per_mwh": [round(max_lf_hr, 2)],
            "heat_rate_at_min_stable_level_mmbtu_per_mwh": [round(min_stbl_hr, 2)],
            "ramp_up_rate_fraction_of_max_cap_mw_per_min": [
                round(ramp_up_rate / max_mw / 60, 2)
            ],
            "ramp_down_rate_fraction_of_max_cap_mw_per_min": [
                round(ramp_down_rate / max_mw / 60, 2)
            ],
        }
    )

    return plant_unit_op_char_df


# %% Build adjusted op char df from epa_cems_w_net_gen
for index, row in epa_plant_unit_pairs.iterrows():
    plant_id = row["plant_id_epa"]
    unit_id = row["emissions_unit_id_epa"]
    plant_op_char_adjusted_df = build_op_char_df_ADJUSTED(plant_id, unit_id)
    epa_op_char_output_df_ADJUSTED = pd.concat(
        [epa_op_char_output_df_ADJUSTED, plant_op_char_adjusted_df]
    )

epa_op_char_output_df_ADJUSTED.to_csv("epa_op_char_output_df_ADJUSTED.csv", index=False)
