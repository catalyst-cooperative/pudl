"""Functions for compiling derived aspects of the EIA 930 data."""

import pandas as pd
from dagster import asset

from pudl.analysis.timeseries_cleaning import (
    ImputeTimeseriesSettings,
    impute_timeseries_asset_factory,
)


def _add_timezone(
    df: pd.DataFrame, core_eia__codes_balancing_authorities: pd.DataFrame
) -> pd.DataFrame:
    other = core_eia__codes_balancing_authorities[["code", "report_timezone"]].rename(
        columns={"code": "balancing_authority_code_eia"}
    )
    return df.merge(other, on=["balancing_authority_code_eia"]).rename(
        columns={"report_timezone": "timezone"}
    )


@asset(
    compute_kind="Python",
    required_resource_keys={"dataset_settings"},
)
def _out_eia930__hourly_operations(
    core_eia930__hourly_operations: pd.DataFrame,
    core_eia__codes_balancing_authorities: pd.DataFrame,
) -> pd.DataFrame:
    """Adds timezone column and combined ID with BA/subregion used for imputation."""
    core_eia930__hourly_operations = _add_timezone(
        core_eia930__hourly_operations,
        core_eia__codes_balancing_authorities,
    )
    # TODO: BA code WAUE does not have listed timezone, so dropping these records for now
    waue_mask = core_eia930__hourly_operations["balancing_authority_code_eia"] == "WAUE"
    assert core_eia930__hourly_operations.loc[waue_mask, "timezone"].isnull().all(), (
        "WAUE not expected to have a timezone"
    )
    core_eia930__hourly_operations = core_eia930__hourly_operations.loc[~waue_mask]
    assert core_eia930__hourly_operations.timezone.notnull().all(), (
        "All records should have a timezone after dropping WAUE"
    )

    return core_eia930__hourly_operations


@asset(
    compute_kind="Python",
    required_resource_keys={"dataset_settings"},
)
def _out_eia930__hourly_subregion_demand(
    core_eia930__hourly_subregion_demand: pd.DataFrame,
    core_eia__codes_balancing_authorities: pd.DataFrame,
) -> pd.DataFrame:
    """Adds timezone column and combined ID with BA/subregion used for imputation."""
    core_eia930__hourly_subregion_demand = _add_timezone(
        core_eia930__hourly_subregion_demand,
        core_eia__codes_balancing_authorities,
    )

    core_eia930__hourly_subregion_demand["combined_subregion_ba_id"] = (
        core_eia930__hourly_subregion_demand["balancing_authority_code_eia"]
        + core_eia930__hourly_subregion_demand["balancing_authority_subregion_code_eia"]
    )
    return core_eia930__hourly_subregion_demand


def _years_from_context(context) -> list[int]:
    return [
        int(half_year[:4])
        for half_year in context.resources.dataset_settings.eia.eia930.half_years
    ]


imputed_subregion_demand_assets = impute_timeseries_asset_factory(
    input_asset_name="_out_eia930__hourly_subregion_demand",
    output_asset_name="out_eia930__hourly_subregion_demand",
    years_from_context=_years_from_context,
    value_col="demand_reported_mwh",
    imputed_value_col="demand_imputed_pudl_mwh",
    id_col="combined_subregion_ba_id",
    settings=ImputeTimeseriesSettings(method_overrides={2019: "tnn", 2025: "tnn"}),
)


imputed_ba_demand_assets = impute_timeseries_asset_factory(
    input_asset_name="_out_eia930__hourly_operations",
    output_asset_name="out_eia930__hourly_operations",
    years_from_context=_years_from_context,
    value_col="demand_reported_mwh",
    imputed_value_col="demand_imputed_pudl_mwh",
    id_col="balancing_authority_code_eia",
)


@asset(io_manager_key="parquet_io_manager")
def out_eia930__hourly_aggregated_demand(
    out_eia930__hourly_operations: pd.DataFrame,
    core_eia__codes_balancing_authorities: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate imputed demand from the BA level to region, interconnect, and contiguous US."""
    aggregation_levels = {
        "region": "balancing_authority_region_code_eia",
        "interconnect": "interconnect_code_eia",
    }

    # Mege with ``core_eia__codes_balancing_authorities`` to get mapping between
    # Balancing authorities and regions/interconnects
    other = core_eia__codes_balancing_authorities[
        [
            "code",
            "balancing_authority_region_code_eia",
            "interconnect_code_eia",
        ]
    ].rename(columns={"code": "balancing_authority_code_eia"})

    df = out_eia930__hourly_operations.merge(other, on=["balancing_authority_code_eia"])

    # Sum to aggregation levels and rename columns
    aggregated_dfs = []
    for level, column in aggregation_levels.items():
        aggregated_df = (
            df.groupby([column, "datetime_utc"], as_index=False, observed=True)[
                "demand_imputed_pudl_mwh"
            ]
            .sum()
            .rename(columns={column: "aggregation_group"})
        )
        aggregated_df["aggregation_level"] = level
        aggregated_df["aggregation_group"] = aggregated_df[
            "aggregation_group"
        ].str.lower()
        aggregated_dfs.append(aggregated_df)

    conus_df = df.groupby(["datetime_utc"], as_index=False)[
        "demand_imputed_pudl_mwh"
    ].sum()
    conus_df["aggregation_group"] = "conus"
    conus_df["aggregation_level"] = "conus"

    return pd.concat(aggregated_dfs + [conus_df])
