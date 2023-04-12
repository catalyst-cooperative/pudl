"""Denormalized, aggregated, and filled versions of the EIA 923 tables.

Basic Denormalized EIA-923 assets
* denorm_generation_eia923 (temporal aggregation)
* denorm_fuel_receipts_costs_eia923 (temporal aggregation + coalmine)
* denorm_boiler_fuel_eia923 (temporal aggregation)
* denorm_generation_fuel_eia923 (temporal aggregation + nuke vs. non-nuke)

More complex denormalized EIA-923 assets:
* denorm_generation_fuel_all_eia923 (combined nuke and non-nuke assets)


Generation Fuel:
* could also use a nuke vs. non-nuke asset factory
* Generation Fuel All is derived from nuclear and non-nuclear (separate asset)

Generation:
* Needs to be able to use allocated net generation from generation fuel table. Should
  this be the default output?

* Aggregations can be separated from denormalization.
* Aggregations should only be performed on the normalized tables.
* Denormalization process is identical on raw, monthly, or annual aggregates.
* Aggregation asset factory specific to each output table.
* Hand off aggregated table to denormalization asset / function?

PudlTabl integration considerations:
* PudlTabl.freq will control what table is pulled from the DB.
"""
from typing import Literal

import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, asset, graph_multi_asset, op

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

AGG_FREQS = {
    "AS": "yearly",
    "MS": "monthly",
}


def generation_eia923_aggregated_asset_factory(
    freq: Literal["AS", "MS"],
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """A factory for building aggregated generation_eia923 assets.

    * Can this be generalized to aggregate many different tables?
    * Seems like the only table-specific parts are the aggregation function and the
      groupby columns.
    * Maybe it can take a list of groupby columns and a dictionary of aggregation
      functions as arguments?
    """

    @asset(
        name=f"generation_{AGG_FREQS[freq]}_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_eia923_asset(generation_eia923: pd.DataFrame) -> pd.DataFrame:
        f"""A version of the generation_eia923 table aggregated {AGG_FREQS[freq]}."""
        if freq is not None:
            # Create a date index for grouping based on freq
            dt_idx = pd.DatetimeIndex(generation_eia923.report_date)
            generation_eia923 = generation_eia923.set_index(dt_idx)
            by = ["plant_id_eia", "generator_id"] + [pd.Grouper(freq=freq)]
            gb = generation_eia923.groupby(by=by)
            generation_eia923 = gb.agg(
                {"net_generation_mwh": pudl.helpers.sum_na}
            ).reset_index()
        return generation_eia923

    return generation_eia923_asset


generation_assets = [
    generation_eia923_aggregated_asset_factory(freq=f, io_manager_key=None)
    for f in AGG_FREQS
]


def generation_fuel_eia923_aggregated_asset_factory(
    freq: Literal["AS", "MS"],
    nuclear: bool,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """A factory for aggregated nuclear and non-nuclear generation fuel assets."""
    table_root = "generation_fuel_nuclear" if nuclear else "generation_fuel"
    input_asset = f"{table_root}_eia923"
    output_asset = f"{table_root}_{AGG_FREQS[freq]}_eia923"

    @asset(
        ins={input_asset: AssetIn()},
        name=output_asset,
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def gen_fuel(**ins) -> pd.DataFrame:
        gf_df = ins[input_asset]
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = [
            "plant_id_eia",
            "fuel_type_code_pudl",
            "energy_source_code",
            "prime_mover_code",
        ]
        if nuclear:
            by = by + ["nuclear_unit_id"]
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by, observed=True)
        gf_df = gf_gb.agg(
            {
                "fuel_consumed_units": pudl.helpers.sum_na,
                "fuel_consumed_for_electricity_units": pudl.helpers.sum_na,
                "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                "fuel_consumed_for_electricity_mmbtu": pudl.helpers.sum_na,
                "net_generation_mwh": pudl.helpers.sum_na,
            }
        )
        gf_df["fuel_mmbtu_per_unit"] = (
            gf_df["fuel_consumed_mmbtu"] / gf_df["fuel_consumed_units"]
        )

        gf_df = gf_df.reset_index()
        return gf_df

    return gen_fuel


generation_fuel_assets = [
    generation_fuel_eia923_aggregated_asset_factory(
        freq=freq, nuclear=nuke, io_manager_key=None
    )
    for freq in AGG_FREQS
    for nuke in [True, False]
]


def generation_fuel_all_eia923_aggregated_asset_factory(
    freq: Literal["AS", "MS"] | None,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """Factory for aggregated combined nuclear & non-nulcear generation fuel assets."""
    freq_str = AGG_FREQS[freq] + "_" if freq is not None else ""

    @asset(
        ins={
            f"generation_fuel_{freq_str}eia923": AssetIn(),
            f"generation_fuel_nuclear_{freq_str}eia923": AssetIn(),
        },
        name=f"generation_fuel_all_{freq_str}eia923",
    )
    def gen_fuel_all(**ins) -> pd.DataFrame:
        return pudl.output.eia923.generation_fuel_all_eia923(
            gf=ins[f"generation_fuel_{freq_str}eia923"],
            gfn=ins[f"generation_fuel_nuclear_{freq_str}eia923"],
        )

    return gen_fuel_all


generation_fuel_all_assets = [
    generation_fuel_all_eia923_aggregated_asset_factory(freq=freq, io_manager_key=None)
    for freq in list(AGG_FREQS) + [None]
]


################################################################################
# Denormalization down here. Not dealing with it for now.
################################################################################
def denorm_generation_eia923(
    generation_eia923: pd.DataFrame,
    denorm_plants_utilities_eia: pd.DataFrame,
    boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """A function that denormalized the generation_eia923 table.

    * I think this could be an asset factory or a multi-asset.
    * Can this be easily generalized to denormalize lots of tables?
    """
    # Merge annual plant/utility data in with the more granular dataframe
    gen = pudl.helpers.date_merge(
        left=generation_eia923,
        right=denorm_plants_utilities_eia,
        on=["plant_id_eia"],
        date_on=["year"],
        how="left",
    ).dropna(subset=["plant_id_eia", "utility_id_eia", "generator_id"])

    # Pull the BGA table and make it unit-generator only:
    bga_gens = boiler_generator_assn_eia860.loc[
        :, ["report_date", "plant_id_eia", "generator_id", "unit_id_pudl"]
    ].drop_duplicates()

    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    gen = pudl.helpers.date_merge(
        left=gen,
        right=bga_gens,
        on=["plant_id_eia", "generator_id"],
        date_on=["year"],
        how="left",
    )
    gen = gen.pipe(
        pudl.helpers.organize_cols,
        cols=[
            "report_date",
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "utility_id_eia",
            "utility_id_pudl",
            "utility_name_eia",
            "generator_id",
        ],
    ).pipe(apply_pudl_dtypes, group="eia")
    return gen
