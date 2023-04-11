"""Denormalized versions of the EIA 923 tables.

* denorm_generation_eia923
* denorm_boiler_fuel_eia923
* denorm_fuel_receipts_costs_eia923
* denorm_generation_fuel_nuclear_eia923
* denorm_generation_fuel_nonnuclear_eia923
* denorm_generation_fuel_all_eia923

Temporal aggregations should probably use asset factories:
* raw
* annual
* monthly

PudlTabl.freq will need to control which table is pulled from the DB.

Generation Fuel:
* could also use a nuke vs. non-nuke asset factory
* Generation Fuel All is derived from nuclear and non-nuclear (separate asset)

* First make it all work without any aggregation -- just the raw tables.
* Then add in factories to add the temporal aggregations.

* Aggregations can be separated from denormalization.
* Aggregations should only be performed on the normalized tables.
* Denormalization process is identical on raw, monthly, or annual aggregates.
* Aggregation asset factory specific to each output table.
* Hand off aggregated table to denormalization asset / function?
"""
import pandas as pd
from dagster import AssetsDefinition, asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

AGG_FREQS = [None, "AS", "MS"]


def generation_eia923_asset_factory(
    freq: str | None = None,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """Factory for building denormalized, aggregated generation_eia923 assets."""

    @asset(
        name=f"denorm_{freq.lower() + '_' if freq else ''}generation_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_eia923_asset(
        generation_eia923: pd.DataFrame,
        denorm_plants_utilities_eia: pd.DataFrame,
        boiler_generator_assn_eia860: pd.DataFrame,
    ) -> pd.DataFrame:
        """A denormalized, aggregated version of the generation_eia923 table."""
        if freq is not None:
            # Create a date index for grouping based on freq
            dt_idx = pd.DatetimeIndex(generation_eia923.report_date)
            generation_eia923 = generation_eia923.set_index(dt_idx)
            by = ["plant_id_eia", "generator_id"] + [pd.Grouper(freq=freq)]
            gb = generation_eia923.groupby(by=by)
            generation_eia923 = gb.agg(
                {"net_generation_mwh": pudl.helpers.sum_na}
            ).reset_index()

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

    return generation_eia923_asset


generation_assets = [
    generation_eia923_asset_factory(freq=f, io_manager_key=None) for f in AGG_FREQS
]
