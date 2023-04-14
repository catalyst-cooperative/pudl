"""Denormalized, aggregated, and filled versions of the basic EIA-923 tables."""
from typing import Literal

import numpy as np
import pandas as pd
from dagster import AssetsDefinition, Field, asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)

AGG_FREQS = {
    "AS": "yearly",
    "MS": "monthly",
}

FIRST_COLS = [
    "report_date",
    "plant_id_eia",
    "plant_id_pudl",
    "plant_name_eia",
    "utility_id_eia",
    "utility_id_pudl",
    "utility_name_eia",
]


#####################################################################################
# Helper functions for these assets
#####################################################################################
def denorm_by_plant(
    df: pd.DataFrame, pu: pd.DataFrame, first_cols: list[str] | None = None
) -> pd.DataFrame:
    """Denormalize a table that is reported on a per-plant basis."""
    if first_cols is None:
        first_cols = FIRST_COLS
    df = (
        pudl.helpers.date_merge(
            left=df,
            right=pu,
            on=["plant_id_eia"],
            date_on=["year"],
            how="left",
        )
        .dropna(subset=["plant_id_eia", "utility_id_eia"])
        .pipe(pudl.helpers.organize_cols, cols=first_cols)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return df


def denorm_by_gen(
    df: pd.DataFrame,
    pu: pd.DataFrame,
    bga: pd.DataFrame,
    first_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Denormalize a table that is reported on a per-generator basis."""
    if first_cols is None:
        first_cols = FIRST_COLS + ["generator_id", "unit_id_pudl"]
    bga_gens = bga.loc[
        :, ["report_date", "plant_id_eia", "generator_id", "unit_id_pudl"]
    ].drop_duplicates()
    df = denorm_by_plant(df, pu)
    df = (
        pudl.helpers.date_merge(
            left=df,
            right=bga_gens,
            on=["plant_id_eia", "generator_id"],
            date_on=["year"],
            how="left",
        )
        .dropna(subset=["plant_id_eia", "utility_id_eia", "generator_id"])
        .pipe(pudl.helpers.organize_cols, cols=first_cols)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return df


def denorm_by_boil(
    df: pd.DataFrame,
    pu: pd.DataFrame,
    bga: pd.DataFrame,
    first_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Denormalize a table that is reported on a per-boiler basis."""
    if first_cols is None:
        first_cols = FIRST_COLS + ["boiler_id", "unit_id_pudl"]
    bga_boils = bga.loc[
        :, ["report_date", "plant_id_eia", "boiler_id", "unit_id_pudl"]
    ].drop_duplicates()
    df = denorm_by_plant(df, pu)
    df = (
        pudl.helpers.date_merge(
            left=df,
            right=bga_boils,
            on=["plant_id_eia", "boiler_id"],
            date_on=["year"],
            how="left",
        )
        .dropna(subset=["plant_id_eia", "utility_id_eia", "boiler_id"])
        .pipe(pudl.helpers.organize_cols, cols=first_cols)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return df


def _fill_fuel_costs_by_state(
    frc_df: pd.DataFrame,
    fuel_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Fill in missing fuel costs with state-level averages."""
    out_df = pd.merge(
        frc_df,
        fuel_costs,
        on=["report_date", "state", "fuel_type_code_pudl"],
        how="left",
    )

    out_df["fuel_cost_from_eiaapi"] = (
        # add an indicator column to show if a value has been imputed
        out_df["fuel_cost_per_mmbtu"].isnull()
        & out_df["bulk_agg_fuel_cost_per_mmbtu"].notnull()
    )
    out_df.loc[:, "fuel_cost_per_mmbtu"].fillna(
        out_df["bulk_agg_fuel_cost_per_mmbtu"], inplace=True
    )

    return out_df


#####################################################################################
# Simple Denormalized Assets
#####################################################################################
@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_generation_eia923(
    generation_eia923: pd.DataFrame,
    denorm_plants_utilities_eia: pd.DataFrame,
    boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`generation_eia923` table."""
    return denorm_by_gen(
        generation_eia923,
        pu=denorm_plants_utilities_eia,
        bga=boiler_generator_assn_eia860,
    )


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_generation_fuel_combined_eia923(
    generation_fuel_eia923: pd.DataFrame,
    generation_fuel_nuclear_eia923: pd.DataFrame,
    denorm_plants_utilities_eia: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the `generation_fuel_combined_eia923` table.

    This asset first combines the :ref:`generation_fuel_eia923` and
    :ref:`generation_fuel_nuclear_eia923` into a single table with a uniform primary
    key (consolidating multiple nuclear unit IDs into a single plant record) and then
    denormalizes it by merging in some addition plant and utility level columns.

    This table contians the records at their originally reported temporal resolution,
    so it's outside of :func:`time_aggregated_eia923_asset_factory`.
    """
    gf = pudl.output.eia923.generation_fuel_all_eia923(
        gf=generation_fuel_eia923,
        gfn=generation_fuel_nuclear_eia923,
    )
    return denorm_by_plant(gf, pu=denorm_plants_utilities_eia)


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_boiler_fuel_eia923(
    boiler_fuel_eia923: pd.DataFrame,
    denorm_plants_utilities_eia: pd.DataFrame,
    boiler_generator_assn_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`boiler_fuel_eia923` table.

    The total heat content is also calculated as it's useful in its own right and
    required later to calculate average heat content per unit of fuel.
    """
    boiler_fuel_eia923["fuel_consumed_mmbtu"] = (
        boiler_fuel_eia923["fuel_consumed_units"]
        * boiler_fuel_eia923["fuel_mmbtu_per_unit"]
    )
    return denorm_by_boil(
        boiler_fuel_eia923,
        pu=denorm_plants_utilities_eia,
        bga=boiler_generator_assn_eia860,
    )


@asset(
    io_manager_key="pudl_sqlite_io_manager",
    config_schema={
        "fill": Field(
            bool,
            default_value=True,
            description=(
                "If True, attempt to fill missing fuel prices from aggregate EIA data."
            ),
        ),
        "roll": Field(
            bool,
            default_value=True,
            description=("If True, use rolling averages to fill missing fuel prices."),
        ),
    },
    compute_kind="Python",
)
def denorm_fuel_receipts_costs_eia923(
    context,
    fuel_receipts_costs_eia923: pd.DataFrame,
    coalmine_eia923: pd.DataFrame,
    denorm_plants_utilities_eia: pd.DataFrame,
    state_average_fuel_costs_eia: pd.DataFrame,
    plants_entity_eia: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`fuel_receipts_costs_eia923` table."""
    coalmine_eia923 = coalmine_eia923.drop(columns=["data_maturity"])
    plant_states = plants_entity_eia[["plant_id_eia", "state"]]
    frc_df = (
        pd.merge(
            fuel_receipts_costs_eia923,
            coalmine_eia923.rename(
                columns={
                    "state": "mine_state",
                    "county_id_fips": "coalmine_county_id_fips",
                }
            ),
            how="left",
            on="mine_id_pudl",
        )
        .merge(plant_states, how="left", on="plant_id_eia")
        .drop(columns=["mine_id_pudl"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    if context.op_config["fill"]:
        logger.info("filling in fuel cost NaNs")
        frc_df = _fill_fuel_costs_by_state(
            frc_df, fuel_costs=state_average_fuel_costs_eia
        )
    # add the flag column to note that we didn't fill in with API data
    else:
        frc_df = frc_df.assign(fuel_cost_from_eiaapi=False)
    # this next step smoothes fuel_cost_per_mmbtu as a rolling monthly average.
    # for each month where there is any data make weighted averages of each
    # plant/fuel/month.
    if context.op_config["roll"]:
        logger.info("filling in fuel cost NaNs with rolling averages")
        frc_df = pudl.helpers.fillna_w_rolling_avg(
            frc_df,
            group_cols=["plant_id_eia", "energy_source_code"],
            data_col="fuel_cost_per_mmbtu",
            window=12,
            min_periods=6,
            win_type="triang",
        )
    # Calculate useful frequency-independent totals:
    frc_df["fuel_consumed_mmbtu"] = (
        frc_df["fuel_mmbtu_per_unit"] * frc_df["fuel_received_units"]
    )
    frc_df["total_fuel_cost"] = (
        frc_df["fuel_consumed_mmbtu"] * frc_df["fuel_cost_per_mmbtu"]
    )

    return denorm_by_plant(frc_df, pu=denorm_plants_utilities_eia)


#####################################################################################
# Time Aggregated Assets
#####################################################################################
def time_aggregated_eia923_asset_factory(
    freq: Literal["AS", "MS"],
    io_manager_key: str | None = None,
) -> list[AssetsDefinition]:
    """Build EIA-923 asset definitions, aggregated by year or month."""

    @asset(
        name=f"denorm_generation_{AGG_FREQS[freq]}_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_agg_eia923(
        denorm_generation_eia923: pd.DataFrame,
        denorm_plants_utilities_eia: pd.DataFrame,
        boiler_generator_assn_eia860: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`generation_eia923` monthly or annually."""
        return (
            # Create a date index for grouping based on freq
            denorm_generation_eia923.set_index(
                pd.DatetimeIndex(denorm_generation_eia923.report_date)
            )
            .groupby(
                by=["plant_id_eia", "generator_id", pd.Grouper(freq=freq)],
                observed=True,
            )
            .agg({"net_generation_mwh": pudl.helpers.sum_na})
            .reset_index()
            .pipe(
                denorm_by_gen,
                pu=denorm_plants_utilities_eia,
                bga=boiler_generator_assn_eia860,
            )
        )

    @asset(
        name=f"denorm_generation_fuel_combined_{AGG_FREQS[freq]}_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_fuel_combined_agg_eia923(
        denorm_generation_fuel_combined_eia923: pd.DataFrame,
        denorm_plants_utilities_eia: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`generation_fuel_combined_eia923` monthly or annually."""
        gf_both = (
            # Create a date index for temporal resampling:
            denorm_generation_fuel_combined_eia923.set_index(
                pd.DatetimeIndex(denorm_generation_fuel_combined_eia923.report_date)
            )
            .groupby(
                by=[
                    "plant_id_eia",
                    "fuel_type_code_pudl",
                    "energy_source_code",
                    "prime_mover_code",
                    pd.Grouper(freq=freq),
                ],
                observed=True,
            )
            .agg(
                {
                    # Sum up these values so we can calculate quantity weighted averages
                    "fuel_consumed_units": pudl.helpers.sum_na,
                    "fuel_consumed_for_electricity_units": pudl.helpers.sum_na,
                    "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                    "fuel_consumed_for_electricity_mmbtu": pudl.helpers.sum_na,
                    "net_generation_mwh": pudl.helpers.sum_na,
                }
            )
        ).reset_index()
        # Nuclear plants don't report units of fuel consumed, so fuel heat content ends
        # up being calculated as infinite. However, some nuclear plants report using
        # small amounts of DFO. Ensure infinte heat contents are set to NA instead:
        # Duplicates pudl.output.eia923.generation_fuel_all_eia923(). Should figure out
        # how to consolidate.
        gf = gf_both.loc[gf_both.energy_source_code != "NUC"]
        gfn = gf_both.loc[gf_both.energy_source_code == "NUC"]

        gf["fuel_mmbtu_per_unit"] = gf.fuel_consumed_mmbtu / gf.fuel_consumed_units

        gfn.assign(
            fuel_mmbtu_per_unit=np.where(
                gfn.fuel_consumed_units != 0,
                gfn.fuel_consumed_mmbtu / gfn.fuel_consumed_units,
                np.nan,
            )
        )
        return (
            pd.concat([gf, gfn])
            .sort_values(
                [
                    "report_date",
                    "plant_id_eia",
                    "prime_mover_code",
                    "energy_source_code",
                ]
            )
            .reset_index(drop=True)
            .pipe(denorm_by_plant, pu=denorm_plants_utilities_eia)
        )

    @asset(
        name=f"denorm_boiler_fuel_{AGG_FREQS[freq]}_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def boiler_fuel_agg_eia923(
        denorm_boiler_fuel_eia923: pd.DataFrame,
        denorm_plants_utilities_eia: pd.DataFrame,
        boiler_generator_assn_eia860: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`generation_fuel_combined_eia923` monthly or annually."""
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        return (
            denorm_boiler_fuel_eia923.assign(
                total_sulfur_content=lambda x: x.fuel_consumed_units
                * x.sulfur_content_pct,
                total_ash_content=lambda x: x.fuel_consumed_units * x.ash_content_pct,
            )
            .set_index(pd.DatetimeIndex(denorm_boiler_fuel_eia923.report_date))
            .groupby(
                by=[
                    "plant_id_eia",
                    "boiler_id",
                    "energy_source_code",
                    "fuel_type_code_pudl",
                    "prime_mover_code",
                    pd.Grouper(freq=freq),
                ],
                observed=True,
            )
            # Sum up these totals within each group, and recalculate the per-unit
            # values (weighted in this case by fuel_consumed_units)
            .agg(
                {
                    "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                    "fuel_consumed_units": pudl.helpers.sum_na,
                    "total_sulfur_content": pudl.helpers.sum_na,
                    "total_ash_content": pudl.helpers.sum_na,
                }
            )
            .assign(
                fuel_mmbtu_per_unit=lambda x: x.fuel_consumed_mmbtu
                / x.fuel_consumed_units,
                sulfur_content_pct=lambda x: x.total_sulfur_content
                / x.fuel_consumed_units,
                ash_content_pct=lambda x: x.total_ash_content / x.fuel_consumed_units,
            )
            .drop(columns=["total_ash_content", "total_sulfur_content"])
            .reset_index()
            .pipe(
                denorm_by_boil,
                pu=denorm_plants_utilities_eia,
                bga=boiler_generator_assn_eia860,
            )
        )

    @asset(
        name=f"denorm_fuel_receipts_costs_{AGG_FREQS[freq]}_eia923",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def fuel_receipts_costs_agg_eia923(
        denorm_fuel_receipts_costs_eia923: pd.DataFrame,
        denorm_plants_utilities_eia: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate the :ref:`fuel_receipts_costs_eia923` table monthly or annually."""
        return (
            denorm_fuel_receipts_costs_eia923.set_index(
                pd.DatetimeIndex(denorm_fuel_receipts_costs_eia923.report_date)
            )
            .assign(
                total_ash_content=lambda x: x.ash_content_pct * x.fuel_received_units,
                total_sulfur_content=lambda x: x.sulfur_content_pct
                * x.fuel_received_units,
                total_mercury_content=lambda x: x.mercury_content_ppm
                * x.fuel_received_units,
                total_moisture_content=lambda x: x.moisture_content_pct
                * x.fuel_received_units,
                total_chlorine_content=lambda x: x.chlorine_content_ppm
                * x.fuel_received_units,
            )
            .groupby(
                by=["plant_id_eia", "fuel_type_code_pudl", pd.Grouper(freq=freq)],
                observed=True,
            )
            .agg(
                {
                    "fuel_received_units": pudl.helpers.sum_na,
                    "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                    "total_fuel_cost": pudl.helpers.sum_na,
                    "total_sulfur_content": pudl.helpers.sum_na,
                    "total_ash_content": pudl.helpers.sum_na,
                    "total_mercury_content": pudl.helpers.sum_na,
                    "total_moisture_content": pudl.helpers.sum_na,
                    "total_chlorine_content": pudl.helpers.sum_na,
                    "fuel_cost_from_eiaapi": "any",
                    "state": "first",
                }
            )
            .assign(
                fuel_cost_per_mmbtu=lambda x: x.total_fuel_cost / x.fuel_consumed_mmbtu,
                fuel_mmbtu_per_unit=lambda x: x.fuel_consumed_mmbtu
                / x.fuel_received_units,
                ash_content_pct=lambda x: x.total_ash_content / x.fuel_received_units,
                sulfur_content_pct=lambda x: x.total_sulfur_content
                / x.fuel_received_units,
                mercury_content_ppm=lambda x: x.total_mercury_content
                / x.fuel_received_units,
                moisture_content_pct=lambda x: x.total_moisture_content
                / x.fuel_received_units,
                chlorine_content_ppm=lambda x: x.total_chlorine_content
                / x.fuel_received_units,
            )
            .drop(
                columns=[
                    "total_ash_content",
                    "total_sulfur_content",
                    "total_moisture_content",
                    "total_chlorine_content",
                    "total_mercury_content",
                ]
            )
            .reset_index()
            .pipe(denorm_by_plant, pu=denorm_plants_utilities_eia)
        )

    return [
        boiler_fuel_agg_eia923,
        fuel_receipts_costs_agg_eia923,
        generation_agg_eia923,
        generation_fuel_combined_agg_eia923,
    ]


generation_fuel_agg_eia923_assets = [
    ass
    for freq in list(AGG_FREQS)
    for ass in time_aggregated_eia923_asset_factory(
        freq=freq, io_manager_key="pudl_sqlite_io_manager"
    )
]
