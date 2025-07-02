"""Denormalized, aggregated, and filled versions of the basic EIA-923 tables."""

from typing import Literal

import numpy as np
import pandas as pd
from dagster import AssetsDefinition, Field, asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)

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
            right=pu.drop(columns=["data_maturity"]),
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
    out_df.loc[:, "fuel_cost_per_mmbtu"] = out_df.loc[:, "fuel_cost_per_mmbtu"].fillna(
        out_df["bulk_agg_fuel_cost_per_mmbtu"]
    )

    return out_df


def drop_ytd_for_annual_tables(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Drop records in annual tables where data_maturity is incremental_ytd.

    This avoids accidental aggregation errors due to sub-annually reported data.

    Args:
        df: A pd.DataFrame that contains a data_maturity column and for
            which you want to drop values where data_maturity = incremental_ytd.
        freq: either MS or YS to indicate the level of aggretation for a specific table.

    Returns:
        pd.DataFrame: The same input pd.DataFrames but without any rows where
            data_maturity = incremental_ytd.
    """
    if freq == "YS":
        logger.info(
            "Removing rows where data_maturity is incremental_ytd to avoid "
            "aggregation errors."
        )
        df = df.loc[df["data_maturity"] != "incremental_ytd"].copy()
    return df


#####################################################################################
# Simple Denormalized Assets
#####################################################################################
@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_eia923__generation(
    core_eia923__monthly_generation: pd.DataFrame,
    _out_eia__plants_utilities: pd.DataFrame,
    core_eia860__assn_boiler_generator: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`core_eia923__monthly_generation` table."""
    return denorm_by_gen(
        core_eia923__monthly_generation,
        pu=_out_eia__plants_utilities,
        bga=core_eia860__assn_boiler_generator,
    )


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_eia923__generation_fuel_combined(
    core_eia923__monthly_generation_fuel: pd.DataFrame,
    core_eia923__monthly_generation_fuel_nuclear: pd.DataFrame,
    _out_eia__plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the `generation_fuel_combined_eia923` table.

    This asset first combines the :ref:`core_eia923__monthly_generation_fuel` and
    :ref:`core_eia923__monthly_generation_fuel_nuclear` into a single table with a uniform primary
    key (consolidating multiple nuclear unit IDs into a single plant record) and then
    denormalizes it by merging in some addition plant and utility level columns.

    This table contains the records at their originally reported temporal resolution,
    so it's outside of :func:`time_aggregated_eia923_asset_factory`.
    """
    primary_key = [
        "report_date",
        "plant_id_eia",
        "prime_mover_code",
        "energy_source_code",
    ]
    sum_cols = [
        "fuel_consumed_for_electricity_mmbtu",
        "fuel_consumed_for_electricity_units",
        "fuel_consumed_mmbtu",
        "fuel_consumed_units",
        "net_generation_mwh",
    ]
    other_cols = [
        "nuclear_unit_id",  # dropped in the groupby / aggregation.
        "fuel_mmbtu_per_unit",  # recalculated based on aggregated sum_cols.
    ]
    # Rather than enumerating all of the non-data columns, identify them by process of
    # elimination, in case they change in the future.
    non_data_cols = list(
        set(core_eia923__monthly_generation_fuel_nuclear.columns)
        - set(primary_key + sum_cols + other_cols)
    )

    gfn_gb = core_eia923__monthly_generation_fuel_nuclear.groupby(primary_key)
    # Ensure that all non-data columns are homogeneous within groups
    if gfn_gb[non_data_cols].nunique().ne(1).any(axis=None):
        raise ValueError(
            "Found inhomogeneous non-data cols while aggregating nuclear generation. "
            f"Non-data cols: {non_data_cols}"
        )
    gfn_agg = pd.concat(
        [
            gfn_gb[non_data_cols].first(),
            gfn_gb[sum_cols].sum(min_count=1),
        ],
        axis="columns",
    )
    # Nuclear plants don't report units of fuel consumed, so fuel heat content ends up
    # being calculated as infinite. However, some nuclear plants report using small
    # amounts of DFO. Ensure infinite heat contents are set to NA instead:
    gfn_agg = gfn_agg.assign(
        fuel_mmbtu_per_unit=np.where(
            gfn_agg.fuel_consumed_units != 0,
            gfn_agg.fuel_consumed_mmbtu / gfn_agg.fuel_consumed_units,
            np.nan,
        )
    ).reset_index()
    gf = (
        pd.concat([gfn_agg, core_eia923__monthly_generation_fuel])
        .sort_values(primary_key)
        .reset_index(drop=True)
    )
    return denorm_by_plant(gf, pu=_out_eia__plants_utilities)


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_eia923__boiler_fuel(
    core_eia923__monthly_boiler_fuel: pd.DataFrame,
    _out_eia__plants_utilities: pd.DataFrame,
    core_eia860__assn_boiler_generator: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`core_eia923__monthly_boiler_fuel` table.

    The total heat content is also calculated as it's useful in its own right and
    required later to calculate average heat content per unit of fuel.
    """
    core_eia923__monthly_boiler_fuel["fuel_consumed_mmbtu"] = (
        core_eia923__monthly_boiler_fuel["fuel_consumed_units"]
        * core_eia923__monthly_boiler_fuel["fuel_mmbtu_per_unit"]
    )
    return denorm_by_boil(
        core_eia923__monthly_boiler_fuel,
        pu=_out_eia__plants_utilities,
        bga=core_eia860__assn_boiler_generator,
    )


@asset(
    io_manager_key="pudl_io_manager",
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
def out_eia923__fuel_receipts_costs(
    context,
    core_eia923__monthly_fuel_receipts_costs: pd.DataFrame,
    core_eia923__entity_coalmine: pd.DataFrame,
    _out_eia__plants_utilities: pd.DataFrame,
    _out_eia__monthly_state_fuel_prices: pd.DataFrame,
    core_eia__entity_plants: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalize the :ref:`core_eia923__monthly_fuel_receipts_costs` table."""
    core_eia923__entity_coalmine = core_eia923__entity_coalmine.drop(
        columns=["data_maturity"]
    )
    plant_states = core_eia__entity_plants[["plant_id_eia", "state"]]
    frc_df = (
        pd.merge(
            core_eia923__monthly_fuel_receipts_costs,
            core_eia923__entity_coalmine.rename(
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
    )

    def _add_fuel_cost_per_mmbtu_source_col(
        frc_df: pd.DataFrame, source: Literal["original", "eiaapi", "rolling_avg"]
    ):
        """Add a source column indicator.

        Assumes all non-null fuel costs that are not labeled with a source comes from
        the input source. Apply to the original source before any imputations have been
        applied and apply this directly after a new source of fuel cost has been added.
        """
        if "fuel_cost_per_mmbtu_source" not in frc_df:
            frc_df["fuel_cost_per_mmbtu_source"] = pd.NA
        frc_df.loc[
            frc_df["fuel_cost_per_mmbtu_source"].isnull()
            & frc_df["fuel_cost_per_mmbtu"].notnull(),
            "fuel_cost_per_mmbtu_source",
        ] = source
        return frc_df

    frc_df = _add_fuel_cost_per_mmbtu_source_col(frc_df, "original")
    if context.op_config["fill"]:
        logger.info("filling in fuel cost NaNs")
        frc_df = _fill_fuel_costs_by_state(
            frc_df, fuel_costs=_out_eia__monthly_state_fuel_prices
        )
        frc_df = _add_fuel_cost_per_mmbtu_source_col(frc_df, "eiaapi")
    # this next step smooths fuel_cost_per_mmbtu as a rolling monthly average.
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
        frc_df = _add_fuel_cost_per_mmbtu_source_col(frc_df, "rolling_avg")
    # Calculate useful frequency-independent totals:
    frc_df["fuel_consumed_mmbtu"] = (
        frc_df["fuel_mmbtu_per_unit"] * frc_df["fuel_received_units"]
    )
    frc_df["total_fuel_cost"] = (
        frc_df["fuel_consumed_mmbtu"] * frc_df["fuel_cost_per_mmbtu"]
    )
    return denorm_by_plant(frc_df, pu=_out_eia__plants_utilities)


#####################################################################################
# Time Aggregated Assets
#####################################################################################
def time_aggregated_eia923_asset_factory(
    freq: Literal["YS", "MS"],
    io_manager_key: str | None = None,
) -> list[AssetsDefinition]:
    """Build EIA-923 asset definitions, aggregated by year or month."""
    agg_freqs = {"YS": "yearly", "MS": "monthly"}

    @asset(
        name=f"out_eia923__{agg_freqs[freq]}_generation",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_agg_eia923(
        out_eia923__generation: pd.DataFrame,
        _out_eia__plants_utilities: pd.DataFrame,
        core_eia860__assn_boiler_generator: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`out_eia923__generation` monthly or annually."""
        return (
            # Create a date index for grouping based on freq
            out_eia923__generation.set_index(
                pd.DatetimeIndex(out_eia923__generation.report_date)
            )
            .pipe(drop_ytd_for_annual_tables, freq)
            .groupby(
                by=["plant_id_eia", "generator_id", pd.Grouper(freq=freq)],
                observed=True,
            )
            .agg({"net_generation_mwh": pudl.helpers.sum_na, "data_maturity": "first"})
            .reset_index()
            .pipe(
                denorm_by_gen,
                pu=_out_eia__plants_utilities,
                bga=core_eia860__assn_boiler_generator,
            )
        )

    @asset(
        name=f"out_eia923__{agg_freqs[freq]}_generation_fuel_combined",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def generation_fuel_combined_agg_eia923(
        out_eia923__generation_fuel_combined: pd.DataFrame,
        _out_eia__plants_utilities: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`generation_fuel_combined_eia923` monthly or annually."""
        gf_both = (
            # Create a date index for temporal resampling:
            out_eia923__generation_fuel_combined.set_index(
                pd.DatetimeIndex(out_eia923__generation_fuel_combined.report_date)
            )
            .pipe(drop_ytd_for_annual_tables, freq)
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
                    "data_maturity": "first",
                }
            )
        ).reset_index()
        # Nuclear plants don't report units of fuel consumed, so fuel heat content ends
        # up being calculated as infinite. However, some nuclear plants report using
        # small amounts of DFO. Ensure infinite heat contents are set to NA instead:
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
            .pipe(denorm_by_plant, pu=_out_eia__plants_utilities)
        )

    @asset(
        name=f"out_eia923__{agg_freqs[freq]}_boiler_fuel",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def boiler_fuel_agg_eia923(
        out_eia923__boiler_fuel: pd.DataFrame,
        _out_eia__plants_utilities: pd.DataFrame,
        core_eia860__assn_boiler_generator: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate :ref:`core_eia923__monthly_boiler_fuel` monthly or annually."""
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        return (
            out_eia923__boiler_fuel.assign(
                total_sulfur_content=lambda x: x.fuel_consumed_units
                * x.sulfur_content_pct,
                total_ash_content=lambda x: x.fuel_consumed_units * x.ash_content_pct,
            )
            .set_index(pd.DatetimeIndex(out_eia923__boiler_fuel.report_date))
            .pipe(drop_ytd_for_annual_tables, freq)
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
                    "data_maturity": "first",
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
                pu=_out_eia__plants_utilities,
                bga=core_eia860__assn_boiler_generator,
            )
        )

    @asset(
        name=f"out_eia923__{agg_freqs[freq]}_fuel_receipts_costs",
        io_manager_key=io_manager_key,
        compute_kind="Python",
    )
    def fuel_receipts_costs_agg_eia923(
        out_eia923__fuel_receipts_costs: pd.DataFrame,
        _out_eia__plants_utilities: pd.DataFrame,
    ) -> pd.DataFrame:
        """Aggregate the :ref:`core_eia923__monthly_fuel_receipts_costs` table monthly or annually."""
        return (
            out_eia923__fuel_receipts_costs.set_index(
                pd.DatetimeIndex(out_eia923__fuel_receipts_costs.report_date)
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
            .pipe(drop_ytd_for_annual_tables, freq)
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
                    "fuel_cost_per_mmbtu_source": pudl.helpers.groupby_agg_label_unique_source_or_mixed,
                    "state": "first",
                    "data_maturity": "first",
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
            .pipe(denorm_by_plant, pu=_out_eia__plants_utilities)
        )

    return [
        boiler_fuel_agg_eia923,
        fuel_receipts_costs_agg_eia923,
        generation_agg_eia923,
        generation_fuel_combined_agg_eia923,
    ]


generation_fuel_agg_eia923_assets = [
    ass
    for freq in ["YS", "MS"]
    for ass in time_aggregated_eia923_asset_factory(
        freq=freq, io_manager_key="pudl_io_manager"
    )
]
