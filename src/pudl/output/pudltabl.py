"""This module provides a class enabling tabular compilations from the PUDL DB.

Many of our potential users are comfortable using spreadsheets, not databases, so we are
creating a collection of tabular outputs that contain the most useful core information
from the PUDL data packages, including additional keys and human readable names for the
objects (utilities, plants, generators) being described in the table.

These tabular outputs can be joined with each other using those keys, and used as a data
source within Microsoft Excel, Access, R Studio, or other data analysis packages that
folks may be familiar with.  They aren't meant to completely replicate all the data and
relationships contained within the full PUDL database, but should serve as a generally
usable set of PUDL data products.

The PudlTabl class can also provide access to complex derived values, like the generator
and plant level marginal cost of electricity (MCOE), which are defined in the analysis
module.

In the long run, this is a probably a kind of prototype for pre-packaged API outputs or
data products that we might want to be able to provide to users a la carte.
"""

from datetime import date, datetime
from functools import partial
from typing import Literal, Self

# Useful high-level external modules.
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import pudl
from pudl.metadata.classes import Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl:
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    def __init__(
        self: Self,
        freq: Literal["YS", "MS", None] = None,
        start_date: str | date | datetime | pd.Timestamp = None,
        end_date: str | date | datetime | pd.Timestamp = None,
        fill_net_gen: bool = False,
    ) -> Self:
        """Initialize the PUDL output object.

        This class provides a thin wrapper around the PUDL Parquet outputs, reproducing
        the behavior of the old PudlTabl output caching object.

        Args:
            freq: A string indicating the time frequency at which to aggregate reported
                data. ``MS`` is monthly and ``YS`` is yearly. If None, the data will not
                be aggregated.

            start_date: Beginning date for data to pull from the PUDL DB. If a string,
                it should use the ISO 8601 ``YYYY-MM-DD`` format.
            end_date: End date for data to pull from the PUDL DB. If a string, it should
                use the ISO 8601 ``YYYY-MM-DD`` format.
            fill_net_gen: if True, use the net generation from the
                core_eia923__monthly_generation_fuel - which is reported at the
                plant/fuel/prime mover level and re-allocated to generators in
                ``mcoe()``, ``capacity_factor()`` and ``heat_rate_by_unit()``.
        """
        logger.warning(
            "PudlTabl is deprecated and will be removed in mid-2025. Please read data"
            "directly from the PUDL SQLite database or access the PUDL parquet files in the PUDL outputs."
        )
        if freq not in (None, "YS", "MS"):
            raise ValueError(
                f"freq must be one of None, 'MS', or 'YS', but we got {freq}."
            )
        self.freq: Literal["YS", "MS", None] = freq
        self.fill_net_gen: bool = fill_net_gen

        # grab full working date range if start or end date are not set
        self.start_date = (
            min(pudl.helpers.get_working_dates_by_datasource("ferc1"))
            if start_date is None
            else pd.to_datetime(start_date)
        )
        self.end_date = (
            max(pudl.helpers.get_working_dates_by_datasource("eia"))
            if end_date is None
            else pd.to_datetime(end_date)
        )

        self._register_output_methods()

    def _register_output_methods(self: Self):
        """Load output assets and register a class method for retrieving each one."""
        # Map table name to PudlTabl method.
        # PudlTabl will generate a method to read each table from the DB with the given method name
        table_method_map_any_freq = {  # table_name: method_name
            # denorm_ferc1
            "out_ferc1__yearly_balance_sheet_assets_sched110": "denorm_balance_sheet_assets_ferc1",
            "out_ferc1__yearly_balance_sheet_liabilities_sched110": "denorm_balance_sheet_liabilities_ferc1",
            "out_ferc1__yearly_cash_flows_sched120": "denorm_cash_flow_ferc1",
            "out_ferc1__yearly_depreciation_summary_sched336": "denorm_depreciation_amortization_summary_ferc1",
            "out_ferc1__yearly_energy_dispositions_sched401": "denorm_electric_energy_dispositions_ferc1",
            "out_ferc1__yearly_energy_sources_sched401": "denorm_electric_energy_sources_ferc1",
            "out_ferc1__yearly_operating_expenses_sched320": "denorm_electric_operating_expenses_ferc1",
            "out_ferc1__yearly_operating_revenues_sched300": "denorm_electric_operating_revenues_ferc1",
            "out_ferc1__yearly_depreciation_changes_sched219": "denorm_electric_plant_depreciation_changes_ferc1",
            "out_ferc1__yearly_depreciation_by_function_sched219": "denorm_electric_plant_depreciation_functional_ferc1",
            "out_ferc1__yearly_sales_by_rate_schedules_sched304": "denorm_electricity_sales_by_rate_schedule_ferc1",
            "out_ferc1__yearly_income_statements_sched114": "denorm_income_statement_ferc1",
            "out_ferc1__yearly_other_regulatory_liabilities_sched278": "denorm_other_regulatory_liabilities_ferc1",
            "out_ferc1__yearly_retained_earnings_sched118": "denorm_retained_earnings_ferc1",
            "out_ferc1__yearly_transmission_lines_sched422": "denorm_transmission_statistics_ferc1",
            "out_ferc1__yearly_utility_plant_summary_sched200": "denorm_utility_plant_summary_ferc1",
            "_out_ferc1__yearly_plants_utilities": "pu_ferc1",
            "out_ferc1__yearly_steam_plants_sched402": "plants_steam_ferc1",
            "out_ferc1__yearly_steam_plants_fuel_sched402": "fuel_ferc1",
            "out_ferc1__yearly_steam_plants_fuel_by_plant_sched402": "fbp_ferc1",
            "out_ferc1__yearly_small_plants_sched410": "plants_small_ferc1",
            "out_ferc1__yearly_hydroelectric_plants_sched406": "plants_hydro_ferc1",
            "out_ferc1__yearly_pumped_storage_plants_sched408": "plants_pumped_storage_ferc1",
            "out_ferc1__yearly_purchased_power_and_exchanges_sched326": "purchased_power_ferc1",
            "out_ferc1__yearly_plant_in_service_sched204": "plant_in_service_ferc1",
            "out_ferc1__yearly_all_plants": "plants_all_ferc1",
            # denorm_eia (data comes from multiple EIA forms)
            "out_eia__yearly_plants": "plants_eia860",
            "out_eia__yearly_utilities": "utils_eia860",
            "out_eia__yearly_boilers": "boil_eia860",
            "_out_eia__yearly_generators": "gens_eia860",
            "_out_eia__plants_utilities": "pu_eia860",
            # eia860 (denormalized, data primarily from EIA-860)
            "out_eia860__yearly_ownership": "own_eia860",
            "core_eia860__assn_boiler_generator": "bga_eia860",
            "out_eia860__yearly_emissions_control_equipment": "denorm_emissions_control_equipment_eia860",
            "core_eia860__assn_yearly_boiler_emissions_control_equipment": "boiler_emissions_control_equipment_assn_eia860",
            "core_eia860__scd_emissions_control_equipment": "emissions_control_equipment_eia860",
            "core_eia860__assn_boiler_stack_flue": "boiler_stack_flue_assn_eia860",
            "core_eia860__assn_boiler_cooling": "boiler_cooling_assn_eia860",
            # eia861 (clean)
            "core_eia861__yearly_service_territory": "service_territory_eia861",
            "core_eia861__yearly_sales": "sales_eia861",
            "core_eia861__yearly_short_form": "short_form_eia861",
            "core_eia861__yearly_advanced_metering_infrastructure": "advanced_metering_infrastructure_eia861",
            "core_eia861__yearly_demand_response": "demand_response_eia861",
            "core_eia861__yearly_demand_response_water_heater": "demand_response_water_heater_eia861",
            "core_eia861__yearly_demand_side_management_sales": "demand_side_management_sales_eia861",
            "core_eia861__yearly_demand_side_management_ee_dr": "demand_side_management_ee_dr_eia861",
            "core_eia861__yearly_demand_side_management_misc": "demand_side_management_misc_eia861",
            "core_eia861__yearly_distributed_generation_tech": "distributed_generation_tech_eia861",
            "core_eia861__yearly_distributed_generation_fuel": "distributed_generation_fuel_eia861",
            "core_eia861__yearly_distributed_generation_misc": "distributed_generation_misc_eia861",
            "core_eia861__yearly_distribution_systems": "distribution_systems_eia861",
            "core_eia861__yearly_dynamic_pricing": "dynamic_pricing_eia861",
            "core_eia861__yearly_energy_efficiency": "energy_efficiency_eia861",
            "core_eia861__yearly_green_pricing": "green_pricing_eia861",
            "core_eia861__yearly_mergers": "mergers_eia861",
            "core_eia861__yearly_net_metering_customer_fuel_class": "net_metering_customer_fuel_class_eia861",
            "core_eia861__yearly_net_metering_misc": "net_metering_misc_eia861",
            "core_eia861__yearly_non_net_metering_customer_fuel_class": "non_net_metering_customer_fuel_class_eia861",
            "core_eia861__yearly_non_net_metering_misc": "non_net_metering_misc_eia861",
            "core_eia861__yearly_operational_data_revenue": "operational_data_revenue_eia861",
            "core_eia861__yearly_operational_data_misc": "operational_data_misc_eia861",
            "core_eia861__yearly_reliability": "reliability_eia861",
            "core_eia861__yearly_utility_data_nerc": "utility_data_nerc_eia861",
            "core_eia861__yearly_utility_data_rto": "utility_data_rto_eia861",
            "core_eia861__yearly_utility_data_misc": "utility_data_misc_eia861",
            "core_eia861__assn_utility": "core_eia861__assn_utility",
            "core_eia861__yearly_balancing_authority": "balancing_authority_eia861",
            "core_eia861__assn_balancing_authority": "balancing_authority_assn_eia861",
            # eia923 (denormalized, data primarily from EIA-923)
            "out_eia923__AGG_boiler_fuel": "bf_eia923",
            "out_eia923__AGG_fuel_receipts_costs": "frc_eia923",
            "out_eia923__AGG_generation": "gen_original_eia923",
            "out_eia923__AGG_generation_fuel_combined": "gf_eia923",
            # EPA CAMD to EIA crosswalk
            "core_epa__assn_eia_epacamd": "epacamd_eia",
            # ferc714
            "core_ferc714__respondent_id": "respondent_id_ferc714",
            "out_ferc714__respondents_with_fips": "fipsified_respondents_ferc714",
            "out_ferc714__summarized_demand": "summarized_demand_ferc714",
            # service territory
            "out_eia861__yearly_balancing_authority_service_territory": "compiled_geometry_balancing_authority_eia861",
            "out_eia861__yearly_utility_service_territory": "compiled_geometry_utility_eia861",
            # plant parts
            "out_eia__yearly_generators_by_ownership": "gens_mega_eia",
            "out_eia__yearly_plant_parts": "plant_parts_eia",
            "out_pudl__yearly_assn_eia_ferc1_plant_parts": "ferc1_eia",
        }

        table_method_map_any_agg = {
            "out_eia923__AGG_generation_fuel_by_generator_energy_source": "gen_fuel_by_generator_energy_source_eia923",
            "out_eia923__AGG_generation_fuel_by_generator": "gen_fuel_by_generator_eia923",
            "_out_eia__AGG_heat_rate_by_unit": "hr_by_unit",
            "_out_eia__AGG_heat_rate_by_generator": "hr_by_gen",
            "_out_eia__AGG_capacity_factor_by_generator": "capacity_factor",
            "_out_eia__AGG_fuel_cost_by_generator": "fuel_cost",
            "_out_eia__AGG_derived_generator_attributes": "mcoe",
            "out_eia__AGG_generators": "mcoe_generators",
        }

        table_method_map_yearly_only = {
            "out_eia923__yearly_generation_fuel_by_generator_energy_source_owner": "gen_fuel_by_generator_energy_source_owner_eia923",
        }

        for _table_name, method_name in (
            table_method_map_any_freq
            | table_method_map_any_agg
            | table_method_map_yearly_only
        ).items():
            if hasattr(PudlTabl, method_name):
                logger.warning(
                    f"Automatically generated PudlTabl method {method_name} overrides "
                    "explicitly defined class method. One of these should be deleted."
                )

        for table_name, method_name in table_method_map_any_freq.items():
            # Create method called table_name that will read the asset from DB
            self.__dict__[method_name] = partial(
                self.get_table,
                table_name=table_name,
                allowed_freqs=[None, "YS", "MS"],
            )

        for table_name, method_name in table_method_map_any_agg.items():
            self.__dict__[method_name] = partial(
                self.get_table,
                table_name=table_name,
                allowed_freqs=["YS", "MS"],
            )

        for table_name, method_name in table_method_map_yearly_only.items():
            self.__dict__[method_name] = partial(
                self.get_table,
                table_name=table_name,
                allowed_freqs=["YS"],
            )

    def get_table(
        self: Self,
        table_name: str,
        allowed_freqs: list[str | None] = [None, "YS", "MS"],
    ) -> pd.DataFrame:
        """Grab an output table from PUDL parquet files.

        Args:
            table_name: Name of table to get.
            allowed_freqs: List of allowed aggregation frequencies for table.
        """
        if self.freq not in allowed_freqs:
            raise ValueError(
                f"{table_name} needs one of these frequencies {allowed_freqs}, "
                f"but got {self.freq}"
            )
        table_name = self._agg_table_name(table_name)
        logger.warning(
            "PudlTabl is deprecated and will be removed in mid-2025. To access the "
            f"data returned by this method, read from the {table_name} table in the "
            f"pudl.sqlite database or access the {table_name}.parquet file in the PUDL "
            "outputs."
        )
        res = Resource.from_id(table_name)
        pyarrow_schema: pa.Schema = res.to_pyarrow()

        df = pq.read_table(
            source=PudlPaths().parquet_path(table_name),
            schema=pyarrow_schema,
            use_threads=True,  # Enable multi-threading for faster reads
            memory_map=True,  # Use memory mapping for better memory efficiency
            filters=self._build_parquet_date_filters(pyarrow_schema=pyarrow_schema),
        ).to_pandas()
        # Enforce the expected PUDL dtypes and other constraints on the DataFrame:
        return res.enforce_schema(df)

    def _build_parquet_date_filters(
        self: Self, pyarrow_schema: pa.Schema
    ) -> list[tuple[str, str, date | int]] | None:
        """Build filters to read records from parquet between start and end dates."""
        # Determine date column and value converter
        if "report_date" in pyarrow_schema.names:
            date_col = "report_date"

            def convert_date(d):
                return pd.to_datetime(d).date()
        elif "report_year" in pyarrow_schema.names:
            date_col = "report_year"

            def convert_date(d):
                return pd.to_datetime(d).year
        else:
            return None

        # Build filters
        filters = []
        if self.start_date is not None:
            filters.append((date_col, ">=", convert_date(self.start_date)))
        if self.end_date is not None:
            filters.append((date_col, "<=", convert_date(self.end_date)))

        return filters if filters else None

    def _agg_table_name(self: Self, table_name: str) -> str:
        """Substitute appropriate frequency in aggregated table names.

        If the table isn't aggregated, return the original name.
        """
        agg_freqs = {
            "YS": "yearly",
            "MS": "monthly",
        }
        if "_AGG" in table_name:
            if self.freq is not None:
                table_name = table_name.replace("AGG", agg_freqs[self.freq])
            else:
                table_name = table_name.replace("_AGG", "")
        return table_name

    ###########################################################################
    # Tables requiring special treatment:
    ###########################################################################
    def gen_eia923(self: Self) -> pd.DataFrame:
        """Pull EIA 923 net generation data by generator.

        Net generation is reported in two seperate tables in EIA 923: in the
        core_eia923__monthly_generation and core_eia923__monthly_generation_fuel tables.
        While the core_eia923__monthly_generation_fuel table is more complete (the
        core_eia923__monthly_generation table includes only ~55% of the reported MWhs),
        the core_eia923__monthly_generation table is more granular (it is reported at
        the generator level).

        This method either grabs the core_eia923__monthly_generation table that is
        reported by generator, or allocates net generation from the
        core_eia923__monthly_generation_fuel table to the generator level.

        Returns:
            A denormalized generation table for interactive use.
        """
        if self.fill_net_gen:
            if self.freq not in ["YS", "MS"]:
                raise ValueError(
                    "Allocated net generation requires frequency of `YS` or `MS`, "
                    f"got {self.freq}"
                )
            table_name = self._agg_table_name(
                "out_eia923__AGG_generation_fuel_by_generator"
            )
            gen_df = self.get_table(table_name)
            resource = Resource.from_id(table_name)
            gen_df = gen_df.loc[:, resource.get_field_names()]
        else:
            table_name = self._agg_table_name("out_eia923__AGG_generation")
            gen_df = self.get_table(table_name)
        return gen_df
