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

from collections import defaultdict
from datetime import date, datetime
from functools import partial
from typing import Any, Literal

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.metadata.classes import Resource
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)


###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl:
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    def __init__(
        self,
        pudl_engine: sa.engine.Engine,
        freq: Literal["AS", "MS", None] = None,
        start_date: str | date | datetime | pd.Timestamp = None,
        end_date: str | date | datetime | pd.Timestamp = None,
        fill_fuel_cost: bool = False,
        roll_fuel_cost: bool = False,
        fill_net_gen: bool = False,
        fill_tech_desc: bool = True,
        unit_ids: bool = False,
    ):
        """Initialize the PUDL output object.

        Private data members are not initialized until they are requested.  They are
        then cached within the object unless they get re-initialized via a method that
        includes update=True.

        Some methods (e.g mcoe) will take a while to run, since they need to pull
        substantial data and do a bunch of calculations.

        Args:
            pudl_engine: A connection engine for the PUDL DB.
            freq: A string indicating the time frequency at which to aggregate
                reported data. ``MS`` is monththly and ``AS`` is annually. If
                None, the data will not be aggregated.
            start_date: Beginning date for data to pull from the PUDL DB. If
                a string, it should use the ISO 8601 ``YYYY-MM-DD`` format.
            end_date: End date for data to pull from the PUDL DB. If a string,
                it should use the ISO 8601 ``YYYY-MM-DD`` format.
            fill_fuel_cost: if True, fill in missing ``frc_eia923()`` fuel cost
                data with state-fuel averages from EIA's bulk electricity data.
            roll_fuel_cost: if True, apply a rolling average to a subset of
                output table's columns (currently only ``fuel_cost_per_mmbtu``
                for the ``fuel_receipts_costs_eia923`` table.)
            fill_net_gen: if True, use the net generation from the
                generation_fuel_eia923 - which is reported at the
                plant/fuel/prime mover level and  re-allocated to generators in
                ``mcoe()``, ``capacity_factor()`` and ``heat_rate_by_unit()``.
            fill_tech_desc: If True, fill the technology_description
                field to years earlier than 2013 based on plant and
                energy_source_code_1 and fill in technologies with only one matching
                code.
            unit_ids: If True, use several heuristics to assign
                individual generators to functional units. EXPERIMENTAL.
        """
        if not isinstance(pudl_engine, sa.engine.base.Engine):
            raise TypeError(
                "PudlTabl needs pudl_engine to be a SQLAlchemy Engine, but we "
                f"got a {type(pudl_engine)}."
            )
        self.pudl_engine: sa.engine.Engine = pudl_engine

        if freq not in (None, "AS", "MS"):
            raise ValueError(
                f"freq must be one of None, 'MS', or 'AS', but we got {freq}."
            )
        self.freq: Literal["AS", "MS", None] = freq

        # grab all working eia dates to use to set start and end dates if they
        # are not set
        if start_date is None:
            self.start_date = min(pudl.helpers.get_working_dates_by_datasource("ferc1"))
        else:
            # Make sure it's a date... and not a string.
            self.start_date = pd.to_datetime(start_date)

        if end_date is None:
            self.end_date = max(pudl.helpers.get_working_dates_by_datasource("eia"))
        else:
            # Make sure it's a date... and not a string.
            self.end_date = pd.to_datetime(end_date)

        self.roll_fuel_cost: bool = roll_fuel_cost
        self.fill_fuel_cost: bool = fill_fuel_cost
        self.fill_net_gen: bool = fill_net_gen
        self.fill_tech_desc = fill_tech_desc  # only for eia860 table.
        self.unit_ids = unit_ids

        # Used to persist the output tables. Returns None if they don't exist.
        self._dfs = defaultdict(lambda: None)

        self._register_output_methods()

    def _register_output_methods(self):
        """Load output assets and register a class method for retrieving each one."""
        # Map table name to PudlTabl method.
        # PudlTabl will generate a method to read each table from the DB with the given method name
        table_method_map = {  # table_name: method_name
            # denorm_ferc1
            "denorm_balance_sheet_assets_ferc1": "denorm_balance_sheet_assets_ferc1",
            "denorm_balance_sheet_liabilities_ferc1": "denorm_balance_sheet_liabilities_ferc1",
            "denorm_cash_flow_ferc1": "denorm_cash_flow_ferc1",
            "denorm_depreciation_amortization_summary_ferc1": "denorm_depreciation_amortization_summary_ferc1",
            "denorm_electric_energy_dispositions_ferc1": "denorm_electric_energy_dispositions_ferc1",
            "denorm_electric_energy_sources_ferc1": "denorm_electric_energy_sources_ferc1",
            "denorm_electric_operating_expenses_ferc1": "denorm_electric_operating_expenses_ferc1",
            "denorm_electric_operating_revenues_ferc1": "denorm_electric_operating_revenues_ferc1",
            "denorm_electric_plant_depreciation_changes_ferc1": "denorm_electric_plant_depreciation_changes_ferc1",
            "denorm_electric_plant_depreciation_functional_ferc1": "denorm_electric_plant_depreciation_functional_ferc1",
            "denorm_electricity_sales_by_rate_schedule_ferc1": "denorm_electricity_sales_by_rate_schedule_ferc1",
            "denorm_income_statement_ferc1": "denorm_income_statement_ferc1",
            "denorm_other_regulatory_liabilities_ferc1": "denorm_other_regulatory_liabilities_ferc1",
            "denorm_retained_earnings_ferc1": "denorm_retained_earnings_ferc1",
            "denorm_transmission_statistics_ferc1": "denorm_transmission_statistics_ferc1",
            "denorm_utility_plant_summary_ferc1": "denorm_utility_plant_summary_ferc1",
            "denorm_plants_utilities_ferc1": "pu_ferc1",
            "denorm_plants_steam_ferc1": "plants_steam_ferc1",
            "denorm_fuel_ferc1": "fuel_ferc1",
            "denorm_fuel_by_plant_ferc1": "fbp_ferc1",
            "denorm_plants_small_ferc1": "plants_small_ferc1",
            "denorm_plants_hydro_ferc1": "plants_hydro_ferc1",
            "denorm_plants_pumped_storage_ferc1": "plants_pumped_storage_ferc1",
            "denorm_purchased_power_ferc1": "purchased_power_ferc1",
            "denorm_plant_in_service_ferc1": "plant_in_service_ferc1",
            "denorm_plants_all_ferc1": "plants_all_ferc1",
            # denorm_eia (data comes from multiple EIA forms)
            "denorm_plants_eia": "plants_eia860",
            "denorm_utilities_eia": "utils_eia860",
            "denorm_boilers_eia": "boil_eia860",
            "denorm_generators_eia": "gens_eia860",
            "denorm_plants_utilities_eia": "pu_eia860",
            # eia860 (denormalized, data primarily from EIA-860)
            "denorm_ownership_eia860": "own_eia860",
            "boiler_generator_assn_eia860": "bga_eia860",
            # eia861 (clean)
            "service_territory_eia861": "service_territory_eia861",
            "sales_eia861": "sales_eia861",
            "advanced_metering_infrastructure_eia861": "advanced_metering_infrastructure_eia861",
            "demand_response_eia861": "demand_response_eia861",
            "demand_response_water_heater_eia861": "demand_response_water_heater_eia861",
            "demand_side_management_sales_eia861": "demand_side_management_sales_eia861",
            "demand_side_management_ee_dr_eia861": "demand_side_management_ee_dr_eia861",
            "demand_side_management_misc_eia861": "demand_side_management_misc_eia861",
            "distributed_generation_tech_eia861": "distributed_generation_tech_eia861",
            "distributed_generation_fuel_eia861": "distributed_generation_fuel_eia861",
            "distributed_generation_misc_eia861": "distributed_generation_misc_eia861",
            "distribution_systems_eia861": "distribution_systems_eia861",
            "dynamic_pricing_eia861": "dynamic_pricing_eia861",
            "energy_efficiency_eia861": "energy_efficiency_eia861",
            "green_pricing_eia861": "green_pricing_eia861",
            "mergers_eia861": "mergers_eia861",
            "net_metering_customer_fuel_class_eia861": "net_metering_customer_fuel_class_eia861",
            "net_metering_misc_eia861": "net_metering_misc_eia861",
            "non_net_metering_customer_fuel_class_eia861": "non_net_metering_customer_fuel_class_eia861",
            "non_net_metering_misc_eia861": "non_net_metering_misc_eia861",
            "operational_data_revenue_eia861": "operational_data_revenue_eia861",
            "operational_data_misc_eia861": "operational_data_misc_eia861",
            "reliability_eia861": "reliability_eia861",
            "utility_data_nerc_eia861": "utility_data_nerc_eia861",
            "utility_data_rto_eia861": "utility_data_rto_eia861",
            "utility_data_misc_eia861": "utility_data_misc_eia861",
            "utility_assn_eia861": "utility_assn_eia861",
            "balancing_authority_eia861": "balancing_authority_eia861",
            "balancing_authority_assn_eia861": "balancing_authority_assn_eia861",
            # eia923 (denormalized, data primarily from EIA-923)
            "denorm_boiler_fuel_AGG_eia923": "bf_eia923",
            "denorm_fuel_receipts_costs_AGG_eia923": "frc_eia923",
            "denorm_generation_AGG_eia923": "gen_original_eia923",
            "denorm_generation_fuel_combined_AGG_eia923": "gf_eia923",
            # ferc714
            "respondent_id_ferc714": "respondent_id_ferc714",
            "demand_hourly_pa_ferc714": "demand_hourly_pa_ferc714",
        }

        for table_name, method_name in table_method_map.items():
            if hasattr(PudlTabl, method_name):
                logger.warning(
                    f"Automatically generated PudlTabl method {method_name} overrides "
                    "explicitly defined class method. One of these should be deleted."
                )

            table_name = self._agg_table_name(table_name)
            # Create method called asset_name that will read the asset from DB
            self.__dict__[method_name] = partial(
                self._get_table_from_db,
                table_name=table_name,
                resource=Resource.from_id(table_name),
            )

    def _agg_table_name(self, table_name: str) -> str:
        """Substitute appropriate frequency in aggregated table names."""
        agg_freqs = {
            "AS": "yearly",
            "MS": "monthly",
        }
        if "_AGG" in table_name:
            if self.freq is not None:
                table_name = table_name.replace("AGG", agg_freqs[self.freq])
            else:
                table_name = table_name.replace("_AGG", "")
        return table_name

    def _get_table_from_db(
        self, table_name: str, resource: Resource, update: bool = False
    ) -> pd.DataFrame:
        """Grab output table from PUDL DB.

        Args:
            table_name: Name of table to get.
            resource: Resource metadata used to enforce schema on table.
            update: Ignored. Retained for backwards compatibility only.
        """
        table_name = self._agg_table_name(table_name)
        return pd.concat(
            [
                resource.enforce_schema(df)
                for df in pd.read_sql(
                    self._select_between_dates(table_name),
                    self.pudl_engine,
                    chunksize=100_000,
                )
            ]
        )

    def _select_between_dates(self, table: str) -> sa.sql.expression.Select:
        """For a given table, returns an SQL query that filters by date, if specified.

        Method uses the PudlTabl ``start_date`` and ``end_date`` attributes.  For EIA
        and most other tables, it compares ``report_date`` column against start and end
        dates.  For FERC1 ``report_year`` is used.  If neither ``report_date`` nor
        ``report_year`` are present, no date filtering is done.

        Arguments:
            table: name of table to be called in SQL query.

        Returns:
            A SQLAlchemy select object restricting the date column (either
            ``report_date`` or ``report_year``) to lie between ``self.start_date`` and
            ``self.end_date`` (inclusive).
        """
        md = sa.MetaData()
        md.reflect(self.pudl_engine)
        tbl = md.tables[f"{table}"]
        tbl_select = sa.sql.select(tbl)

        start_date = pd.to_datetime(self.start_date)
        end_date = pd.to_datetime(self.end_date)

        if "report_date" in tbl.columns:  # Date format
            date_col = tbl.c.report_date
        elif "report_year" in tbl.columns:  # Integer format
            date_col = tbl.c.report_year
            if self.start_date is not None:
                start_date = pd.to_datetime(self.start_date).year
            if self.end_date is not None:
                end_date = pd.to_datetime(self.end_date).year
        else:
            date_col = None
        if self.start_date and date_col is not None:
            tbl_select = tbl_select.where(date_col >= start_date)
        if self.end_date and date_col is not None:
            tbl_select = tbl_select.where(date_col <= end_date)
        return tbl_select

    ###########################################################################
    # EIA 860/923 OUTPUTS
    ###########################################################################
    def gen_eia923(self, update: bool = False) -> pd.DataFrame:
        """Pull EIA 923 net generation data by generator.

        Net generation is reported in two seperate tables in EIA 923: in the
        generation_eia923 and generation_fuel_eia923 tables. While the
        generation_fuel_eia923 table is more complete (the generation_eia923
        table includes only ~55% of the reported MWhs), the generation_eia923
        table is more granular (it is reported at the generator level).

        This method either grabs the generation_eia923 table that is reported
        by generator, or allocates net generation from the
        generation_fuel_eia923 table to the generator level.

        Args:
            update: Ignored. Retained for backwards compatibility only.

        Returns:
            A denormalized generation table for interactive use.
        """
        if self.fill_net_gen:
            if self.freq not in ["AS", "MS"]:
                raise AssertionError(
                    "Allocated net generation requires frequency of `AS` or `MS`, "
                    f"got {self.freq}"
                )
            table_name = self._agg_table_name("generation_fuel_by_generator_AGG_eia923")
            resource = Resource.from_id(table_name)
            gen_df = self._get_table_from_db(table_name, resource=resource)
            gen_df = gen_df.loc[:, resource.get_field_names()]
        else:
            table_name = self._agg_table_name("denorm_generation_AGG_eia923")
            resource = Resource.from_id(table_name)
            gen_df = self._get_table_from_db(table_name, resource=resource)
        return gen_df

    def gen_fuel_by_generator_energy_source_eia923(
        self, update: bool = False
    ) -> pd.DataFrame:
        """Generation and fuel consumption allocated to generators and energy source."""
        if self.freq not in ["AS", "MS"]:
            raise AssertionError(
                "Allocated net generation requires frequency of `AS` or `MS`, "
                f"got {self.freq}"
            )
        table_name = self._agg_table_name(
            "generation_fuel_by_generator_energy_source_AGG_eia923"
        )
        resource = Resource.from_id(table_name)
        return self._get_table_from_db(table_name, resource=resource)

    def gen_fuel_by_generator_eia923(self, update: bool = False) -> pd.DataFrame:
        """Net generation from gen fuel table allocated to generators."""
        if self.freq not in ["AS", "MS"]:
            raise AssertionError(
                "Allocated net generation requires frequency of `AS` or `MS`, "
                f"got {self.freq}"
            )
        table_name = self._agg_table_name("generation_fuel_by_generator_AGG_eia923")
        resource = Resource.from_id(table_name)
        return self._get_table_from_db(table_name, resource=resource)

    def gen_fuel_by_generator_energy_source_owner_eia923(
        self, update: bool = False
    ) -> pd.DataFrame:
        """Generation and fuel consumption by generator/energy_source_code/owner."""
        if self.freq != "AS":
            raise AssertionError(
                "Allocated net generation by owner can only be calculated annually. "
                f"Got a frequency of: {self.freq}"
            )
        table_name = "generation_fuel_by_generator_energy_source_owner_yearly_eia923"
        resource = Resource.from_id(table_name)
        return self._get_table_from_db(table_name, resource=resource)

    ###########################################################################
    # EIA MCOE OUTPUTS
    ###########################################################################
    def hr_by_gen(self, update: bool = False) -> pd.DataFrame:
        """Calculate and return generator level heat rates (mmBTU/MWh).

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["hr_by_gen"] is None:
            self._dfs["hr_by_gen"] = pudl.analysis.mcoe.heat_rate_by_gen(self)
        return self._dfs["hr_by_gen"]

    def hr_by_unit(self, update: bool = False) -> pd.DataFrame:
        """Calculate and return generation unit level heat rates.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["hr_by_unit"] is None:
            self._dfs["hr_by_unit"] = pudl.analysis.mcoe.heat_rate_by_unit(self)
        return self._dfs["hr_by_unit"]

    def fuel_cost(self, update: bool = False) -> pd.DataFrame:
        """Calculate and return generator level fuel costs per MWh.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["fuel_cost"] is None:
            self._dfs["fuel_cost"] = pudl.analysis.mcoe.fuel_cost(self)
        return self._dfs["fuel_cost"]

    def capacity_factor(
        self,
        update: bool = False,
        min_cap_fact: float | None = None,
        max_cap_fact: float | None = None,
    ) -> pd.DataFrame:
        """Calculate and return generator level capacity factors.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.
            min_cap_fact: Minimum capacity factor to include in the output.
            max_cap_fact: Maximum capacity factor to include in the output.

        Returns:
            A denormalized capacity factor table for interactive use.
        """
        if update or self._dfs["capacity_factor"] is None:
            self._dfs["capacity_factor"] = pudl.analysis.mcoe.capacity_factor(
                self, min_cap_fact=min_cap_fact, max_cap_fact=max_cap_fact
            )
        return self._dfs["capacity_factor"]

    def mcoe(
        self,
        update: bool = False,
        min_heat_rate: float = 5.5,
        min_fuel_cost_per_mwh: float = 0.0,
        min_cap_fact: float = 0.0,
        max_cap_fact: float = 1.5,
        all_gens: bool = True,
        gens_cols: Literal["all"] | list[str] | None = None,
    ) -> pd.DataFrame:
        """Calculate and return generator level MCOE based on EIA data.

        Eventually this calculation will include non-fuel operating expenses
        as reported in FERC Form 1, but for now only the fuel costs reported
        to EIA are included. They are attibuted based on the unit-level heat
        rates and fuel costs.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.
            min_heat_rate: lowest plausible heat rate, in mmBTU/MWh. Any MCOE
                records with lower heat rates are presumed to be invalid, and
                are discarded before returning.
            min_cap_fact: minimum generator capacity factor. Generator records
                with a lower capacity factor will be filtered out before
                returning. This allows the user to exclude generators that
                aren't being used enough to have valid.
            min_fuel_cost_per_mwh: minimum fuel cost on a per MWh basis that is
                required for a generator record to be considered valid. For
                some reason there are now a large number of $0 fuel cost
                records, which previously would have been NaN.
            max_cap_fact: maximum generator capacity factor. Generator records
                with a lower capacity factor will be filtered out before
                returning. This allows the user to exclude generators that
                aren't being used enough to have valid.
            all_gens: Controls whether the output contains records for
                all generators in the :ref:`generators_eia860` table, or only
                those generators with associated MCOE data. True by default.
            gens_cols: equal to the string "all", None, or a list of names of
                column attributes to include from the :ref:`generators_eia860` table in
                addition to the list of defined `DEFAULT_GENS_COLS` in the MCOE analysis
                module. If "all", all columns from the generators table will be included.
                By default, the `DEFAULT_GENS_COLS` defined in the MCOE analysis module
                will be merged into the final MCOE output.

        Returns:
            A compilation of generator attributes, including fuel costs per MWh.
        """
        if update or self._dfs["mcoe"] is None:
            self._dfs["mcoe"] = pudl.analysis.mcoe.mcoe(
                self,
                min_heat_rate=min_heat_rate,
                min_fuel_cost_per_mwh=min_fuel_cost_per_mwh,
                min_cap_fact=min_cap_fact,
                max_cap_fact=max_cap_fact,
                all_gens=all_gens,
                gens_cols=gens_cols,
            )
        return self._dfs["mcoe"]

    ###########################################################################
    # Plant Parts EIA outputs
    ###########################################################################
    def gens_mega_eia(
        self,
        update: bool = False,
        gens_cols: Literal["all"] | list[str] | None = None,
    ) -> pd.DataFrame:
        """Generate and return a generators table with ownership integrated.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.
            gens_cols: equal to the string "all", None, or a list of additional column
                attributes to include from the EIA 860 generators table in the output
                mega gens table. By default all columns necessary to create the plant
                parts EIA table are included.

        Returns:
            A table of all of the generators with identifying columns and data columns,
            sliced by ownership which makes "total" and "owned" records for each
            generator owner. The "owned" records have the generator's data scaled to the
            ownership percentage (e.g. if a 100 MW generator has a 75% stake owner and a
            25% stake owner, this will result in two "owned" records with 75 MW and 25
            MW). The "total" records correspond to the full plant for every owner (e.g.
            using the same 2-owner 100 MW generator as above, each owner will have a
            records with 100 MW).

        Raises:
            AssertionError: If the frequency of the pudl_out object is not ``AS``.
        """
        if update or self._dfs["gens_mega_eia"] is None:
            if self.freq != "AS":
                raise AssertionError(
                    "The frequency of the pudl_out object must be `AS` for the "
                    f"plant-parts table and we got {self.freq}"
                )
            if gens_cols is None:
                gens_cols = []
            if gens_cols != "all":
                default_cols = [
                    "technology_description",
                    "energy_source_code_1",
                    "prime_mover_code",
                    "generator_operating_date",
                    "generator_retirement_date",
                    "operational_status",
                    "capacity_mw",
                    "fuel_type_code_pudl",
                    "planned_generator_retirement_date",
                ]
                gens_cols = list(set(gens_cols + default_cols))
            self._dfs[
                "gens_mega_eia"
            ] = pudl.analysis.plant_parts_eia.MakeMegaGenTbl().execute(
                mcoe=self.mcoe(all_gens=True, gens_cols=gens_cols),
                own_eia860=self.own_eia860(),
            )
        return self._dfs["gens_mega_eia"]

    def plant_parts_eia(
        self,
        update: bool = False,
        update_gens_mega: bool = False,
        gens_cols: Any = None,
    ) -> pd.DataFrame:
        """Generate and return master plant-parts EIA.

        Args:
            update: If True, re-calculate dataframe even if a cached version exists.
            update_gens_mega: If True, update the gigantic Gens Mega table.
            gens_cols: equal to the string "all", None, or a list of
                additional column attributes to include from the EIA 860 generators table
                in the output mega gens table. By default all columns necessary to create
                the EIA plant part list are included.
        """
        update_any = any([update, update_gens_mega])
        if update_any or self._dfs["plant_parts_eia"] is None:
            # default columns needed to create plant part list
            if gens_cols is None:
                gens_cols = []
            if gens_cols != "all":
                default_cols = [
                    "technology_description",
                    "energy_source_code_1",
                    "prime_mover_code",
                    "generator_operating_date",
                    "generator_retirement_date",
                    "operational_status",
                    "capacity_mw",
                    "fuel_type_code_pudl",
                    "planned_generator_retirement_date",
                ]
                gens_cols = list(set(gens_cols + default_cols))
            # make the plant-parts objects
            self.parts_compiler = pudl.analysis.plant_parts_eia.MakePlantParts(self)
            # make the plant-parts df!
            self._dfs["plant_parts_eia"] = self.parts_compiler.execute(
                gens_mega=self.gens_mega_eia(
                    update=update_gens_mega,
                    gens_cols=gens_cols,
                )
            )

        return self._dfs["plant_parts_eia"]

    ###########################################################################
    # GLUE OUTPUTS
    ###########################################################################
    def ferc1_eia(
        self,
        update: bool = False,
        update_plant_parts_eia: bool = False,
        update_plants_all_ferc1: bool = False,
        update_fbp_ferc1: bool = False,
    ) -> pd.DataFrame:
        """Generate the connection between FERC1 and EIA."""
        update_any = any(
            [update, update_plant_parts_eia, update_plants_all_ferc1, update_fbp_ferc1]
        )
        if update_any or self._dfs["ferc1_eia"] is None:
            self._dfs["ferc1_eia"] = pudl.analysis.ferc1_eia.execute(
                plant_parts_eia=self.plant_parts_eia(update=update_plant_parts_eia),
                plants_all_ferc1=self.plants_all_ferc1(),
                fbp_ferc1=self.fbp_ferc1(),
            )
        return self._dfs["ferc1_eia"]

    def epacamd_eia(self) -> pd.DataFrame:
        """Read the EPACAMD-EIA Crosswalk from the PUDL DB."""
        return pd.read_sql("epacamd_eia", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="glue"
        )
