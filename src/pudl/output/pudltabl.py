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
from typing import Any, Literal

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.analysis.allocate_net_gen import (
    aggregate_gen_fuel_by_generator,
    allocate_gen_fuel_by_generator_energy_source,
    scale_allocated_net_gen_by_ownership,
)
from pudl.metadata.classes import Package
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

    def pu_eia860(self, update=False):
        """Pull a dataframe of EIA plant-utility associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["pu_eia"] is None:
            self._dfs["pu_eia"] = pudl.output.eia860.plants_utils_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["pu_eia"]

    def pu_ferc1(self, update=False):
        """Pull a dataframe of FERC plant-utility associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["pu_ferc1"] is None:
            self._dfs["pu_ferc1"] = pudl.output.ferc1.plants_utils_ferc1(
                self.pudl_engine
            )
        return self._dfs["pu_ferc1"]

    def advanced_metering_infrastructure_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "advanced_metering_infrastructure_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def balancing_authority_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("balancing_authority_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def balancing_authority_assn_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("balancing_authority_assn_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def demand_response_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("demand_response_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def demand_response_water_heater_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "demand_response_water_heater_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def demand_side_management_sales_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "demand_side_management_sales_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def demand_side_management_ee_dr_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "demand_side_management_ee_dr_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def demand_side_management_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("demand_side_management_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def distributed_generation_tech_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("distributed_generation_tech_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def distributed_generation_fuel_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("distributed_generation_fuel_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def distributed_generation_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("distributed_generation_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def distribution_systems_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("distribution_systems_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def dynamic_pricing_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("dynamic_pricing_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def energy_efficiency_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("energy_efficiency_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def green_pricing_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("green_pricing_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def mergers_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("mergers_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def net_metering_customer_fuel_class_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "net_metering_customer_fuel_class_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def net_metering_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("net_metering_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def non_net_metering_customer_fuel_class_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql(
            "non_net_metering_customer_fuel_class_eia861", self.pudl_engine
        ).pipe(apply_pudl_dtypes, group="eia")

    def non_net_metering_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("non_net_metering_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def operational_data_revenue_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("operational_data_revenue_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def operational_data_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("operational_data_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def reliability_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("reliability_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def sales_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("sales_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def service_territory_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("service_territory_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def utility_assn_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("utility_assn_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def utility_data_nerc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("utility_data_nerc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def utility_data_rto_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("utility_data_rto_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    def utility_data_misc_eia861(self) -> pd.DataFrame:
        """An interim EIA 861 output function."""
        return pd.read_sql("utility_data_misc_eia861", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="eia"
        )

    ###########################################################################
    # FERC 714 Outputs
    ###########################################################################
    def respondent_id_ferc714(self) -> pd.DataFrame:
        """An interim FERC 714 output function."""
        return pd.read_sql("respondent_id_ferc714", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="ferc714"
        )

    def demand_hourly_pa_ferc714(self) -> pd.DataFrame:
        """An interim FERC 714 output function."""
        dhpa_res = Package.from_resource_ids().get_resource("demand_hourly_pa_ferc714")
        # Concatenating a bunch of smaller chunks reduces peak memory usage drastically
        # and doesn't seem to take any longer.
        return pd.concat(
            [
                # enforce_schema() cuts memory use by ~70% b/c of categorical tzones
                dhpa_res.enforce_schema(df)
                for df in pd.read_sql(
                    "demand_hourly_pa_ferc714",
                    self.pudl_engine,
                    chunksize=100_000,
                )
            ]
        )

    ###########################################################################
    # EIA 860/923 OUTPUTS
    ###########################################################################
    def utils_eia860(self, update=False):
        """Pull a dataframe describing utilities reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["utils_eia860"] is None:
            self._dfs["utils_eia860"] = pudl.output.eia860.utilities_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["utils_eia860"]

    def bga_eia860(self, update=False):
        """Pull a dataframe of boiler-generator associations from EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["bga_eia860"] is None:
            self._dfs["bga_eia860"] = pudl.output.eia860.boiler_generator_assn_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["bga_eia860"]

    def plants_eia860(self, update=False):
        """Pull a dataframe of plant level info reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_eia860"] is None:
            self._dfs["plants_eia860"] = pudl.output.eia860.plants_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self._dfs["plants_eia860"]

    def gens_eia860(self, update=False):
        """Pull a dataframe describing generators, as reported in EIA 860.

        If you want to fill the technology_description field, recreate
        the pudl_out object with the parameter fill_tech_desc = True.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["gens_eia860"] is None:
            self._dfs["gens_eia860"] = pudl.output.eia860.generators_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,
                unit_ids=self.unit_ids,
                fill_tech_desc=self.fill_tech_desc,
            )

        return self._dfs["gens_eia860"]

    def boil_eia860(self, update=False):
        """Pull a dataframe of boiler level info reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["boil_eia860"] is None:
            self._dfs["boil_eia860"] = pudl.output.eia860.boilers_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self._dfs["boil_eia860"]

    def own_eia860(self, update=False):
        """Pull a dataframe of generator level ownership data from EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["own_eia860"] is None:
            self._dfs["own_eia860"] = pudl.output.eia860.ownership_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["own_eia860"]

    def gf_eia923(self, update: bool = False) -> pd.DataFrame:
        """Pull combined nuclear and non-nuclear generation fuel data.

        Args:
            update: If True, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            A denormalized table for interactive use.
        """
        if update or self._dfs["gf_eia923"] is None:
            self._dfs["gf_eia923"] = pudl.output.eia923.generation_fuel_all_eia923(
                gf=self.gf_nonuclear_eia923(update=update),
                gfn=self.gf_nuclear_eia923(update=update),
            )
        return self._dfs["gf_eia923"]

    def gf_nonuclear_eia923(self, update: bool = False) -> pd.DataFrame:
        """Pull non-nuclear EIA 923 generation and fuel consumption data.

        Args:
            update: If True, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            A denormalized table for interactive use.
        """
        if update or self._dfs["gf_nonuclear_eia923"] is None:
            self._dfs[
                "gf_nonuclear_eia923"
            ] = pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                nuclear=False,
            )
        return self._dfs["gf_nonuclear_eia923"]

    def gf_nuclear_eia923(self, update: bool = False) -> pd.DataFrame:
        """Pull EIA 923 generation and fuel consumption data for nuclear units.

        Args:
            update: If True, re-calculate the output dataframe, even if a cached version
                exists.

        Returns:
            A denormalized table for interactive use.
        """
        if update or self._dfs["gf_nuclear_eia923"] is None:
            self._dfs["gf_nuclear_eia923"] = pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                nuclear=True,
            )
        return self._dfs["gf_nuclear_eia923"]

    def frc_eia923(self, update=False):
        """Pull EIA 923 fuel receipts and costs data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["frc_eia923"] is None:
            self._dfs["frc_eia923"] = pudl.output.eia923.fuel_receipts_costs_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                fill=self.fill_fuel_cost,
                roll=self.roll_fuel_cost,
            )
        return self._dfs["frc_eia923"]

    def bf_eia923(self, update=False):
        """Pull EIA 923 boiler fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["bf_eia923"] is None:
            self._dfs["bf_eia923"] = pudl.output.eia923.boiler_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self._dfs["bf_eia923"]

    def gen_eia923(self, update=False):
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
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["gen_eia923"] is None:
            if self.fill_net_gen:
                if self.freq not in ["AS", "MS"]:
                    raise AssertionError(
                        "Frequency must be either `AS` or `MS` to allocate net "
                        f"generation. Got {self.freq}"
                    )
                logger.info(
                    "Allocating net generation from the generation_fuel_eia923 "
                    "to the generator level instead of using the less complete "
                    "generation_eia923 table."
                )

                self._dfs["gen_eia923"] = self.gen_fuel_by_generator_eia923(
                    update=update
                ).loc[:, list(self.gen_original_eia923().columns)]
            else:
                self._dfs["gen_eia923"] = self.gen_original_eia923(update=update)
        return self._dfs["gen_eia923"]

    def gen_original_eia923(self, update=False):
        """Pull the original EIA 923 net generation data by generator."""
        if update or self._dfs["gen_og_eia923"] is None:
            self._dfs["gen_og_eia923"] = pudl.output.eia923.generation_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        return self._dfs["gen_og_eia923"]

    def gen_fuel_by_generator_energy_source_eia923(self, update=False):
        """Net generation and fuel data allocated to generator/energy_source_code.

        Net generation and fuel data originally reported in the gen fuel table
        """
        if update or self._dfs["gen_fuel_by_genid_esc_eia923"] is None:
            self._dfs[
                "gen_fuel_by_genid_esc_eia923"
            ] = allocate_gen_fuel_by_generator_energy_source(pudl_out=self)
        return self._dfs["gen_fuel_by_genid_esc_eia923"]

    def gen_fuel_by_generator_eia923(self, update=False):
        """Net generation from gen fuel table allocated to generators."""
        if update or self._dfs["gen_fuel_allocated_eia923"] is None:
            if self.freq not in ["AS", "MS"]:
                raise AssertionError(
                    "Frequency must be either `AS` or `MS` to allocate net "
                    f"generation. Got {self.freq}"
                )
            self._dfs["gen_fuel_allocated_eia923"] = aggregate_gen_fuel_by_generator(
                pudl_out=self,
                net_gen_fuel_alloc=self.gen_fuel_by_generator_energy_source_eia923(
                    update=update
                ),
            )
        return self._dfs["gen_fuel_allocated_eia923"]

    def gen_fuel_by_generator_energy_source_owner_eia923(self, update=False):
        """Generation and fuel consumption by generator/energy_source_code/owner."""
        if update or self._dfs["gen_fuel_by_genid_esc_own"] is None:
            self._dfs[
                "gen_fuel_by_genid_esc_own"
            ] = scale_allocated_net_gen_by_ownership(
                gen_pm_fuel=self.gen_fuel_by_generator_energy_source_eia923(),
                gens=self.gens_eia860(),
                own_eia860=self.own_eia860(),
            )
        return self._dfs["gen_fuel_by_genid_esc_own"]

    ###########################################################################
    # FERC FORM 1 OUTPUTS
    ###########################################################################
    def plants_steam_ferc1(self, update=False):
        """Pull the FERC Form 1 steam plants data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_steam_ferc1"] is None:
            self._dfs["plants_steam_ferc1"] = pudl.output.ferc1.plants_steam_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plants_steam_ferc1"]

    def fuel_ferc1(self, update=False):
        """Pull the FERC Form 1 steam plants fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["fuel_ferc1"] is None:
            self._dfs["fuel_ferc1"] = pudl.output.ferc1.fuel_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["fuel_ferc1"]

    def fbp_ferc1(self, update=False):
        """Summarize FERC Form 1 fuel usage by plant.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["fbp_ferc1"] is None:
            self._dfs["fbp_ferc1"] = pudl.output.ferc1.fuel_by_plant_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["fbp_ferc1"]

    def plants_small_ferc1(self, update=False):
        """Pull the FERC Form 1 Small Plants Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_small_ferc1"] is None:
            self._dfs["plants_small_ferc1"] = pudl.output.ferc1.plants_small_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plants_small_ferc1"]

    def plants_hydro_ferc1(self, update=False):
        """Pull the FERC Form 1 Hydro Plants Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_hydro_ferc1"] is None:
            self._dfs["plants_hydro_ferc1"] = pudl.output.ferc1.plants_hydro_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plants_hydro_ferc1"]

    def plants_pumped_storage_ferc1(self, update=False):
        """Pull the FERC Form 1 Pumped Storage Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_pumped_storage_ferc1"] is None:
            self._dfs[
                "plants_pumped_storage_ferc1"
            ] = pudl.output.ferc1.plants_pumped_storage_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plants_pumped_storage_ferc1"]

    def purchased_power_ferc1(self, update=False):
        """Pull the FERC Form 1 Purchased Power Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["purchased_power_ferc1"] is None:
            self._dfs[
                "purchased_power_ferc1"
            ] = pudl.output.ferc1.purchased_power_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["purchased_power_ferc1"]

    def plant_in_service_ferc1(self, update=False):
        """Pull the FERC Form 1 Plant in Service Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plant_in_service_ferc1"] is None:
            self._dfs[
                "plant_in_service_ferc1"
            ] = pudl.output.ferc1.plant_in_service_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plant_in_service_ferc1"]

    def plants_all_ferc1(self, update=False):
        """Pull the FERC Form 1 all plants table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["plants_all_ferc1"] is None:
            self._dfs["plants_all_ferc1"] = pudl.output.ferc1.plants_all_ferc1(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            )
        return self._dfs["plants_all_ferc1"]

    ###########################################################################
    # EIA MCOE OUTPUTS
    ###########################################################################
    def hr_by_gen(self, update=False):
        """Calculate and return generator level heat rates (mmBTU/MWh).

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["hr_by_gen"] is None:
            self._dfs["hr_by_gen"] = pudl.analysis.mcoe.heat_rate_by_gen(self)
        return self._dfs["hr_by_gen"]

    def hr_by_unit(self, update=False):
        """Calculate and return generation unit level heat rates.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["hr_by_unit"] is None:
            self._dfs["hr_by_unit"] = pudl.analysis.mcoe.heat_rate_by_unit(self)
        return self._dfs["hr_by_unit"]

    def fuel_cost(self, update=False):
        """Calculate and return generator level fuel costs per MWh.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
        """
        if update or self._dfs["fuel_cost"] is None:
            self._dfs["fuel_cost"] = pudl.analysis.mcoe.fuel_cost(self)
        return self._dfs["fuel_cost"]

    def capacity_factor(self, update=False, min_cap_fact=None, max_cap_fact=None):
        """Calculate and return generator level capacity factors.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.
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
        gens_cols: Any = None,
    ):
        """Calculate and return generator level MCOE based on EIA data.

        Eventually this calculation will include non-fuel operating expenses
        as reported in FERC Form 1, but for now only the fuel costs reported
        to EIA are included. They are attibuted based on the unit-level heat
        rates and fuel costs.

        Args:
            update: If true, re-calculate the output dataframe, even if
                a cached version exists.
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
            :class:`pandas.DataFrame`: a compilation of generator attributes,
            including fuel costs per MWh.
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

    def gens_mega_eia(
        self,
        update: bool = False,
        gens_cols: Any = None,
    ) -> pd.DataFrame:
        """Generate and return a generators table with ownership integrated.

        Args:
            update: If True, re-calculate the output dataframe, even
                if a cached version exists.
            gens_cols: equal to the string "all", None, or a list of
                additional column attributes to include from the EIA 860 generators table
                in the output mega gens table. By default all columns necessary to create
                the plant parts EIA table are included.

        Returns:
            A table of all of the generators with identifying
            columns and data columns, sliced by ownership which makes
            "total" and "owned" records for each generator owner. The "owned"
            records have the generator's data scaled to the ownership percentage
            (e.g. if a 100 MW generator has a 75% stake owner and a 25% stake
            owner, this will result in two "owned" records with 75 MW and 25
            MW). The "total" records correspond to the full plant for every
            owner (e.g. using the same 2-owner 100 MW generator as above, each
            owner will have a records with 100 MW).

        Raises:
            AssertionError: If the frequency of the pudl_out object is not 'AS'
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
            update: If true, re-calculate the output dataframe, even
                if a cached version exists.
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
                plants_all_ferc1=self.plants_all_ferc1(update=update_plants_all_ferc1),
                fbp_ferc1=self.fbp_ferc1(update=update_fbp_ferc1),
            )
        return self._dfs["ferc1_eia"]

    def epacamd_eia(self) -> pd.DataFrame:
        """Read the EPACAMD-EIA Crosswalk from the PUDL DB."""
        return pd.read_sql("epacamd_eia", self.pudl_engine).pipe(
            apply_pudl_dtypes, group="glue"
        )

    ###########################################################################
    # FOR PICKLING AND OTHER IO
    ###########################################################################

    def __getstate__(self) -> dict:
        """Get current object state for serializing (pickling).

        This method is run as part of pickling the object. It needs to return the
        object's current state with any un-serializable objects converted to a form that
        can be serialized. See :meth:`object.__getstate__` for further details on the
        expected behavior of this method.
        """
        return self.__dict__.copy() | {
            # defaultdict may be serializable but lambdas are not, so it must go
            "_dfs": dict(self.__dict__["_dfs"]),
            # sqlalchemy engines are also a problem here, saving the URL should
            # provide enough of what is needed to recreate it, though that means the
            # pickle is not portable, but any fix to that will happen when the object
            # is restored
            "pudl_engine": str(self.__dict__["pudl_engine"].url),
        }

    def __setstate__(self, state: dict) -> None:
        """Restore the object's state from a dictionary.

        This method is run when the object is restored from a pickle. Anything
        that was changed in :meth:`pudl.output.pudltabl.PudlTabl.__getstate__` must be
        undone here. Another important detail is that ``__init__`` is not run when an
        object is de-serialized, so any setup there that alters external state might
        need to happen here as well.

        Args:
            state: the object state to restore. This is effectively the output
                of :meth:`pudl.output.pudltabl.PudlTabl.__getstate__`.
        """
        try:
            pudl_engine = sa.create_engine(state["pudl_engine"])
            # make sure that the URL for the engine from ``state`` is usable now
            pudl_engine.connect()
        except sa.exc.OperationalError:
            # if the URL from ``state`` is not valid, e.g. because it is for a local
            # DB on a different computer, create the engine from PUDL defaults
            pudl_settings = pudl.workspace.setup.get_defaults()
            logger.warning(
                "Unable to connect to the restored pudl_db URL %s. "
                "Will use the default pudl_db %s instead.",
                state["pudl_engine"],
                pudl_settings["pudl_db"],
            )
            pudl_engine = sa.create_engine(pudl_settings["pudl_db"])

        self.__dict__ = state | {
            # recreate the defaultdict from the vanilla one from ``state``
            "_dfs": defaultdict(lambda: None, state["_dfs"]),
            "pudl_engine": pudl_engine,
        }


def get_table_meta(pudl_engine):
    """Grab the pudl SQLite database table metadata."""
    md = sa.MetaData()
    md.reflect(pudl_engine)
    return md.tables
