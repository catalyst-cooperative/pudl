"""
This module provides a class enabling tabular compilations from the PUDL DB.

Many of our potential users are comfortable using spreadsheets, not databases,
so we are creating a collection of tabular outputs that contain the most
useful core information from the PUDL data packages, including additional keys
and human readable names for the objects (utilities, plants, generators) being
described in the table.

These tabular outputs can be joined with each other using those keys, and used
as a data source within Microsoft Excel, Access, R Studio, or other data
analysis packages that folks may be familiar with.  They aren't meant to
completely replicate all the data and relationships contained within the full
PUDL database, but should serve as a generally usable set of PUDL data
products.

The PudlTabl class can also provide access to complex derived values, like the
generator and plant level marginal cost of electricity (MCOE), which are
defined in the analysis module.

In the long run, this is a probably a kind of prototype for pre-packaged API
outputs or data products that we might want to be able to provide to users a la
carte.

Todo:
    Return to for update arg and returns values in functions below

"""

import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Literal, Union

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.settings import Eia861Settings, Ferc714Settings
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl(object):
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    def __init__(
        self,
        pudl_engine: sa.engine.Engine,
        ds: Union[Datastore, None] = None,
        freq: Literal["AS", "MS", None] = None,
        start_date: Union[str, date, datetime, pd.Timestamp] = None,
        end_date: Union[str, date, datetime, pd.Timestamp] = None,
        fill_fuel_cost: bool = False,
        roll_fuel_cost: bool = False,
        fill_net_gen: bool = False,
        fill_tech_desc: bool = True,
        unit_ids: bool = False
    ):
        """
        Initialize the PUDL output object.

        Private data members are not initialized until they are requested.
        They are then cached within the object unless they get re-initialized
        via a method that includes update=True.

        Some methods (e.g mcoe) will take a while to run, since they need to
        pull substantial data and do a bunch of calculations.

        Args:
            pudl_engine: A connection engine for the PUDL DB.
            freq: A string indicating the time frequency at which to aggregate
                reported data. ``MS`` is monththly and ``AS`` is annually. If
                None, the data will not be aggregated.
            ds: A PUDL Datastore from which raw input data can be obtained.
                Required because the ``ferc714`` and ``eia861`` datasets are
                not yet integrated into the database outputs, and are instead
                processed from their raw form upon request.
            start_date: Beginning date for data to pull from the PUDL DB. If
                a string, it should use the ISO 8601 ``YYYY-MM-DD`` format.
            end_date: End date for data to pull from the PUDL DB. If a string,
                it should use the ISO 8601 ``YYYY-MM-DD`` format.
            fill_fuel_cost: if True, fill in missing ``frc_eia923()`` fuel cost
                data with state-level averages obtained from EIA's API.
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
        # Validating ds is deferred to the etl_eia861 & etl_ferc714 methods
        # because those are the only places a datastore is required.
        self.ds: Union[Datastore, None] = ds
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
        eia_dates = pudl.helpers.get_working_eia_dates()
        if start_date is None:
            self.start_date = min(eia_dates)
        else:
            # Make sure it's a date... and not a string.
            self.start_date = pd.to_datetime(start_date)

        if end_date is None:
            self.end_date = max(eia_dates)
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
        """
        Pull a dataframe of EIA plant-utility associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['pu_eia'] is None:
            self._dfs['pu_eia'] = pudl.output.eia860.plants_utils_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['pu_eia']

    def pu_ferc1(self, update=False):
        """
        Pull a dataframe of FERC plant-utility associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['pu_ferc1'] is None:
            self._dfs['pu_ferc1'] = pudl.output.ferc1.plants_utils_ferc1(
                self.pudl_engine)
        return self._dfs['pu_ferc1']

    ###########################################################################
    # EIA 861 Interim Outputs (awaiting full DB integration)
    ###########################################################################
    def etl_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update: bool = False
    ):
        """
        A single function that runs the temporary EIA 861 ETL and sets all DFs.

        This is an interim solution that provides a (somewhat) standard way of accessing
        the EIA 861 data prior to its being fully integrated into the PUDL database. If
        any of the dataframes is attempted to be accessed, all of them are set. Only
        the tables that have actual transform functions are included, and as new
        transform functions are completed, they would need to be added to the list
        below. Surely there is a way to do this automatically / magically but that's
        beyond my knowledge right now.

        Args:
            eia861_settings: Object containing validated settings relevant to EIA 861.
            update: Whether to overwrite the existing dataframes if they exist.

        """
        if isinstance(self.ds, Datastore):
            pass
        elif self.ds is None:
            pudl_settings = pudl.workspace.setup.get_defaults()
            if pudl_settings["pudl_in"] is None:
                raise FileNotFoundError(
                    "In order to run the ad-hoc EIA-861 ETL PUDL needs a valid "
                    "Datastore, but none was found. Run 'pudl_setup --help' "
                    "to see how to create one."
                )
            self.ds = Datastore(local_cache_path=pudl_settings["data_dir"])
        else:
            raise TypeError(
                "PudlTabl needs a PUDL Datastore object, but we got "
                f"a {type(self.ds)}."
            )

        if update or self._dfs["balancing_authority_eia861"] is None:
            logger.warning("Running the interim EIA 861 ETL process!")

            eia861_raw_dfs = (
                pudl.extract.eia861.Extractor(self.ds)
                .extract(settings=eia861_settings)
            )
            self._dfs.update(
                pudl.transform.eia861.transform(eia861_raw_dfs, eia861_settings))

    def advanced_metering_infrastructure_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["advanced_metering_infrastructure_eia861"]

    def balancing_authority_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["balancing_authority_eia861"]

    def balancing_authority_assn_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["balancing_authority_assn_eia861"]

    def demand_response_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["demand_response_eia861"]

    def demand_response_water_heater_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["demand_response_water_heater_eia861"]

    def demand_side_management_sales_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["demand_side_management_sales_eia861"]

    def demand_side_management_ee_dr_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["demand_side_management_ee_dr_eia861"]

    def demand_side_management_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["demand_side_management_misc_eia861"]

    def distributed_generation_tech_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["distributed_generation_tech_eia861"]

    def distributed_generation_fuel_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["distributed_generation_fuel_eia861"]

    def distributed_generation_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["distributed_generation_misc_eia861"]

    def distribution_systems_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["distribution_systems_eia861"]

    def dynamic_pricing_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["dynamic_pricing_eia861"]

    def energy_efficiency_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["energy_efficiency_eia861"]

    def green_pricing_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["green_pricing_eia861"]

    def mergers_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["mergers_eia861"]

    def net_metering_customer_fuel_class_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["net_metering_customer_fuel_class_eia861"]

    def net_metering_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["net_metering_misc_eia861"]

    def non_net_metering_customer_fuel_class_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["non_net_metering_customer_fuel_class_eia861"]

    def non_net_metering_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["non_net_metering_misc_eia861"]

    def operational_data_revenue_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["operational_data_revenue_eia861"]

    def operational_data_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["operational_data_misc_eia861"]

    def reliability_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["reliability_eia861"]

    def sales_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["sales_eia861"]

    def service_territory_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["service_territory_eia861"]

    def utility_assn_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["utility_assn_eia861"]

    def utility_data_nerc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["utility_data_nerc_eia861"]

    def utility_data_rto_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["utility_data_rto_eia861"]

    def utility_data_misc_eia861(
        self,
        eia861_settings: Eia861Settings = Eia861Settings(),
        update=False
    ):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["utility_data_misc_eia861"]

    ###########################################################################
    # FERC 714 Interim Outputs (awaiting full DB integration)
    ###########################################################################
    def etl_ferc714(
        self,
        ferc714_settings: Ferc714Settings = Ferc714Settings(),
        update: bool = False
    ):
        """
        A single function that runs the temporary FERC 714 ETL and sets all DFs.

        This is an interim solution, so that we can have a (relatively) standard way of
        accessing the FERC 714 data prior to getting it integrated into the PUDL DB.
        Some of these are not yet cleaned up, but there are dummy transform functions
        which pass through the raw DFs with some minor alterations, so all the data is
        available as it exists right now.

        An attempt to access *any* of the dataframes results in all of them being
        populated, since generating all of them is almost the same amount of work as
        generating one of them.

        Args:
            update: Whether to overwrite the existing dataframes if they exist.

        """
        if isinstance(self.ds, Datastore):
            pass
        elif self.ds is None:
            pudl_settings = pudl.workspace.setup.get_defaults()
            if pudl_settings["pudl_in"] is None:
                raise FileNotFoundError(
                    "In order to run the ad-hoc FERC-714 ETL PUDL needs a valid "
                    "Datastore, but none was found. Run 'pudl_setup --help' "
                    "to see how to create one."
                )
            self.ds = Datastore(local_cache_path=pudl_settings["data_dir"])
        else:
            raise TypeError(
                "PudlTabl needs a PUDL Datastore object, but we got "
                f"a {type(self.ds)}."
            )

        if update or self._dfs["respondent_id_ferc714"] is None:
            logger.warning("Running the interim FERC 714 ETL process!")
            ferc714_raw_dfs = pudl.extract.ferc714.extract(
                ferc714_settings=ferc714_settings, ds=self.ds)
            ferc714_tfr_dfs = pudl.transform.ferc714.transform(
                ferc714_raw_dfs, ferc714_settings=ferc714_settings)
            self._dfs.update(ferc714_tfr_dfs)

    def respondent_id_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["respondent_id_ferc714"]

    def demand_hourly_pa_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["demand_hourly_pa_ferc714"]

    def description_pa_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["description_pa_ferc714"]

    def id_certification_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["id_certification_ferc714"]

    def gen_plants_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["gen_plants_ba_ferc714"]

    def demand_monthly_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["demand_monthly_ba_ferc714"]

    def net_energy_load_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["net_energy_load_ba_ferc714"]

    def adjacency_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["adjacency_ba_ferc714"]

    def interchange_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["interchange_ba_ferc714"]

    def lambda_hourly_ba_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["lambda_hourly_ba_ferc714"]

    def lambda_description_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["lambda_description_ferc714"]

    def demand_forecast_pa_ferc714(self, update=False):
        """An interim FERC 714 output function."""
        self.etl_ferc714(update=update)
        return self._dfs["demand_forecast_pa_ferc714"]

    ###########################################################################
    # EIA 860/923 OUTPUTS
    ###########################################################################

    def utils_eia860(self, update=False):
        """
        Pull a dataframe describing utilities reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['utils_eia860'] is None:
            self._dfs['utils_eia860'] = pudl.output.eia860.utilities_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['utils_eia860']

    def bga_eia860(self, update=False):
        """
        Pull a dataframe of boiler-generator associations from EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['bga_eia860'] is None:
            self._dfs['bga_eia860'] = pudl.output.eia860.boiler_generator_assn_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['bga_eia860']

    def plants_eia860(self, update=False):
        """
        Pull a dataframe of plant level info reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_eia860'] is None:
            self._dfs['plants_eia860'] = pudl.output.eia860.plants_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,)
        return self._dfs['plants_eia860']

    def gens_eia860(self, update=False):
        """
        Pull a dataframe describing generators, as reported in EIA 860.

        If you want to fill the technology_description field, recreate
        the pudl_out object with the parameter fill_tech_desc = True.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['gens_eia860'] is None:
            self._dfs['gens_eia860'] = pudl.output.eia860.generators_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,
                unit_ids=self.unit_ids,
                fill_tech_desc=self.fill_tech_desc)

        return self._dfs['gens_eia860']

    def own_eia860(self, update=False):
        """
        Pull a dataframe of generator level ownership data from EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['own_eia860'] is None:
            self._dfs['own_eia860'] = pudl.output.eia860.ownership_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['own_eia860']

    def gf_eia923(self, update: bool = False) -> pd.DataFrame:
        """
        Pull combined nuclear and non-nuclear generation fuel data.

        Args:
            update: If True, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            A denormalized table for interactive use.

        """
        if update or self._dfs['gf_eia923'] is None:
            self._dfs['gf_eia923'] = (
                pudl.output.eia923.generation_fuel_all_eia923(
                    gf=self.gf_nonuclear_eia923(update=update),
                    gfn=self.gf_nuclear_eia923(update=update),
                )
            )
        return self._dfs['gf_eia923']

    def gf_nonuclear_eia923(self, update: bool = False) -> pd.DataFrame:
        """
        Pull non-nuclear EIA 923 generation and fuel consumption data.

        Args:
            update: If True, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            A denormalized table for interactive use.

        """
        if update or self._dfs['gf_nonuclear_eia923'] is None:
            self._dfs['gf_nonuclear_eia923'] = pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                nuclear=False,
            )
        return self._dfs['gf_nonuclear_eia923']

    def gf_nuclear_eia923(self, update: bool = False) -> pd.DataFrame:
        """
        Pull EIA 923 generation and fuel consumption data for nuclear units.

        Args:
            update: If True, re-calculate the output dataframe, even if a cached version
                exists.

        Returns:
            A denormalized table for interactive use.

        """
        if update or self._dfs['gf_nuclear_eia923'] is None:
            self._dfs['gf_nuclear_eia923'] = pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                nuclear=True
            )
        return self._dfs['gf_nuclear_eia923']

    def frc_eia923(self, update=False):
        """
        Pull EIA 923 fuel receipts and costs data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['frc_eia923'] is None:
            self._dfs['frc_eia923'] = pudl.output.eia923.fuel_receipts_costs_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                fill=self.fill_fuel_cost,
                roll=self.roll_fuel_cost)
        return self._dfs['frc_eia923']

    def bf_eia923(self, update=False):
        """
        Pull EIA 923 boiler fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['bf_eia923'] is None:
            self._dfs['bf_eia923'] = pudl.output.eia923.boiler_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['bf_eia923']

    def gen_eia923(self, update=False):
        """
        Pull EIA 923 net generation data by generator.

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
        if update or self._dfs['gen_eia923'] is None:
            if self.fill_net_gen:
                logger.info(
                    'Allocating net generation from the generation_fuel_eia923 '
                    'to the generator level instead of using the less complete '
                    'generation_eia923 table.'
                )
                self._dfs['gen_eia923'] = self.gen_allocated_eia923(
                    update=update)
            else:
                self._dfs['gen_eia923'] = self.gen_original_eia923(
                    update=update)
        return self._dfs['gen_eia923']

    def gen_original_eia923(self, update=False):
        """Pull the original EIA 923 net generation data by generator."""
        if update or self._dfs['gen_og_eia923'] is None:
            self._dfs['gen_og_eia923'] = pudl.output.eia923.generation_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['gen_og_eia923']

    def gen_allocated_eia923(self, update=False):
        """Net generation from gen fuel table allocated to generators."""
        if update or self._dfs['gen_allocated_eia923'] is None:
            self._dfs['gen_allocated_eia923'] = (
                pudl.analysis.allocate_net_gen.allocate_gen_fuel_by_gen(self)
            )
        return self._dfs['gen_allocated_eia923']

    ###########################################################################
    # FERC FORM 1 OUTPUTS
    ###########################################################################
    def plants_steam_ferc1(self, update=False):
        """
        Pull the FERC Form 1 steam plants data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_steam_ferc1'] is None:
            self._dfs['plants_steam_ferc1'] = pudl.output.ferc1.plants_steam_ferc1(
                self.pudl_engine)
        return self._dfs['plants_steam_ferc1']

    def fuel_ferc1(self, update=False):
        """
        Pull the FERC Form 1 steam plants fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['fuel_ferc1'] is None:
            self._dfs['fuel_ferc1'] = pudl.output.ferc1.fuel_ferc1(
                self.pudl_engine)
        return self._dfs['fuel_ferc1']

    def fbp_ferc1(self, update=False):
        """
        Summarize FERC Form 1 fuel usage by plant.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['fbp_ferc1'] is None:
            self._dfs['fbp_ferc1'] = pudl.output.ferc1.fuel_by_plant_ferc1(
                self.pudl_engine)
        return self._dfs['fbp_ferc1']

    def plants_small_ferc1(self, update=False):
        """
        Pull the FERC Form 1 Small Plants Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_small_ferc1'] is None:
            self._dfs['plants_small_ferc1'] = pudl.output.ferc1.plants_small_ferc1(
                self.pudl_engine)
        return self._dfs['plants_small_ferc1']

    def plants_hydro_ferc1(self, update=False):
        """
        Pull the FERC Form 1 Hydro Plants Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_hydro_ferc1'] is None:
            self._dfs['plants_hydro_ferc1'] = pudl.output.ferc1.plants_hydro_ferc1(
                self.pudl_engine)
        return self._dfs['plants_hydro_ferc1']

    def plants_pumped_storage_ferc1(self, update=False):
        """
        Pull the FERC Form 1 Pumped Storage Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_pumped_storage_ferc1'] is None:
            self._dfs['plants_pumped_storage_ferc1'] = pudl.output.ferc1.plants_pumped_storage_ferc1(
                self.pudl_engine)
        return self._dfs['plants_pumped_storage_ferc1']

    def purchased_power_ferc1(self, update=False):
        """
        Pull the FERC Form 1 Purchased Power Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['purchased_power_ferc1'] is None:
            self._dfs['purchased_power_ferc1'] = pudl.output.ferc1.purchased_power_ferc1(
                self.pudl_engine)
        return self._dfs['purchased_power_ferc1']

    def plant_in_service_ferc1(self, update=False):
        """
        Pull the FERC Form 1 Plant in Service Table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['plant_in_service_ferc1'] is None:
            self._dfs['plant_in_service_ferc1'] = pudl.output.ferc1.plant_in_service_ferc1(
                self.pudl_engine)
        return self._dfs['plant_in_service_ferc1']

    def all_plants_ferc1(self, update=False):
        """
        Pull the FERC Form 1 all plants table.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['all_plants_ferc1'] is None:
            self._dfs['all_plants_ferc1'] = pudl.output.ferc1.all_plants_ferc1(
                self.pudl_engine)
        return self._dfs['all_plants_ferc1']

    ###########################################################################
    # EIA MCOE OUTPUTS
    ###########################################################################
    def hr_by_gen(self, update=False):
        """
        Calculate and return generator level heat rates (mmBTU/MWh).

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['hr_by_gen'] is None:
            self._dfs['hr_by_gen'] = pudl.analysis.mcoe.heat_rate_by_gen(self)
        return self._dfs['hr_by_gen']

    def hr_by_unit(self, update=False):
        """
        Calculate and return generation unit level heat rates.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['hr_by_unit'] is None:
            self._dfs['hr_by_unit'] = (
                pudl.analysis.mcoe.heat_rate_by_unit(self)
            )
        return self._dfs['hr_by_unit']

    def fuel_cost(self, update=False):
        """
        Calculate and return generator level fuel costs per MWh.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['fuel_cost'] is None:
            self._dfs['fuel_cost'] = pudl.analysis.mcoe.fuel_cost(self)
        return self._dfs['fuel_cost']

    def capacity_factor(self, update=False, min_cap_fact=None, max_cap_fact=None):
        """
        Calculate and return generator level capacity factors.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['capacity_factor'] is None:
            self._dfs['capacity_factor'] = (
                pudl.analysis.mcoe.capacity_factor(
                    self, min_cap_fact=min_cap_fact, max_cap_fact=max_cap_fact)
            )
        return self._dfs['capacity_factor']

    def mcoe(
        self,
        update=False,
        min_heat_rate=5.5,
        min_fuel_cost_per_mwh=0.0,
        min_cap_fact=0.0,
        max_cap_fact=1.5,
        all_gens=True,
    ):
        """
        Calculate and return generator level MCOE based on EIA data.

        Eventually this calculation will include non-fuel operating expenses
        as reported in FERC Form 1, but for now only the fuel costs reported
        to EIA are included. They are attibuted based on the unit-level heat
        rates and fuel costs.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
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
            all_gens (bool): Controls whether the output contains records for
                all generators in the :ref:`generators_eia860` table, or only
                those generators with associated MCOE data. True by default.

        Returns:
            :class:`pandas.DataFrame`: a compilation of generator attributes,
            including fuel costs per MWh.

        """
        if update or self._dfs['mcoe'] is None:
            self._dfs['mcoe'] = pudl.analysis.mcoe.mcoe(
                self,
                min_heat_rate=min_heat_rate,
                min_fuel_cost_per_mwh=min_fuel_cost_per_mwh,
                min_cap_fact=min_cap_fact,
                max_cap_fact=max_cap_fact,
                all_gens=all_gens,
            )
        return self._dfs['mcoe']

    def gens_mega_eia(self, update=False):
        """
        Generate and return a generators table with ownership integrated.

        Args:
            update (boolean): If true, re-calculate the output dataframe, even
                if a cached version exists. Defualt is `False`.

        Returns:
            pandas.DataFrame: a table of all of the generators with identifying
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
        if update or self._dfs['gens_mega_eia'] is None:
            if self.freq != 'AS':
                raise AssertionError(
                    "The frequency of the pudl_out object must be `AS` for the "
                    f"plant-parts table and we got {self.freq}"
                )
            self._dfs['gens_mega_eia'] = (
                pudl.analysis.plant_parts_eia.MakeMegaGenTbl().execute(
                    mcoe=self.mcoe(all_gens=True),
                    own_eia860=self.own_eia860()
                )
            )
        return self._dfs['gens_mega_eia']

    def plant_parts_eia(self, update=False, update_gens_mega=False, update_true_gran=False):
        """
        Generate and return master plant-parts EIA.

        Args:
            update (boolean): If true, re-calculate the output dataframe, even
                if a cached version exists. Defualt is `False`.
            deep_update (boolean): If True, re-calculate both the output
                dataframe and its inputs. Defualt is `False`.
        """
        # generate the true_gran table
        # the true_gran table is really not helpful on it's own
        if update_true_gran or self._dfs['true_grans_eia'] is None:
            self._dfs['true_grans_eia'] = (
                pudl.analysis.plant_parts_eia.LabelTrueGranularities()
                .execute(self.gens_mega_eia())
            )

        update_any = any([update, update_gens_mega, update_true_gran])
        if update_any or self._dfs['plant_parts_eia'] is None:
            # make the plant-parts objects
            self.parts_compiler = pudl.analysis.plant_parts_eia.MakePlantParts(
                self)
            # make the plant-parts df!
            self._dfs['plant_parts_eia'] = self.parts_compiler.execute(
                gens_mega=self.gens_mega_eia(update=update_gens_mega),
                true_grans=self._dfs['true_grans_eia']
            )

        return self._dfs['plant_parts_eia']


def get_table_meta(pudl_engine):
    """Grab the pudl sqlitie database table metadata."""
    md = sa.MetaData()
    md.reflect(pudl_engine)
    return md.tables
