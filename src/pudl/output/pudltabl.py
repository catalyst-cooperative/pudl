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
import pathlib

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl(object):
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    def __init__(self, pudl_engine, ds=None, freq=None, start_date=None, end_date=None,
                 fill=False, roll=False):
        """
        Initialize the PUDL output object.

        Private data members are not initialized until they are requested.
        They are then cached within the object unless they get re-initialized
        via a method that includes update=True.

        Some methods (e.g mcoe) will take a while to run, since they need to
        pull substantial data and do a bunch of calculations.

        Args:
            freq (str): String describing time frequency at which to aggregate
                the reported data. E.g. 'MS' (monthly start).
            start_date (date): Beginning date for data to pull from the
                PUDL DB.
            end_date (date): End date for data to pull from the PUDL DB.
            pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
                for the PUDL DB.
            roll (boolean): if set to True, apply a rolling average to a
                subset of output table's columns (currently only
                'fuel_cost_per_mmbtu' for the frc table).

        """
        self.pudl_engine = pudl_engine
        self.freq = freq
        # We need datastore access because some data is not yet integrated into the
        # PUDL DB. See the etl_eia861 method.
        self.ds = ds

        if start_date is None:
            self.start_date = \
                pd.to_datetime(
                    '{}-01-01'.format(min(pc.working_years['eia923'])))
        else:
            # Make sure it's a date... and not a string.
            self.start_date = pd.to_datetime(start_date)

        if end_date is None:
            self.end_date = \
                pd.to_datetime(
                    '{}-12-31'.format(max(pc.working_years['eia923'])))
        else:
            # Make sure it's a date... and not a string.
            self.end_date = pd.to_datetime(end_date)

        if not pudl_engine:
            raise AssertionError('PudlTabl object needs a pudl_engine')

        self.roll = roll
        self.fill = fill
        # We populate this library of dataframes as they are generated, and
        # allow them to persist, in case they need to be used again.
        self._dfs = {
            "pu_eia": None,
            "pu_ferc1": None,

            "utils_eia860": None,
            "bga_eia860": None,
            "plants_eia860": None,
            "gens_eia860": None,
            "own_eia860": None,

            # TODO add the other tables -- this is just an interim check
            "balancing_authority_eia861": None,

            # TODO add the other tables -- this is just an interim check
            "respondent_id_ferc714": None,

            "gf_eia923": None,
            "frc_eia923": None,
            "bf_eia923": None,
            "gen_eia923": None,

            "plants_steam_ferc1": None,
            "fuel_ferc1": None,
            "fbp_ferc1": None,
            "plants_small_ferc1": None,
            "plants_hydro_ferc1": None,
            "plants_pumped_storage_ferc1": None,
            "purchased_power_ferc1": None,
            "plant_in_service_ferc1": None,

            "bga": None,
            "hr_by_unit": None,
            "hr_by_gen": None,
            "fuel_cost": None,
            "capacity_factor": None,
            "mcoe": None,
        }

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
    def etl_eia861(self, update=False):
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
            update (bool): Whether to overwrite the existing dataframes if they exist.

        """
        if update or self._dfs["balancing_authority_eia861"] is None:
            logger.warning("Running the interim EIA 861 ETL process! (~2 minutes)")

            if self.ds is None:
                pudl_in = pathlib.Path(pudl.workspace.setup.get_defaults()["pudl_in"])
                self.ds = pudl.workspace.datastore.Datastore(
                    pudl_in=pudl_in,
                    sandbox=True,
                )

            eia861_raw_dfs = (
                pudl.extract.eia861.Extractor(self.ds)
                .extract(pc.working_years["eia861"])
            )
            eia861_tfr_dfs = pudl.transform.eia861.transform(eia861_raw_dfs)
            for table in eia861_tfr_dfs:
                self._dfs[table] = eia861_tfr_dfs[table]

    def balancing_authority_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["balancing_authority_eia861"]

    def balancing_authority_assn_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["balancing_authority_assn_eia861"]

    def utility_assn_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["utility_assn_eia861"]

    def service_territory_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["service_territory_eia861"]

    def sales_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["sales_eia861"]

    def advanced_metering_infrastructure_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["advanced_metering_infrastructure_eia861"]

    def distribution_systems_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["distribution_systems_eia861"]

    def green_pricing_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["green_pricing_eia861"]

    def dynamic_pricing_eia861(self, update=False):
        """An interim EIA 861 output function."""
        self.etl_eia861(update=update)
        return self._dfs["dynamic_pricing_eia861"]

    ###########################################################################
    # FERC 714 Interim Outputs (awaiting full DB integration)
    ###########################################################################
    def etl_ferc714(self, update=False):
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
            update (bool): Whether to overwrite the existing dataframes if they exist.

        """
        if update or self._dfs["respondent_id_ferc714"] is None:
            logger.warning("Running the interim FERC 714 ETL process! (~11 minutes)")
            ferc714_raw_dfs = pudl.extract.ferc714.extract()
            ferc714_tfr_dfs = pudl.transform.ferc714.transform(ferc714_raw_dfs)
            for table in ferc714_tfr_dfs:
                self._dfs[table] = ferc714_tfr_dfs[table]

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
                end_date=self.end_date)
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

    def gf_eia923(self, update=False):
        """
        Pull EIA 923 generation and fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['gf_eia923'] is None:
            self._dfs['gf_eia923'] = pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['gf_eia923']

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
                fill=self.fill,
                roll=self.roll)
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

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['gen_eia923'] is None:
            self._dfs['gen_eia923'] = pudl.output.eia923.generation_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['gen_eia923']

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

    ###########################################################################
    # EIA MCOE OUTPUTS
    ###########################################################################
    def bga(self, update=False):
        """
        Pull the more complete EIA/PUDL boiler-generator associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['bga'] is None:
            self._dfs['bga'] = pudl.output.glue.boiler_generator_assn(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['bga']

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
            self._dfs['hr_by_gen'] = pudl.analysis.mcoe.heat_rate_by_gen(
                self)
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
            self._dfs['hr_by_unit'] = pudl.analysis.mcoe.heat_rate_by_unit(
                self)
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

    def capacity_factor(self, update=False,
                        min_cap_fact=None, max_cap_fact=None):
        """
        Calculate and return generator level capacity factors.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if update or self._dfs['capacity_factor'] is None:
            self._dfs['capacity_factor'] = pudl.analysis.mcoe.capacity_factor(
                self, min_cap_fact=min_cap_fact, max_cap_fact=max_cap_fact)
        return self._dfs['capacity_factor']

    def mcoe(self, update=False,
             min_heat_rate=5.5, min_fuel_cost_per_mwh=0.0,
             min_cap_fact=0.0, max_cap_fact=1.5):
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
                max_cap_fact=max_cap_fact)
        return self._dfs['mcoe']


def get_table_meta(pudl_engine):
    """Grab the pudl sqlitie database table metadata."""
    md = sa.MetaData()
    md.reflect(pudl_engine)
    return md.tables
