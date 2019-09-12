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
generator and plat level marginal cost of electricity (MCOE), which are defined
in the analysis module.

In the long run, this is a probably a kind of prototype for pre-packaged API
outputs or data products that we might want to be able to provide to users a la
carte.

Todo:
    Return to for update arg and returns values in functions below

"""

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
import pudl.constants as pc

###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl(object):
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    def __init__(self, freq=None, start_date=None, end_date=None,
                 pudl_engine=None):
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

        """
        self.pudl_engine = pudl_engine
        self.freq = freq

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
        self.pudl_engine = pudl_engine

        self.pt = get_table_meta(self.pudl_engine)
        # We populate this library of dataframes as they are generated, and
        # allow them to persist, in case they need to be used again.
        self._dfs = {
            'pu_eia': None,
            'pu_ferc1': None,

            'utils_eia860': None,
            'bga_eia860': None,
            'plants_eia860': None,
            'gens_eia860': None,
            'own_eia860': None,

            'gf_eia923': None,
            'frc_eia923': None,
            'bf_eia923': None,
            'gen_eia923': None,

            'plants_steam_ferc1': None,
            'fuel_ferc1': None,
            'fbp_ferc1': None,

            'bga': None,
            'hr_by_unit': None,
            'hr_by_gen': None,
            'fuel_cost': None,
            'capacity_factor': None,
            'mcoe': None,
        }

    def pu_eia860(self, update=False):
        """
        Pull a dataframe of EIA plant-utility associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['pu_eia'] is None:
            self._dfs['pu_eia'] = pudl.output.eia860.plants_utils_eia860(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['pu_ferc1'] is None:
            self._dfs['pu_ferc1'] = pudl.output.ferc1.plants_utils_ferc1(
                self.pudl_engine, self.pt)
        return self._dfs['pu_ferc1']

    def utils_eia860(self, update=False):
        """
        Pull a dataframe describing utilities reported in EIA 860.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['utils_eia860'] is None:
            self._dfs['utils_eia860'] = pudl.output.eia860.utilities_eia860(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['bga_eia860'] is None:
            self._dfs['bga_eia860'] = \
                pudl.output.eia860.boiler_generator_assn_eia860(
                    self.pudl_engine,
                    self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_eia860'] is None:
            self._dfs['plants_eia860'] = pudl.output.eia860.plants_eia860(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['gens_eia860'] is None:
            self._dfs['gens_eia860'] = pudl.output.eia860.generators_eia860(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['own_eia860'] is None:
            self._dfs['own_eia860'] = pudl.output.eia860.ownership_eia860(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['gf_eia923'] is None:
            self._dfs['gf_eia923'] = \
                pudl.output.eia923.generation_fuel_eia923(
                    self.pudl_engine,
                    self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['frc_eia923'] is None:
            self._dfs['frc_eia923'] = \
                pudl.output.eia923.fuel_receipts_costs_eia923(
                    self.pudl_engine,
                    self.pt,
                    freq=self.freq,
                    start_date=self.start_date,
                    end_date=self.end_date)
        return self._dfs['frc_eia923']

    def bf_eia923(self, update=False):
        """
        Pull EIA 923 boiler fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['bf_eia923'] is None:
            self._dfs['bf_eia923'] = pudl.output.eia923.boiler_fuel_eia923(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['gen_eia923'] is None:
            self._dfs['gen_eia923'] = pudl.output.eia923.generation_eia923(
                self.pudl_engine,
                self.pt,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date)
        return self._dfs['gen_eia923']

    def plants_steam_ferc1(self, update=False):
        """
        Pull the FERC Form 1 steam plants data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['plants_steam_ferc1'] is None:
            self._dfs['plants_steam_ferc1'] = \
                pudl.output.ferc1.plants_steam_ferc1(self.pudl_engine, self.pt)
        return self._dfs['plants_steam_ferc1']

    def fuel_ferc1(self, update=False):
        """
        Pull the FERC Form 1 steam plants fuel consumption data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['fuel_ferc1'] is None:
            self._dfs['fuel_ferc1'] = pudl.output.ferc1.fuel_ferc1(
                self.pudl_engine, self.pt)
        return self._dfs['fuel_ferc1']

    def fbp_ferc1(self, update=False):
        """
        Summarize FERC Form 1 fuel usage by plant.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['fbp_ferc1'] is None:
            self._dfs['fbp_ferc1'] = pudl.output.ferc1.fuel_by_plant_ferc1(
                self.pudl_engine, self.pt)
        return self._dfs['fbp_ferc1']

    def bga(self, update=False):
        """
        Pull the more complete EIA/PUDL boiler-generator associations.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

        """
        if update or self._dfs['bga'] is None:
            self._dfs['bga'] = pudl.output.glue.boiler_generator_assn(
                self.pudl_engine,
                self.pt,
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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

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
            :mod:`pandas.DataFrame`: a denormalized table for interactive use.

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
    pt = md.tables
    return(pt)
