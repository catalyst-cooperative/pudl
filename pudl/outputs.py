"""
A library of useful tabular outputs compiled from multiple data sources.

Many of our potential users are comfortable using spreadsheets, not databases,
so we are creating a collection of tabular outputs that containt the most
useful core information from the PUDL DB, including additional keys and human
readable names for the objects (utilities, plants, generators) being described
in the table.

These tabular outputs can be joined with each other using those keys, and used
as a data source within Microsoft Excel, Access, R Studio, or other data
analysis packages that folks may be familiar with.  They aren't meant to
completely replicate all the data and relationships contained within the full
PUDL database, but should serve as a generally usable set of data products
that contain some calculated values, and allow a variety of interesting
questions to be addressed (e.g. about the marginal cost of electricity on a
generator by generatory basis).

Over time this library will hopefully grow and acccumulate more data and more
post-processing, post-analysis outputs as well.
"""

# Useful high-level external modules.
import sqlalchemy as sa
import pandas as pd
import numpy as np

# Need the models so we can grab table structures. Need some helpers from the
# analysis module
from pudl import analysis, init, mcoe
from pudl import constants as pc
import pudl.models.entities
import pudl.models.glue
import pudl.models.eia
import pudl.models.eia923
import pudl.models.eia860
import pudl.models.ferc1
# Shorthand for easier table referecnes:
pt = pudl.models.entities.PUDLBase.metadata.tables

###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlOutput(object):
    """A class that obtains tabular outputs from the PUDL DB."""

    def __init__(self, freq=None, testing=False,
                 start_date=None, end_date=None):
        self.freq = freq
        self.testing = testing

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

            'bga': None,
            'hr_by_unit': None,
            'hr_by_gen': None,
            'fc': None,
            'cf': None,
            'mcoe': None,
        }

    def plants_utilities_eia(self, update=False):
        if update or self._dfs['pu_eia'] is None:
            self._dfs['pu_eia'] = \
                plants_utils_eia(start_date=self.start_date,
                                 end_date=self.end_date,
                                 testing=self.testing)
        return(self._dfs['pu_eia'])

    def pu_eia(self, update=False):
        return(self.plants_utilities_eia(update=update))

    def plants_utilities_ferc1(self, update=False):
        if update or self._dfs['pu_ferc1'] is None:
            self._dfs['pu_ferc1'] = plants_utils_ferc1(testing=self.testing)
        return(self._dfs['pu_ferc1'])

    def pu_ferc1(self, update=False):
        return(self.plants_utilities_ferc1(update=update))

    def utilities_eia860(self, update=False):
        if update or self._dfs['utils_eia860'] is None:
            self._dfs['utils_eia860'] = \
                utilities_eia860(start_date=self.start_date,
                                 end_date=self.end_date,
                                 testing=self.testing)
        return(self._dfs['utils_eia860'])

    def utils_eia860(self, update=False):
        return(self.utilities_eia860(update=update))

    def boiler_generator_assn_eia860(self, update=False):
        if update or self._dfs['bga_eia860'] is None:
            self._dfs['bga_eia860'] = \
                boiler_generator_assn_eia860(start_date=self.start_date,
                                             end_date=self.end_date,
                                             testing=self.testing)
        return(self._dfs['bga_eia860'])

    def bga_eia860(self, update=False):
        return(self.boiler_generator_assn_eia860(update=update))

    def plants_eia860(self, update=False):
        if update or self._dfs['plants_eia860'] is None:
            self._dfs['plants_eia860'] = \
                plants_eia860(start_date=self.start_date,
                              end_date=self.end_date,
                              testing=self.testing)
        return(self._dfs['plants_eia860'])

    def generators_eia860(self, update=False):
        if update or self._dfs['gens_eia860'] is None:
            self._dfs['gens_eia860'] = \
                generators_eia860(start_date=self.start_date,
                                  end_date=self.end_date,
                                  testing=self.testing)
        return(self._dfs['gens_eia860'])

    def gens_eia860(self, update=False):
        return(self.generators_eia860(update=update))

    def ownership_eia860(self, update=False):
        if update or self._dfs['own_eia860'] is None:
            self._dfs['own_eia860'] = \
                ownership_eia860(start_date=self.start_date,
                                 end_date=self.end_date,
                                 testing=self.testing)
        return(self._dfs['own_eia860'])

    def own_eia860(self, update=False):
        return(self.ownership_eia860(update=update))

    def generation_fuel_eia923(self, update=False):
        if update or self._dfs['gf_eia923'] is None:
            self._dfs['gf_eia923'] = \
                generation_fuel_eia923(freq=self.freq,
                                       start_date=self.start_date,
                                       end_date=self.end_date,
                                       testing=self.testing)
        return(self._dfs['gf_eia923'])

    def gf_eia923(self, update=False):
        return(self.generation_fuel_eia923(update=update))

    def fuel_receipts_costs_eia923(self, update=False):
        if update or self._dfs['frc_eia923'] is None:
            self._dfs['frc_eia923'] = \
                fuel_receipts_costs_eia923(freq=self.freq,
                                           start_date=self.start_date,
                                           end_date=self.end_date,
                                           testing=self.testing)
        return(self._dfs['frc_eia923'])

    def frc_eia923(self, update=False):
        return(self.fuel_receipts_costs_eia923(update=update))

    def boiler_fuel_eia923(self, update=False):
        if update or self._dfs['bf_eia923'] is None:
            self._dfs['bf_eia923'] = \
                boiler_fuel_eia923(freq=self.freq,
                                   start_date=self.start_date,
                                   end_date=self.end_date,
                                   testing=self.testing)
        return(self._dfs['bf_eia923'])

    def bf_eia923(self, update=False):
        return(self.boiler_fuel_eia923(update=update))

    def generation_eia923(self, update=False):
        if update or self._dfs['gen_eia923'] is None:
            self._dfs['gen_eia923'] = \
                generation_eia923(freq=self.freq,
                                  start_date=self.start_date,
                                  end_date=self.end_date,
                                  testing=self.testing)
        return(self._dfs['gen_eia923'])

    def gen_eia923(self, update=False):
        return(self.generation_eia923(update=update))

    def plants_steam_ferc1(self, update=False):
        if update or self._dfs['plants_steam_ferc1'] is None:
            self._dfs['plants_steam_ferc1'] = \
                plants_steam_ferc1(testing=self.testing)
        return(self._dfs['plants_steam_ferc1'])

    def fuel_ferc1(self, update=False):
        if update or self._dfs['fuel_ferc1'] is None:
            self._dfs['fuel_ferc1'] = fuel_ferc1(testing=self.testing)
        return(self._dfs['fuel_ferc1'])

    def boiler_generator_assn_eia(self, update=False, verbose=False):
        if update or self._dfs['bga'] is None:
            self._dfs['bga'] = \
                boiler_generator_assn_eia(start_date=self.start_date,
                                          end_date=self.end_date,
                                          testing=self.testing)
        return(self._dfs['bga'])

    def bga(self, update=False, verbose=False):
        return(self.boiler_generator_assn_eia(update=update))

    def heat_rate_by_unit(self, update=False, verbose=False):
        if update or self._dfs['hr_by_unit'] is None:
            self._dfs['hr_by_unit'] = mcoe.heat_rate_by_unit(self,
                                                             verbose=verbose)
        return(self._dfs['hr_by_unit'])

    def heat_rate_by_gen(self, update=False, verbose=False):
        if update or self._dfs['hr_by_gen'] is None:
            self._dfs['hr_by_gen'] = mcoe.heat_rate_by_gen(self,
                                                           verbose=verbose)
        return(self._dfs['hr_by_gen'])

    def fuel_cost(self, update=False, verbose=False):
        if update or self._dfs['fc'] is None:
            self._dfs['fc'] = mcoe.fuel_cost(self, verbose=verbose)
        return(self._dfs['fc'])

    def capacity_factor(self, update=False, verbose=False):
        if update or self._dfs['cf'] is None:
            self._dfs['cf'] = mcoe.capacity_factor(self, verbose=verbose)
        return(self._dfs['cf'])

    def mcoe(self, update=False,
             min_heat_rate=5.5, min_fuel_cost_per_mwh=0.0,
             min_cap_fact=0.0, max_cap_fact=1.5, verbose=False):
        if update or self._dfs['mcoe'] is None:
            self._dfs['mcoe'] = \
                mcoe.mcoe(self, verbose=verbose,
                          min_heat_rate=min_heat_rate,
                          min_fuel_cost_per_mwh=min_fuel_cost_per_mwh,
                          min_cap_fact=min_cap_fact,
                          max_cap_fact=max_cap_fact)
        return(self._dfs['mcoe'])

###############################################################################
###############################################################################
#   Output Helper Functions
###############################################################################
###############################################################################


def organize_cols(df, cols):
    """
    Organize columns into key ID & name fields & alphabetical data columns.

    For readability, it's nice to group a few key columns at the beginning
    of the dataframe (e.g. report_year or report_date, plant_id...) and then
    put all the rest of the data columns in alphabetical order.

    Args:
        df: The DataFrame to be re-organized.
        cols: The columns to put first, in their desired output ordering.
    """
    # Generate a list of all the columns in the dataframe that are not
    # included in cols
    data_cols = [c for c in df.columns.tolist() if c not in cols]
    data_cols.sort()
    organized_cols = cols + data_cols
    return(df[organized_cols])


def extend_annual(df, date_col='report_date', start_date=None, end_date=None):
    """
    Extend the time range in a dataframe by duplicating first and last years.

    Takes the earliest year's worth of annual data and uses it to create
    earlier years by duplicating it, and changing the year.  Similarly,
    extends a dataset into the future by duplicating the last year's records.

    This is primarily used to extend the EIA860 data about utilities, plants,
    and generators, so that we can analyze a larger set of EIA923 data. EIA923
    data has been integrated a bit further back, and the EIA860 data has a year
    long lag in being released.
    """
    # assert that df time resolution really is annual
    assert analysis.is_annual(df, year_col=date_col)

    earliest_date = pd.to_datetime(df[date_col].min())
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        while start_date < earliest_date:
            prev_year = \
                df[pd.to_datetime(df[date_col]) == earliest_date].copy()
            prev_year[date_col] = earliest_date - pd.DateOffset(years=1)
            df = df.append(prev_year)
            df[date_col] = pd.to_datetime(df[date_col])
            earliest_date = pd.to_datetime(df[date_col].min())

    latest_date = pd.to_datetime(df[date_col].max())
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        while end_date >= latest_date + pd.DateOffset(years=1):
            next_year = df[pd.to_datetime(df[date_col]) == latest_date].copy()
            next_year[date_col] = latest_date + pd.DateOffset(years=1)
            df = df.append(next_year)
            df[date_col] = pd.to_datetime(df[date_col])
            latest_date = pd.to_datetime(df[date_col].max())

    df[date_col] = pd.to_datetime(df[date_col])
    return(df)


###############################################################################
###############################################################################
#   Cross datasource output (e.g. EIA923 + EIA860, PUDL specific IDs)
###############################################################################
###############################################################################
def plants_utils_eia(start_date=None, end_date=None, testing=False):
    """
    Create a dataframe of plant and utility IDs and names from EIA.

    Returns a pandas dataframe with the following columns:
    - report_date (in which data was reported)
    - plant_name (from EIA860)
    - plant_id_eia (from EIA860)
    - plant_id_pudl
    - operator_id (from EIA860)
    - operator_name (from EIA860)
    - util_id_pudl

    Issues:
        - EIA 860 data has only been integrated for 2011-2015. Function needs
          to take start_date & end_date and synthesize the earlier and later
          years if need be.
    """
    pudl_engine = init.connect_db(testing=testing)
    # Contains the one-to-one mapping of EIA plants to their operators, but
    # we only have the 860 data integrated for 2011 forward right now.
    plants_eia = plants_eia860(start_date=start_date,
                               end_date=end_date,
                               testing=testing)
    utils_eia = utilities_eia860(start_date=start_date,
                                 end_date=end_date,
                                 testing=testing)
    # to avoid duplicate columns on the merge...
    plants_eia = plants_eia.drop(['util_id_pudl', 'operator_name'], axis=1)
    out_df = pd.merge(plants_eia, utils_eia,
                      how='left', on=['report_date', 'operator_id'])

    cols_to_keep = ['report_date',
                    'plant_id_eia',
                    'plant_name',
                    'plant_id_pudl',
                    'operator_id',
                    'operator_name',
                    'util_id_pudl']

    out_df = out_df[cols_to_keep]
    out_df = out_df.dropna()
    out_df.plant_id_pudl = out_df.plant_id_pudl.astype(int)
    out_df.util_id_pudl = out_df.util_id_pudl.astype(int)
    return(out_df)


def plants_utils_ferc1(testing=False):
    """Build a dataframe of useful FERC Plant & Utility information."""
    pudl_engine = init.connect_db(testing=testing)

    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='respondent_id')
    return(out_df)


def annotate_export(df, notes_dict, tags_dict, first_cols, sheet_name,
                    xlsx_writer):
    """
    Create annotation tab and header rows for EIA 860, EIA 923, and FERC 1
    fields in a dataframe. This is done using an Excel Writer object, which
    must be created and saved outside the function, thereby allowing multiple
    sheets and associated annotations to be compiled in the same Excel file

    Args:
        df: The dataframe for which annotations are being created
        notes_dict: dictionary with column names as keys and long annotations
            as values
        tags_dict: dictionary of dictionaries with tag categories as keys for
            outer dictionary and values are dictionaries with column names as
            keys and values are tag within the tag category
        first_cols: ordered list of columns that should come first in outfile
        sheet_name: name of data sheet in output spreadsheet
        xlsx_writer: this is an ExcelWriter object used to accumulate multiple
            tabs, which must be created outside of function, before calling the
            first time e.g. "xlsx_writer = pd.ExcelWriter('outfile.xlsx')"

    Returns xlsx_writer which must be called outside the function, after final
        use of function, for written out to excel: "xlsx_writer.save()"
    """
    # Make sure columns we want to be first are kept there
    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
        'boiler_id',
        'ownership_id',
        'owner_name']
    data_cols = [c for c in df.columns.tolist()]
    cols = []
    for c in first_cols:
        if c in data_cols:
            cols.append(c)
    df = organize_cols(df, cols)
    # Transpose the original dataframe to easily add and map tags as columns
    dfnew = df.transpose()
    # For loop where tag is metadata field (e.g. data_source or data_origin) &
    # column is a nested dictionary of column name & value; maps tags_dict to
    # columns in df and creates a new column for each tag category
    for tag, column_dict in tags_dict.items():
        for key in column_dict.keys():  # key is column name
            dfnew[tag] = dfnew.index.to_series().map(column_dict)
    # Take the new columns that were created for each tag category and add them
    # to the index
    for tag, column_dict in tags_dict.items():
        dfnew = dfnew.set_index([tag], append=True)
    # Transpose to return data fields to columns
    dfnew = dfnew.transpose()
    # Create an excel sheet for the data frame
    dfnew.to_excel(xlsx_writer, sheet_name=str(sheet_name),
                   index=True, na_rep='NA')
    # Convert notes dictionary into a pandas series
    notes = pd.Series(notes_dict, name='annotations')
    # Create an excel sheet of the notes_dict
    notes.to_excel(xlsx_writer, sheet_name=str(sheet_name) + '_annotations',
                   index=False, na_rep='NA')
    # Return the xlsx_writer object, which can be written out, outside of
    # fucntion, with 'xlsx_writer.save()'
    return xlsx_writer


###############################################################################
###############################################################################
#   EIA 860 Outputs
###############################################################################
###############################################################################


def utilities_eia860(start_date=None, end_date=None, testing=False):
    """Pull all fields from the EIA860 Utilities table."""
    pudl_engine = init.connect_db(testing=testing)
    utils_eia860_tbl = pt['utilities_eia860']
    utils_eia860_select = sa.sql.select([utils_eia860_tbl])

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        utils_eia860_select = utils_eia860_select.where(
            utils_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        utils_eia860_select = utils_eia860_select.where(
            utils_eia860_tbl.c.report_date <= end_date
        )
    utils_eia860_df = pd.read_sql(utils_eia860_select, pudl_engine)

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.operator_id,
        utils_eia_tbl.c.util_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select,  pudl_engine)

    out_df = pd.merge(utils_eia860_df, utils_eia_df,
                      how='left', on=['operator_id', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_date',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ]

    out_df = organize_cols(out_df, first_cols)
    out_df = extend_annual(out_df, start_date=start_date, end_date=end_date)
    return(out_df)


def boiler_generator_assn_eia(start_date=None, end_date=None,
                              testing=False):
    pudl_engine = init.connect_db(testing=testing)
    bga_eia_tbl = pt['boiler_generator_assn_eia']
    bga_eia_select = sa.sql.select([bga_eia_tbl])

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        bga_eia_select = bga_eia_select.where(
            bga_eia_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        bga_eia_select = bga_eia_select.where(
            bga_eia_tbl.c.report_date <= end_date
        )
    bga_eia_df = pd.read_sql(bga_eia_select, pudl_engine)
    out_df = extend_annual(bga_eia_df,
                           start_date=start_date, end_date=end_date)
    return(out_df)


def boiler_generator_assn_eia860(start_date=None, end_date=None,
                                 testing=False):
    pudl_engine = init.connect_db(testing=testing)
    bga_eia860_tbl = pt['boiler_generator_assn_eia860']
    bga_eia860_select = sa.sql.select([bga_eia860_tbl])

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        bga_eia860_select = bga_eia860_select.where(
            bga_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        bga_eia860_select = bga_eia860_select.where(
            bga_eia860_tbl.c.report_date <= end_date
        )
    bga_eia860_df = pd.read_sql(bga_eia860_select, pudl_engine)
    out_df = extend_annual(bga_eia860_df,
                           start_date=start_date, end_date=end_date)
    return(out_df)


def plants_eia860(start_date=None, end_date=None, testing=False):
    """Pull all fields from the EIA860 Plants table."""
    pudl_engine = init.connect_db(testing=testing)
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([plants_eia860_tbl])
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date <= end_date
        )
    plants_eia860_df = pd.read_sql(plants_eia860_select, pudl_engine)

    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([
        plants_eia_tbl.c.plant_id_eia,
        plants_eia_tbl.c.plant_id_pudl,
    ])
    plants_eia_df = pd.read_sql(plants_eia_select,  pudl_engine)

    out_df = pd.merge(plants_eia860_df, plants_eia_df,
                      how='left', on=['plant_id_eia', ])

    utils_eia_tbl = pt['utilities_eia']
    utils_eia_select = sa.sql.select([
        utils_eia_tbl.c.operator_id,
        utils_eia_tbl.c.util_id_pudl,
    ])
    utils_eia_df = pd.read_sql(utils_eia_select,  pudl_engine)

    out_df = pd.merge(out_df, utils_eia_df,
                      how='left', on=['operator_id', ])

    out_df = out_df.drop(['id'], axis=1)
    first_cols = [
        'report_date',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
    ]

    out_df = organize_cols(out_df, first_cols)
    out_df = extend_annual(out_df, start_date=start_date, end_date=end_date)
    return(out_df)


def generators_eia860(start_date=None, end_date=None, testing=False):
    """
    Pull all fields reported in the generators_eia860 table.

    Merge in other useful fields including the latitude & longitude of the
    plant that the generators are part of, canonical plant & operator names and
    the PUDL IDs of the plant and operator, for merging with other PUDL data
    sources.

    Fill in data for adjacent years if requested, but never fill in earlier
    than the earliest working year of data for EIA923, and never add more than
    one year on after the reported data (since there should at most be a one
    year lag between EIA923 and EIA860 reporting)

    Args:
        start_date (date): the earliest EIA 860 data to retrieve or synthesize
        end_date (date): the latest EIA 860 data to retrieve or synthesize
        testing (bool): Connect to the live PUDL DB or the testing DB?

    Returns:
        A pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
    # Almost all the info we need will come from here.
    gens_eia860_tbl = pt['generators_eia860']
    gens_eia860_select = sa.sql.select([gens_eia860_tbl, ])
    # To get the Lat/Lon coordinates
    plants_eia860_tbl = pt['plants_eia860']
    plants_eia860_select = sa.sql.select([
        plants_eia860_tbl.c.report_date,
        plants_eia860_tbl.c.plant_id_eia,
        plants_eia860_tbl.c.latitude,
        plants_eia860_tbl.c.longitude,
        plants_eia860_tbl.c.balancing_authority_code,
        plants_eia860_tbl.c.balancing_authority_name,
        plants_eia860_tbl.c.iso_rto,
        plants_eia860_tbl.c.iso_rto_code,
    ])

    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        # We don't want to get too crazy with the date extensions...
        # start_date shouldn't go back before the earliest working year of
        # EIA 923
        eia923_start_date = \
            pd.to_datetime('{}-01-01'.format(min(pc.working_years['eia923'])))
        assert start_date >= eia923_start_date
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date >= start_date
        )
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date >= start_date
        )

    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        # end_date shouldn't be more than one year ahead of the most recent
        # year for which we have EIA 860 data:
        eia860_end_date = \
            pd.to_datetime('{}-12-31'.format(max(pc.working_years['eia860'])))
        assert end_date <= eia860_end_date + pd.DateOffset(years=1)
        gens_eia860_select = gens_eia860_select.where(
            gens_eia860_tbl.c.report_date <= end_date
        )
        plants_eia860_select = plants_eia860_select.where(
            plants_eia860_tbl.c.report_date <= end_date
        )

    gens_eia860 = pd.read_sql(gens_eia860_select, pudl_engine)
    # Canonical sources for these fields are elsewhere. We will merge them in.
    gens_eia860 = gens_eia860.drop(['operator_id',
                                    'operator_name',
                                    'plant_name'], axis=1)
    plants_eia860 = pd.read_sql(plants_eia860_select, pudl_engine)
    out_df = pd.merge(gens_eia860, plants_eia860,
                      how='left', on=['report_date', 'plant_id_eia'])
    out_df.report_date = pd.to_datetime(out_df.report_date)

    # Bring in some generic plant & utility information:
    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=testing)
    out_df = pd.merge(out_df, pu_eia, on=['report_date', 'plant_id_eia'])

    # Drop a few extraneous fields...
    cols_to_drop = ['id', ]
    out_df = out_df.drop(cols_to_drop, axis=1)

    # In order to be able to differentiate betweet single and multi-fuel
    # plants, we need to count how many different simple energy sources there
    # are associated with plant's generators. This allows us to do the simple
    # lumping of an entire plant's fuel & generation if its primary fuels
    # are homogeneous, and split out fuel & generation by fuel if it is
    # hetereogeneous.
    ft_count = out_df[['plant_id_eia', 'fuel_type_pudl', 'report_date']].\
        drop_duplicates().groupby(['plant_id_eia', 'report_date']).count()
    ft_count = ft_count.reset_index()
    ft_count = ft_count.rename(
        columns={'fuel_type_pudl': 'fuel_type_count'})
    out_df = pd.merge(out_df, ft_count, how='left',
                      on=['plant_id_eia', 'report_date'])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)
    out_df = extend_annual(out_df, start_date=start_date, end_date=end_date)
    out_df = out_df.sort_values(['report_date',
                                 'plant_id_eia',
                                 'generator_id'])

    return(out_df)


def ownership_eia860(start_date=None, end_date=None, testing=False):
    """
    Pull a useful set of fields related to ownership_eia860 table.

    Args:
        start_date (date): date of the earliest data to retrieve
        end_date (date): date of the latest data to retrieve
        testing (bool): True if we're connecting to the pudl_test DB, False
            if we're connecting to the live PUDL DB. False by default.
    Returns:
        out_df (pandas dataframe)
    """
    pudl_engine = init.connect_db(testing=testing)
    o_eia860_tbl = pt['ownership_eia860']
    o_eia860_select = sa.sql.select([o_eia860_tbl, ])
    o_df = pd.read_sql(o_eia860_select, pudl_engine)

    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=testing)
    pu_eia = pu_eia[['plant_id_eia', 'plant_id_pudl', 'util_id_pudl',
                     'report_date']]

    o_df['report_date'] = pd.to_datetime(o_df.report_date)
    out_df = pd.merge(o_df, pu_eia,
                      how='left', on=['report_date', 'plant_id_eia'])

    out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'generator_id',
        'ownership_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
        'ownership_id',
        'owner_name',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df = extend_annual(out_df, start_date=start_date, end_date=end_date)

    return(out_df)


###############################################################################
###############################################################################
#   EIA 923 Outputs
###############################################################################
###############################################################################
def generation_fuel_eia923(freq=None, testing=False,
                           start_date=None, end_date=None):
    """
    Pull records from the generation_fuel_eia923 table, in a given date range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.
     - plant_id_eia
     - report_date
     - fuel_type_pudl
     - fuel_consumed_total
     - fuel_consumed_for_electricity
     - fuel_mmbtu_per_unit
     - fuel_consumed_total_mmbtu
     - fuel_consumed_for_electricity_mmbtu
     - net_generation_mwh

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        testing (bool): True if we are connecting to the pudl_test DB, False
            if we're using the live DB.  False by default.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        gf_df: a pandas dataframe.
    """
    pudl_engine = init.connect_db(testing=testing)
    gf_tbl = pt['generation_fuel_eia923']
    gf_select = sa.sql.select([gf_tbl, ])
    if start_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date >= start_date)
    if end_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date <= end_date)

    gf_df = pd.read_sql(gf_select, pudl_engine)

    cols_to_drop = ['id']
    gf_df = gf_df.drop(cols_to_drop, axis=1)

    # fuel_type_pudl was formerly aer_fuel_category
    by = ['plant_id_eia', 'fuel_type_pudl']
    if freq is not None:
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by)
        gf_df = gf_gb.agg({
            'fuel_consumed_total': analysis.sum_na,
            'fuel_consumed_for_electricity': analysis.sum_na,
            'fuel_consumed_total_mmbtu': analysis.sum_na,
            'fuel_consumed_for_electricity_mmbtu': analysis.sum_na,
            'net_generation_mwh': analysis.sum_na,
        })
        gf_df['fuel_mmbtu_per_unit'] = \
            gf_df['fuel_consumed_total_mmbtu'] / gf_df['fuel_consumed_total']

        gf_df = gf_df.reset_index()

    # Bring in some generic plant & utility information:
    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=testing)
    out_df = analysis.merge_on_date_year(gf_df, pu_eia, on=['plant_id_eia'])
    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    out_df = organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return(out_df)


def fuel_receipts_costs_eia923(freq=None, testing=False,
                               start_date=None, end_date=None):
    """
    Pull records from fuel_receipts_costs_eia923 table, in a given date range.

    Optionally, aggregate the records at a monthly or longer timescale, as well
    as by fuel type within a plant, by setting freq to something other than
    the default None value.

    If the records are not being aggregated, then all of the fields found in
    the PUDL database are available.  If they are being aggregated, then the
    following fields are preserved, and appropriately summed or re-calculated
    based on the specified aggregation. In both cases, new total values are
    calculated, for total fuel heat content and total fuel cost.
     - plant_id_eia
     - report_date
     - fuel_type_pudl (formerly energy_source_simple)
     - fuel_quantity (sum)
     - fuel_cost_per_mmbtu (weighted average)
     - total_fuel_cost (sum)
     - total_heat_content_mmbtu (sum)
     - heat_content_mmbtu_per_unit (weighted average)
     - sulfur_content_pct (weighted average)
     - ash_content_pct (weighted average)
     - mercury_content_ppm (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB. False by default.

    Returns:
        frc_df: a pandas dataframe.
    """
    pudl_engine = init.connect_db(testing=testing)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt['fuel_receipts_costs_eia923']
    frc_select = sa.sql.select([frc_tbl, ])

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt['coalmine_eia923']
    cmi_select = sa.sql.select([cmi_tbl, ])
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    if start_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date <= end_date)

    frc_df = pd.read_sql(frc_select, pudl_engine)

    frc_df = pd.merge(frc_df, cmi_df,
                      how='left',
                      left_on='mine_id_pudl',
                      right_on='id')

    cols_to_drop = ['fuel_receipt_id', 'mine_id_pudl', 'id']
    frc_df = frc_df.drop(cols_to_drop, axis=1)

    # Calculate a few totals that are commonly needed:
    frc_df['total_heat_content_mmbtu'] = \
        frc_df['heat_content_mmbtu_per_unit'] * frc_df['fuel_quantity']
    frc_df['total_fuel_cost'] = \
        frc_df['total_heat_content_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    if freq is not None:
        by = ['plant_id_eia', 'fuel_type_pudl', pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df['total_ash_content'] = \
            frc_df['ash_content_pct'] * frc_df['fuel_quantity']
        frc_df['total_sulfur_content'] = \
            frc_df['sulfur_content_pct'] * frc_df['fuel_quantity']
        frc_df['total_mercury_content'] = \
            frc_df['mercury_content_ppm'] * frc_df['fuel_quantity']

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg({
            'fuel_quantity': analysis.sum_na,
            'total_heat_content_mmbtu': analysis.sum_na,
            'total_fuel_cost': analysis.sum_na,
            'total_sulfur_content': analysis.sum_na,
            'total_ash_content': analysis.sum_na,
            'total_mercury_content': analysis.sum_na,
        })
        frc_df['fuel_cost_per_mmbtu'] = \
            frc_df['total_fuel_cost'] / frc_df['total_heat_content_mmbtu']
        frc_df['heat_content_mmbtu_per_unit'] = \
            frc_df['total_heat_content_mmbtu'] / frc_df['fuel_quantity']
        frc_df['sulfur_content_pct'] = \
            frc_df['total_sulfur_content'] / frc_df['fuel_quantity']
        frc_df['ash_content_pct'] = \
            frc_df['total_ash_content'] / frc_df['fuel_quantity']
        frc_df['mercury_content_ppm'] = \
            frc_df['total_mercury_content'] / frc_df['fuel_quantity']
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop(['total_ash_content',
                              'total_sulfur_content',
                              'total_mercury_content'], axis=1)

    # Bring in some generic plant & utility information:
    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=testing)
    out_df = analysis.merge_on_date_year(frc_df, pu_eia, on=['plant_id_eia'])

    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=['operator_id', 'operator_name'])

    if freq is None:
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=['fuel_group'])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return(out_df)


def boiler_fuel_eia923(freq=None, testing=False,
                       start_date=None, end_date=None):
    """
    Pull records from the boiler_fuel_eia923 table, in a given data range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.
     - fuel_qty_consumed (sum)
     - fuel_mmbtu_per_unit (weighted average)
     - total_heat_content_mmbtu (sum)
     - sulfur_content_pct (weighted average)
     - ash_content_pct (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB.  False by default.

    Returns:
        bf_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
    bf_eia923_tbl = pt['boiler_fuel_eia923']
    bf_eia923_select = sa.sql.select([bf_eia923_tbl, ])
    if start_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date <= end_date
        )
    bf_df = pd.read_sql(bf_eia923_select, pudl_engine)

    # The total heat content is also useful in its own right, and we'll keep it
    # around.  Also needed to calculate average heat content per unit of fuel.
    bf_df['total_heat_content_mmbtu'] = bf_df['fuel_qty_consumed'] * \
        bf_df['fuel_mmbtu_per_unit']

    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'boiler_id', 'fuel_type_pudl']
    if freq is not None:
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        bf_df['total_sulfur_content'] = bf_df['fuel_qty_consumed'] * \
            bf_df['sulfur_content_pct']
        bf_df['total_ash_content'] = bf_df['fuel_qty_consumed'] * \
            bf_df['ash_content_pct']
        bf_df = bf_df.set_index(pd.DatetimeIndex(bf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        bf_gb = bf_df.groupby(by=by)

        # Sum up these totals within each group, and recalculate the per-unit
        # values (weighted in this case by fuel_qty_consumed)
        bf_df = bf_gb.agg({
            'total_heat_content_mmbtu': analysis.sum_na,
            'fuel_qty_consumed': analysis.sum_na,
            'total_sulfur_content': analysis.sum_na,
            'total_ash_content': analysis.sum_na,
        })

        bf_df['fuel_mmbtu_per_unit'] = \
            bf_df['total_heat_content_mmbtu'] / bf_df['fuel_qty_consumed']
        bf_df['sulfur_content_pct'] = \
            bf_df['total_sulfur_content'] / bf_df['fuel_qty_consumed']
        bf_df['ash_content_pct'] = \
            bf_df['total_ash_content'] / bf_df['fuel_qty_consumed']
        bf_df = bf_df.reset_index()
        bf_df = bf_df.drop(['total_ash_content', 'total_sulfur_content'],
                           axis=1)

    # Grab some basic plant & utility information to add.
    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=False)
    out_df = analysis.merge_on_date_year(bf_df, pu_eia, on=['plant_id_eia'])
    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'boiler_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'boiler_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


def generation_eia923(freq=None, testing=False,
                      start_date=None, end_date=None):
    """
    Sum net generation by generator at the specified frequency.

    In addition, some human readable plant and utility names, as well as some
    ID values for joining with other dataframes is added back in to the
    dataframe before it is returned.

    Args:
        pudl_engine: An SQLAlchemy DB connection engine.
        freq: A string used to specify a time grouping frequency.
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB.  False by default.
    Returns:
        out_df: a pandas dataframe.
    """
    pudl_engine = init.connect_db(testing=testing)
    g_eia923_tbl = pt['generation_eia923']
    g_eia923_select = sa.sql.select([g_eia923_tbl, ])
    if start_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date <= end_date
        )
    g_df = pd.read_sql(g_eia923_select, pudl_engine)

    # Index by date and aggregate net generation.
    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'generator_id']
    if freq is not None:
        g_df = g_df.set_index(pd.DatetimeIndex(g_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        g_gb = g_df.groupby(by=by)
        g_df = g_gb.agg({'net_generation_mwh': analysis.sum_na}).reset_index()

    # Grab EIA 860 plant and utility specific information:
    pu_eia = plants_utils_eia(start_date=start_date,
                              end_date=end_date,
                              testing=testing)

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = analysis.merge_on_date_year(g_df, pu_eia, on=['plant_id_eia'])

    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    # These ID fields are vital -- without them we don't have a complete record
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'generator_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return(out_df)


###############################################################################
###############################################################################
#   FERC Form 1 Outputs
###############################################################################
###############################################################################
def plants_steam_ferc1(testing=False):
    """
    Select and join some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Args:
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB.  False by default.

    Returns:
        steam_df: a pandas dataframe.
    """
    pudl_engine = init.connect_db(testing=testing)
    steam_ferc1_tbl = pt['plants_steam_ferc1']
    steam_ferc1_select = sa.sql.select([steam_ferc1_tbl, ])
    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(steam_df, pu_ferc, on=['respondent_id', 'plant_name'])

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = organize_cols(out_df, first_cols)

    return(out_df)


def fuel_ferc1(testing=False):
    """
    Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Also calculates the total heat content consumed for each fuel, and the
    total cost for each fuel. Total cost is calculated in two different ways,
    on the basis of fuel units consumed (e.g. tons of coal, mcf of gas) and
    on the basis of heat content consumed. In theory these should give the
    same value for total cost, but this is not always the case.

    TODO: Check whether this includes all of the fuel_ferc1 fields...

    Args:
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB.  False by default.
    Returns:
        fuel_df: a pandas dataframe.
    """
    pudl_engine = init.connect_db(testing=testing)
    fuel_ferc1_tbl = pt['fuel_ferc1']
    fuel_ferc1_select = sa.sql.select([fuel_ferc1_tbl, ])
    fuel_df = pd.read_sql(fuel_ferc1_select, pudl_engine)

    # We have two different ways of assessing the total cost of fuel given cost
    # per unit delivered and cost per mmbtu. They *should* be the same, but we
    # know they aren't always. Calculate both so we can compare both.
    fuel_df['fuel_consumed_total_mmbtu'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_avg_mmbtu_per_unit']
    fuel_df['fuel_consumed_total_cost_mmbtu'] = \
        fuel_df['fuel_cost_per_mmbtu'] * fuel_df['fuel_consumed_total_mmbtu']
    fuel_df['fuel_consumed_total_cost_unit'] = \
        fuel_df['fuel_cost_per_unit_burned'] * fuel_df['fuel_qty_burned']

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(fuel_df, pu_ferc, on=['respondent_id', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = organize_cols(out_df, first_cols)

    return(out_df)
