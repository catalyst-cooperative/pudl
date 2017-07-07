"""
Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""

import pandas as pd
import os.path
import glob
from pudl import settings
import pudl.constants as pc

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 860 data.
###########################################################################


def datadir(year):
    """
    Data directory search for EIA Form 860.

    Args:
        year (int): The year that we're trying to read data for.
    Returns:
        path to appropriate EIA 860 data directory.
    """
    # These are the only years we've got...
    assert year in range(2001, 2017)
    if(year < 2008):
        return(os.path.join(settings.EIA860_DATA_DIR,
                            'eia860{}'.format(year)))
    else:
        return(os.path.join(settings.EIA860_DATA_DIR, 'eia860{}'.format(year)))


def get_eia860_file(yr, file):
    """
    Given a year, return the appopriate EIA860 excel file.

    Args:
        year (int): The year that we're trying to read data for.
    Returns:
        path to EIA 860 spreadsheets corresponding to a given year.
    """
    assert(yr > 2010), "EIA860 file selection only works for 2010 & later."
    return(glob.glob(os.path.join(datadir(yr), file))[0])


def get_eia860_xlsx(years, file):
    """
    Read in Excel files to create Excel objects.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years: The years that we're trying to read data for.
    Returns:
        xlsx file of EIA Form 860 for input year(s)
    """
    eia860_xlsx = {}
    for yr in years:
        print("Reading EIA 860 spreadsheet data for {}.".format(yr))
        eia860_xlsx[yr] = pd.ExcelFile(get_eia860_file(yr, file))
    return(eia860_xlsx)


def get_eia860_column_map(page, year):
    """
    Given a year and EIA860 page, return info required to slurp it from Excel.

    The format of the EIA860 has changed slightly over the years, and so it
    is not completely straightforward to pull information from the spreadsheets
    into our analytical framework. This function looks up a map of the various
    tabs in the spreadsheet by year and page, and returns the information
    needed to name the data fields in a standardized way, and pull the right
    cells from each year & page into our database.

    Args:
        page (str): The string label indicating which page of the EIA860 we
            are attempting to read in. Must be one of the following:
                - 'generation_fuel'
                - 'stocks'
                - 'boiler_fuel'
                - 'generator'
                - 'fuel_receipts_costs'
                - 'plant_frame'
        year (int): The year that we're trying to read data for.

    Returns:
        sheetname (int): An integer indicating which page in the worksheet
            the data should be pulled from. 0 is the first page, 1 is the
            second page, etc. For use by pandas.read_excel()
        skiprows (int): An integer indicating how many rows should be skipped
            at the top of the sheet being read in, before the header row that
            contains the strings which will be converted into column names in
            the dataframe which is created by pandas.read_excel()
        column_map (dict): A dictionary that maps the names of the columns
            in the year being read in, to the canonical EIA923 column names
            (i.e. the column names as they are in 2014-2016). This dictionary
            will be used by DataFrame.rename(). The keys are the column names
            in the dataframe as read from older years, and the values are the
            canonmical column names.  All should be stripped of leading and
            trailing whitespace, converted to lower case, and have internal
            non-alphanumeric characters replaced with underscores.
    """
    sheetname = pc.tab_map_eia860.get_value(year, page)
    skiprows = pc.skiprows_eia860.get_value(year, page)

    page_to_df = {
        'boiler_generator_assn': pc.boiler_generator_assn_map_eia860,
        'utility': pc.utility_assn_map_eia860,
        'plant': pc.plant_assn_map_eia860,
        'generator_existing': pc.generator_assn_map_eia860,
        'generator_proposed': pc.generator_proposed_assn_map_eia860,
        'generator_retired': pc.generator_retired_assn_map_eia860,
        'ownership': pc.ownership_assn_map_eia860
    }

    d = page_to_df[page].loc[year].to_dict()

    column_map = {}
    for k, v in d.items():
        column_map[v] = k

    return((sheetname, skiprows, column_map))


def get_eia860_page(page, eia860_xlsx,
                    years=[2011, 2012, 2013, 2014, 2015],
                    verbose=True):
    """
    Read a single table from several years of EIA860 data. Return a DataFrame.

    Args:
        page (str): The string label indicating which page of the EIA860 we
        are attempting to read in. The page argument must be exactly one of the
        following strings:
            - 'boiler_generator_assn'

      years (list): The set of years to read into the dataframe.

    Returns:
        pandas.DataFrame: A dataframe containing the data from the selected
            page and selected years from EIA 860.
    """
    assert min(years) >= 2009,\
        "EIA860 works for 2009 and later. {} requested.".format(min(years))
    assert page in pc.tab_map_eia860.columns and page != 'year_index',\
        "Unrecognized EIA 860 page: {}".format(page)

    if verbose:
        print('Converting EIA 860 {} to DataFrame...'.format(page))

    df = pd.DataFrame()
    for yr in years:
        sheetname, skiprows, column_map = get_eia860_column_map(page, yr)
        newdata = pd.read_excel(eia860_xlsx[yr],
                                sheetname=sheetname,
                                skiprows=skiprows)
        # Clean column names: lowercase, underscores instead of white space,
        # no non-alphanumeric characters
        newdata.columns = newdata.columns.str.replace('[^0-9a-zA-Z]+', ' ')
        newdata.columns = newdata.columns.str.strip().str.lower()
        newdata.columns = newdata.columns.str.replace(' ', '_')

        # boiler_generator_assn tab is missing a YEAR column. Add it!
        if(page == 'boiler_generator_assn', 'utilty'):
            newdata['year'] = yr

        newdata = newdata.rename(columns=column_map)

        df = df.append(newdata)
    return(df)


def get_eia860_plants(years, eia860_xlsx):
    """
    Generate an exhaustive list of EIA 860 plants.

    # Most plants are listed in the 'Plant Frame' tabs for each year. 'Plant
    # Frame' tab does not exist before 2011 and there is plant specific
    # information that is not included in the 'Plant Frame' tab that will be
    # pulled into the plant info table. For years before 2011 it will be used
    # to generate the exhaustive list of plants.

    This function will be used in two ways: to populate the plant info table
    and to check the plant mapping to find missing plants.

    Args:
        years: The year that we're trying to read data for.
        eia860_xlsx: required and should not be modified
    Returns:
        Data frame that populates the plant info table
        A check of plant mapping to identify missing plants
    """
    recent_years = [y for y in years if y >= 2011]

    df_all_years = pd.DataFrame(columns=['plant_id'])

    pf = pd.DataFrame(columns=['plant_id', 'plant_state',
                               'combined_heat_power',
                               'eia_sector', 'naics_code',
                               'reporting_frequency', 'nameplate_capacity_mw',
                               'year'])
    if (len(recent_years) > 0):
        pf = get_eia860_page('boiler_generator_assn_eia860', eia860_xlsx,
                             years=recent_years)
    #     pf_mw = pd.DataFrame(columns=['plant_id', 'nameplate_capacity_mw',
    #                                   'year'])
    #     if 2011 in recent_years:
    #         pf_mw = pf[['plant_id', 'nameplate_capacity_mw', 'year']]
    #         pf_mw = pf_mw[pf_mw['nameplate_capacity_mw'] > 0]
    #     pf = pf[['plant_id', 'plant_name', 'plant_state',
    #              'combined_heat_power',
    #              'eia_sector', 'naics_code',
    #              'reporting_frequency', 'year']]
    #
    # gf = get_eia923_page('generation_fuel', eia923_xlsx, years=years)
    # gf = gf[['plant_id', 'plant_name',
    #          'operator_name', 'operator_id', 'plant_state',
    #          'combined_heat_power', 'census_region', 'nerc_region', 'year']]
    #
    # bf = get_eia923_page('boiler_fuel', eia923_xlsx, years=years)
    # bf = bf[['plant_id', 'plant_state',
    #          'combined_heat_power',
    #          'naics_code',
    #          'eia_sector', 'census_region', 'nerc_region', 'operator_name',
    #          'operator_id', 'year']]
    #
    # g = get_eia923_page('generator', eia923_xlsx, years=years)
    # g = g[['plant_id', 'plant_state', 'combined_heat_power',
    #        'census_region', 'nerc_region', 'naics_code', 'eia_sector',
    #        'operator_name', 'operator_id', 'year']]
    #
    # frc = get_eia923_page('fuel_receipts_costs', eia923_xlsx, years=years)
    # frc = frc[['plant_id', 'plant_state', 'year']]
    #
    # plant_ids = pd.concat(
    #     [pf.plant_id, gf.plant_id, bf.plant_id, g.plant_id, frc.plant_id],)
    # plant_ids = plant_ids.unique()
    #
    # plant_info_compiled = pd.DataFrame(columns=['plant_id'])
    # plant_info_compiled['plant_id'] = plant_ids
    # for tab in [pf, pf_mw, gf, bf, g, frc]:
    #     tab = tab.sort_values(['year', ], ascending=False)
    #     tab = tab.drop_duplicates(subset='plant_id')
    #     plant_info_compiled = plant_info_compiled.merge(tab, on='plant_id',
    #                                                     how='left')
    #     plant_info_compiled_x = plant_info_compiled.filter(regex='_x$')
    #     cols_x = plant_info_compiled_x.columns
    #     if len(cols_x) > 0:
    #         cols_y = plant_info_compiled_x.columns.str.replace('_x$', '_y')
    #         for col_x, col_y in zip(cols_x, cols_y):
    #             plant_info_compiled[col_x].fillna(plant_info_compiled[col_y],
    #                                               inplace=True, axis=0)
    #         plant_info_compiled.drop(cols_y, axis=1, inplace=True)
    #         plant_info_compiled.columns = \
    #             plant_info_compiled.columns.str.replace('_x$', '')
    # plant_info_compiled = plant_info_compiled.drop_duplicates('plant_id')
    # plant_info_compiled = plant_info_compiled.drop(['year'], axis=1)
    # return(plant_info_compiled)
