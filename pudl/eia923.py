"""
Retrieve data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
"""

import pandas as pd
import os.path
import glob
from pudl import settings
import pudl.constants as pc

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
###########################################################################


def datadir(year, basedir=settings.EIA923_DATA_DIR):
    """
    Data directory search for EIA Form 923.

    Args:
        year (int): The year that we're trying to read data for.
        basedir (os.path): Directory in which EIA923 data resides.
    Returns:
        path to appropriate EIA 923 data directory.
    """
    # These are the only years we've got...
    assert year in pc.data_years['eia923']
    if(year < 2008):
        return(os.path.join(basedir, 'f906920_{}'.format(year)))
    else:
        return(os.path.join(basedir, 'f923_{}'.format(year)))


def get_eia923_file(yr, basedir=settings.EIA923_DATA_DIR):
    """
    Given a year, return the appopriate EIA923 excel file.

    Args:
        year (int): The year that we're trying to read data for.
        basedir (os.path): Directory in which EIA923 data resides.
    Returns:
        path to EIA 923 spreadsheets corresponding to a given year.
    """
    assert(yr >= min(pc.working_years['eia923'])),\
        "EIA923 file selection only works for 2009 & later."
    eia923_filematch = glob.glob(os.path.join(
        datadir(yr, basedir=basedir), '*2_3_4*'))
    # There can only be one!
    assert len(eia923_filematch) == 1, \
        'Multiple matching EIA923 spreadsheets found for {}'.format(yr)
    return(eia923_filematch[0])


def get_eia923_column_map(page, year):
    """
    Given a year and EIA923 page, return info required to slurp it from Excel.

    The format of the EIA923 has changed slightly over the years, and so it
    is not completely straightforward to pull information from the spreadsheets
    into our analytical framework. This function looks up a map of the various
    tabs in the spreadsheet by year and page, and returns the information
    needed to name the data fields in a standardized way, and pull the right
    cells from each year & page into our database.

    Args:
        page (str): The string label indicating which page of the EIA923 we
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
    sheetname = pc.tab_map_eia923.get_value(year, page)
    skiprows = pc.skiprows_eia923.get_value(year, page)

    page_to_df = {
        'generation_fuel': pc.generation_fuel_map_eia923,
        'stocks': pc.stocks_map_eia923,
        'boiler_fuel': pc.boiler_fuel_map_eia923,
        'generator': pc.generator_map_eia923,
        'fuel_receipts_costs': pc.fuel_receipts_costs_map_eia923,
        'plant_frame': pc.plant_frame_map_eia923}

    d = page_to_df[page].loc[year].to_dict()

    column_map = {}
    for k, v in d.items():
        column_map[v] = k

    return((sheetname, skiprows, column_map))


def get_eia923_page(page, eia923_xlsx,
                    years=[2011, 2012, 2013, 2014, 2015, 2016],
                    verbose=True):
    """
    Read a single table from several years of EIA923 data. Return a DataFrame.

    Args:
        page (str): The string label indicating which page of the EIA923 we
        are attempting to read in. The page argument must be exactly one of the
        following strings:
            - 'generation_fuel'
            - 'stocks'
            - 'boiler_fuel'
            - 'generator'
            - 'fuel_receipts_costs'
            - 'plant_frame'

      years (list): The set of years to read into the dataframe.

    Returns:
        pandas.DataFrame: A dataframe containing the data from the selected
            page and selected years from EIA 923.
    """
    assert min(years) >= min(pc.working_years['eia923']),\
        "EIA923 works for 2009 and later. {} requested.".format(min(years))
    assert page in pc.tab_map_eia923.columns and page != 'year_index',\
        "Unrecognized EIA 923 page: {}".format(page)

    if verbose:
        print('Converting EIA 923 {} to DataFrame...'.format(page))
    df = pd.DataFrame()
    for yr in years:
        sheetname, skiprows, column_map = get_eia923_column_map(page, yr)
        newdata = pd.read_excel(eia923_xlsx[yr],
                                sheetname=sheetname,
                                skiprows=skiprows)

        # Clean column names: lowercase, underscores instead of white space,
        # no non-alphanumeric characters
        newdata.columns = newdata.columns.str.replace('[^0-9a-zA-Z]+', ' ')
        newdata.columns = newdata.columns.str.strip().str.lower()
        newdata.columns = newdata.columns.str.replace(' ', '_')

        # Drop columns that start with "reserved" because they are empty
        to_drop = [c for c in newdata.columns if c[:8] == 'reserved']
        newdata.drop(to_drop, axis=1, inplace=True)

        # stocks tab is missing a YEAR column for some reason. Add it!
        if(page == 'stocks'):
            newdata['year'] = yr

        newdata = newdata.rename(columns=column_map)
        if(page == 'stocks'):
            newdata = newdata.rename(columns={
                'unnamed_0': 'census_division_and_state'})

        # Drop the fields with plant_id 99999.
        # These are state index
        if(page != 'stocks'):
            newdata = newdata.loc[newdata['plant_id'] != 99999]

        df = df.append(newdata)

    # We could also do additional cleanup here -- for example:
    #  - Substituting ISO-3166 3 letter country codes for the ad-hoc EIA
    #    2-letter country codes.
    #  - Replacing Y/N string values with True/False Booleans
    #  - Replacing '.' strings with np.nan values as appropriate.

    return(df)


def get_eia923_xlsx(years):
    """
    Read in Excel files to create Excel objects.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years: The years that we're trying to read data for.
    Returns:
        xlsx file of EIA Form 923 for input year(s)
    """
    eia923_xlsx = {}
    for yr in years:
        print("Reading EIA 923 spreadsheet data for {}.".format(yr))
        eia923_xlsx[yr] = pd.ExcelFile(get_eia923_file(yr))
    return(eia923_xlsx)


def get_eia923_plants(years, eia923_xlsx):
    """
    Generate an exhaustive list of EIA 923 plants.

    Most plants are listed in the 'Plant Frame' tabs for each year. The 'Plant
    Frame' tab does not exist before 2011 and there is plant specific
    information that is not included in the 'Plant Frame' tab that will be
    pulled into the plant info table. For years before 2011, it will be used to
    generate the exhaustive list of plants.

    This function will be used in two ways: to populate the plant info table
    and to check the plant mapping to find missing plants.

    Args:
        years: The year that we're trying to read data for.
        eia923_xlsx: required and should not be modified
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
        pf = get_eia923_page('plant_frame', eia923_xlsx, years=recent_years)
        pf_mw = pd.DataFrame(columns=['plant_id', 'nameplate_capacity_mw',
                                      'year'])
        if 2011 in recent_years:
            pf_mw = pf[['plant_id', 'nameplate_capacity_mw', 'year']]
            pf_mw = pf_mw[pf_mw['nameplate_capacity_mw'] > 0]
        pf = pf[['plant_id', 'plant_name', 'plant_state',
                 'combined_heat_power',
                 'eia_sector', 'naics_code',
                 'reporting_frequency', 'year']]

    gf = get_eia923_page('generation_fuel', eia923_xlsx, years=years)
    gf = gf[['plant_id', 'plant_name',
             'operator_name', 'operator_id', 'plant_state',
             'combined_heat_power', 'census_region', 'nerc_region', 'year']]

    bf = get_eia923_page('boiler_fuel', eia923_xlsx, years=years)
    bf = bf[['plant_id', 'plant_state',
             'combined_heat_power',
             'naics_code',
             'eia_sector', 'census_region', 'nerc_region', 'operator_name',
             'operator_id', 'year']]

    g = get_eia923_page('generator', eia923_xlsx, years=years)
    g = g[['plant_id', 'plant_state', 'combined_heat_power',
           'census_region', 'nerc_region', 'naics_code', 'eia_sector',
           'operator_name', 'operator_id', 'year']]

    frc = get_eia923_page('fuel_receipts_costs', eia923_xlsx, years=years)
    frc = frc[['plant_id', 'plant_state', 'year']]

    plant_ids = pd.concat(
        [pf.plant_id, gf.plant_id, bf.plant_id, g.plant_id, frc.plant_id],)
    plant_ids = plant_ids.unique()

    plant_info_compiled = pd.DataFrame(columns=['plant_id'])
    plant_info_compiled['plant_id'] = plant_ids
    for tab in [pf, pf_mw, gf, bf, g, frc]:
        tab = tab.sort_values(['year', ], ascending=False)
        tab = tab.drop_duplicates(subset='plant_id')
        plant_info_compiled = plant_info_compiled.merge(tab, on='plant_id',
                                                        how='left')
        plant_info_compiled_x = plant_info_compiled.filter(regex='_x$')
        cols_x = plant_info_compiled_x.columns
        if len(cols_x) > 0:
            cols_y = plant_info_compiled_x.columns.str.replace('_x$', '_y')
            for col_x, col_y in zip(cols_x, cols_y):
                plant_info_compiled[col_x].fillna(plant_info_compiled[col_y],
                                                  inplace=True, axis=0)
            plant_info_compiled.drop(cols_y, axis=1, inplace=True)
            plant_info_compiled.columns = \
                plant_info_compiled.columns.str.replace('_x$', '')
    plant_info_compiled = plant_info_compiled.drop_duplicates('plant_id')
    plant_info_compiled = plant_info_compiled.drop(['year'], axis=1)
    return(plant_info_compiled)


def yearly_to_monthly_eia923(df, md):
    """
    Convert an EIA 923 record with 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth of
    data is reported in a single record, with one field for each of the 12
    months.  This function converts these annualized composite records into a
    set of 12 monthly records containing the same information, by parsing the
    field names for months, and adding a month field.  Non - time series data
    is retained in the same format.

    Args:
        df(pandas.DataFrame): A pandas DataFrame containing the annual
            data to be converted into monthly records.
        md(dict): a dictionary with the numbers 1 - 12 as keys, and the
            patterns used to match field names for each of the months as
            values. These patterns are also used to re - name the columns in
            the dataframe which is returned, so they need to match the entire
            portion of the column name that is month - specific.

    Returns:
        pandas.DataFrame: A dataframe containing the same data as was passed in
            via df, but with monthly records instead of annual records.
    """
    # Pull out each month's worth of data, merge it with the common columns,
    # rename columns to match the PUDL DB, add an appropriate month column,
    # and insert it into the PUDL DB.
    yearly = df.copy()
    monthly = pd.DataFrame()

    for m in md.keys():
        # Grab just the columns for the month we're working on.
        this_month = yearly.filter(regex=md[m])
        # Drop this month's data from the yearly data frame.
        yearly.drop(this_month.columns, axis=1, inplace=True)
        # Rename this month's columns to get rid of the month reference.
        this_month.columns = this_month.columns.str.replace(md[m], '')
        # Add a numerical month column corresponding to this month.
        this_month['month'] = m
        # Add this month's data to the monthly DataFrame we're building.
        monthly = pd.concat([monthly, this_month])

    # Merge the monthly data we've built up with the remaining fields in the
    # data frame we started with -- all of which should be independent of the
    # month, and apply across all 12 of the monthly records created from each
    # of the # initial annual records.
    return(yearly.merge(monthly, left_index=True, right_index=True))
