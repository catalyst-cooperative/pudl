"""
Retrieve data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

@author: alana for Catalyst Cooperative.

This code is for use analyzing EIA Form 923 data, years 2008-2016 Current
version is for years 2014-2016, which have standardized naming conventions and
file formatting.
"""

import pandas as pd
import os.path
from pudl import settings, constants

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 923 data.
###########################################################################


def datadir(year):
    """Given a year, return path to appropriate EIA 923 data directory."""
    # These are the only years we've got...
    assert year in range(2001, 2017)
    if(year < 2008):
        return(os.path.join(settings.EIA923_DATA_DIR,
                            'f906920_{}'.format(year)))
    else:
        return(os.path.join(settings.EIA923_DATA_DIR, 'f923_{}'.format(year)))


def get_eia923_files(years=[2014, 2015, 2016]):
    """
    Generate paths to EIA 923 spreadsheets corresponding to given years.

    This function select spreadsheets containing string '2_3_4' for years
    2014-2016 of EIA Form923 data. Years prior to 2014 do not fit formatting
    used in 'get_eia923_page' function. Additional parsing logic is required.
    """
    from glob import glob

    for yr in years:
        assert(yr > 2008), "EIA923 file selection only works for 2008 & later."

    # Find all the files matching *2_3_4* within the EIA923 data directories
    # corresponding to the years that we're looking at.
    return([glob(os.path.join(datadir(yr), '*2_3_4*'))[0] for yr in years])


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
        column_map: (dict): A dictionary that maps the names of the columns
            in the year being read in, to the canonical EIA923 column names
            (i.e. the column names as they are in 2014-2016). This dictionary
            will be used by DataFrame.rename(). The keys are the column names
            in the dataframe as read from older years, and the values are the
            canonmical column names.  All should be stripped of leading and
            trailing whitespace, converted to lower case, and have internal
            non-alphanumeric characters replaced with underscores.
    """
    sheetname = constants.tab_map_eia923.get_value(year, page)
    skiprows = constants.skiprows_eia923.get_value(year, page)

    page_to_df = {'generation_fuel': constants.generation_fuel_map_eia923,
                  'stocks': constants.stocks_map_eia923,
                  'boiler_fuel': constants.boiler_fuel_map_eia923,
                  'generator': constants.generator_map_eia923,
                  'fuel_receipts_costs':
                  constants.fuel_receipts_costs_map_eia923,
                  'plant_frame': constants.plant_frame_map_eia923}

    d = page_to_df[page].loc[year].to_dict()

    column_map = {}
    for k, v in d.items():
        column_map[v] = k

    return((sheetname, skiprows, column_map))


def get_eia923_page(page, years=[2014, 2015, 2016], verbose=True):
    """
    Read a single table from several years of EIA923 data. Return a DataFrame.

    The page argument must be exactly one of the following strings:
      - 'generation_fuel'
      - 'stocks'
      - 'boiler_fuel'
      - 'generator'
      - 'fuel_receipts_costs'
      - 'plant_frame'
    """
    for year in years:
        assert(year > 2010), "EIA923 parsing only works for 2011 and later."

    assert(page in constants.pagemap_eia923.index),\
        "Unrecognized EIA 923 page: {}".format(page)

    filenames = get_eia923_files(years)

    df = pd.DataFrame()
    for (yr, fn) in zip(years, filenames):
        if verbose:
            print('Reading EIA 923 {} data for {}...'.format(page, yr))

        sheetname, skiprows, column_map = get_eia923_column_map(page, yr)
        newdata = pd.read_excel(fn, sheetname=sheetname, skiprows=skiprows)

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

        df = df.append(newdata)

    # We could also do additional cleanup here -- for example:
    #  - Substituting ISO-3166 3 letter country codes for the ad-hoc EIA
    #    2-letter country codes.
    #  - Replacing Y/N string values with True/False Booleans
    #  - Replacing '.' strings with np.nan values as appropriate.

    return(df)


def yearly_to_monthly_eia923(df, md):
    """
    Convert an EIA 923 record with 12 months of data into 12 monthly records.

    Much of the data reported in EIA 923 is monthly, but all 12 months worth
    of data is reported in a single record, with one field for each of the 12
    months.  This function converts these annualized composite records into a
    set of 12 monthly records containing the same information, by parsing the
    field names for months, and adding a month field.  Non-time series data is
    retained in the same format.

    Args:
        df (pandas.DataFrame): A pandas DataFrame containing the annual
            data to be converted into monthly records.
        md (dict): a dictionary with the numbers 1-12 as keys, and the patterns
            used to match field names for each of the months as values. These
            patterns are also used to re-name the columns in the dataframe
            which is returned, so they need to match the entire portion of the
            column name that is month-specific.

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
