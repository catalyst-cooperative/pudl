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
# Administration (EIA) Form 923 data.
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


def get_eia860_file(yr):
    """
    Given a year, return the appopriate EIA860 excel file.

    Args:
        year (int): The year that we're trying to read data for.
    Returns:
        path to EIA 860 spreadsheets corresponding to a given year.
    """
    assert(yr > 2008), "EIA860 file selection only works for 2009 & later."
    return(glob.glob(os.path.join(datadir(yr), '*EnviroAsso*'))[0])


def get_eia860_xlsx(years):
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
        eia860_xlsx[yr] = pd.ExcelFile(get_eia860_file(yr))
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
        'boiler_generator_assn': pc.boiler_generator_assn_map_eia860}

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
        if(page == 'boiler_generator_assn'):
            newdata['year'] = yr

        newdata = newdata.rename(columns=column_map)

        df = df.append(newdata)
    return(df)
