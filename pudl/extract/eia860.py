"""
Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""

import os.path
import glob
import pandas as pd
import pudl
from pudl.settings import SETTINGS
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
    if year not in pc.data_years['eia860']:
        raise AssertionError(
            f"EIA 860 data requested for unavailable year: {year}.\n"
            f"EIA 860 data is available for {pc.working_years['eia860']}\n"
        )

    return(os.path.join(SETTINGS['eia860_data_dir'],
                        'eia860{}'.format(year)))


def get_eia860_file(yr, file):
    """
    Given a year, return the appopriate EIA860 excel file.

    Args:
        year (int): The year that we're trying to read data for.
    Returns:
        path to EIA 860 spreadsheets corresponding to a given year.
    """
    if yr not in pc.working_years['eia860']:
        raise AssertionError(
            f"Requested non-working EIA 860 year: {yr}.\n"
            f"EIA 860 is only working for: {pc.working_years['eia860']}\n"
        )

    return glob.glob(os.path.join(datadir(yr), file))[0]


def get_eia860_xlsx(years, filename, verbose=True):
    """
    Read in Excel files to create Excel objects.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years: The years that we're trying to read data for.
        files: ['enviro_assn', 'utilities', 'plants', 'generators']

    Returns:
        xlsx file of EIA Form 860 for input year(s)
    """
    eia860_xlsx = {}
    pattern = pc.files_dict_eia860[filename]
    if verbose:
        print(f"Extracting EIA 860 {filename} data...", flush=True)
        print(f"    ", end='', flush=True)
    for yr in years:
        if verbose:
            print(f"{yr} ", end='', flush=True)
        eia860_xlsx[yr] = pd.ExcelFile(get_eia860_file(yr, pattern))
    print(f"\n", end='', flush=True)
    return eia860_xlsx


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
        sheet_name (int): An integer indicating which page in the worksheet
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
        all_columns (pd.Index): The column Index associated with the column
            map -- it includes all of the columns which might be present in
            all of the years of data, for use in setting the column index of
            the raw dataframe which is ultimately extracted, so we can ensure
            that they all have the same columns, even if we're only loading a
            limited number of years.
    """
    sheet_name = pc.tab_map_eia860.at[year, page]
    skiprows = pc.skiprows_eia860.at[year, page]

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

    all_columns = page_to_df[page].columns

    return (sheet_name, skiprows, column_map, all_columns)


def get_eia860_page(page, eia860_xlsx,
                    years=pc.working_years['eia860'],
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
    if page not in pc.tab_map_eia860.columns and page != 'year_index':
        raise AssertionError(
            f"Unrecognized EIA 860 page: {page}\n"
            f"Acceptable EIA 860 pages: {pc.tab_map_eia860.columns}\n"
        )

    if verbose:
        print(f'Converting EIA 860 {page} to DataFrame...')
        print('    ', end='')

    df = pd.DataFrame()
    for yr in years:
        if yr not in pc.working_years['eia860']:
            raise AssertionError(
                f"Requested non-working EIA 860 year: {yr}.\n"
                f"EIA 860 works for {pc.working_years['eia860']}\n"
            )
        print(f"{yr} ", end='')
        sheet_name, skiprows, column_map, all_columns = \
            get_eia860_column_map(page, yr)
        newdata = pd.read_excel(eia860_xlsx[yr],
                                sheet_name=sheet_name,
                                skiprows=skiprows)
        newdata = pudl.helpers.simplify_columns(newdata)

        # boiler_generator_assn tab is missing a YEAR column. Add it!
        if 'report_year' not in newdata.columns:
            newdata['report_year'] = yr

        newdata = newdata.rename(columns=column_map)

        df = df.append(newdata)
    print("\n", end='')

    # We need to ensure that ALL possible columns show up in the dataframe
    # that's being returned, even if they are empty, so that we know we have a
    # consistent set of columns to work with in the transform step of ETL, and
    # the columns match up with the database definition.
    missing_cols = all_columns.difference(df.columns)
    empty_cols = pd.DataFrame(columns=missing_cols)
    df = pd.concat([df, empty_cols], sort=True)
    return df


def create_dfs_eia860(files=pc.files_eia860,
                      eia860_years=pc.working_years['eia860'],
                      verbose=True):
    """
    Create a dictionary of pages (keys) to dataframes (values) from eia860
    tabs.

    Args:
        a list of eia860 files
        a list of years

    Returns:
        dictionary of pages (key) to dataframes (values)

    """
    # Prep for ingesting EIA860
    # Create excel objects
    eia860_dfs = {}
    for f in files:
        eia860_xlsx = get_eia860_xlsx(eia860_years, f, verbose=verbose)
        # Create DataFrames
        pages = pc.file_pages_eia860[f]

        for page in pages:
            eia860_dfs[page] = get_eia860_page(page, eia860_xlsx,
                                               years=eia860_years,
                                               verbose=verbose)
    return eia860_dfs


def extract(eia860_years=pc.working_years['eia860'], verbose=True):
    # Prep for ingesting EIA860
    # create raw 860 dfs from spreadsheets
    eia860_raw_dfs = {}
    if not eia860_years:
        if verbose:
            print('Not extracting EIA 860.')
        return eia860_raw_dfs

    print('============================================================')
    print('Extracting EIA 860 data from spreadsheets.')
    eia860_raw_dfs = create_dfs_eia860(files=pc.files_eia860,
                                       eia860_years=eia860_years,
                                       verbose=verbose)
    return eia860_raw_dfs
