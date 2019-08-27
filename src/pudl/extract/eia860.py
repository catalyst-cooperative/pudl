"""
Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""

import glob
import logging
import os.path

import pandas as pd

import pudl
import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)

###########################################################################
# Helper functions & other objects to ingest & process Energy Information
# Administration (EIA) Form 860 data.
###########################################################################


def get_eia860_file(yr, file, data_dir):
    """
    Construct the appopriate path for a given EIA860 Excel file.

    Args:
        year (int): The year that we're trying to read data for.
        file (str): A string containing part of the file name for a given EIA
            860 file (e.g. '*Generat*')
        data_dir (str): Top level datastore directory.

    Returns:
        str: Path to EIA 860 spreadsheets corresponding to a given year.

    Raises:
        AssertionError: If the requested year is not in the list of working
            years for EIA 860.

    """
    if yr not in pc.working_years['eia860']:
        raise AssertionError(
            f"Requested non-working EIA 860 year: {yr}.\n"
            f"EIA 860 is only working for: {pc.working_years['eia860']}\n"
        )

    eia860_dir = datastore.path('eia860', year=yr, file=False,
                                data_dir=data_dir)
    eia860_file = glob.glob(os.path.join(eia860_dir, file))[0]

    return eia860_file


def get_eia860_xlsx(years, filename, data_dir):
    """Read in Excel files to create Excel objects.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years (list): The years that we're trying to read data for.
        filename (str): ['enviro_assn', 'utilities', 'plants', 'generators']

    Returns:
        pandas.io.excel.ExcelFile: xlsx file of EIA Form 860 for input year(s)

    """
    eia860_xlsx = {}
    pattern = pc.files_dict_eia860[filename]
    for yr in years:
        logger.info(
            f"Extracting data from EIA 860 {filename} spreadsheet for {yr}.")
        eia860_xlsx[yr] = pd.ExcelFile(
            get_eia860_file(yr, pattern, data_dir=data_dir)
        )
    return eia860_xlsx


def get_eia860_column_map(page, year):
    """Given a year and EIA860 page, returns info needed to slurp it from Excel.

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
        tuple: A tuple containing:
            - int: sheet_name, an integer indicating which page in the worksheet
              the data should be pulled from. 0 is the first page, 1 is the
              second page, etc. For use by pandas.read_excel()
            - int: skiprows, an integer indicating how many rows should be skipped
              at the top of the sheet being read in, before the header row
              that contains the strings which will be converted into column
              names in the dataframe which is created by pandas.read_excel()
            - dict: column_map, a dictionary that maps the names of the columns
              in the year being read in, to the canonical EIA923 column names
              (i.e. the column names as they are in 2014-2016). This
              dictionary will be used by DataFrame.rename(). The keys are the
              column names in the dataframe as read from older years, and the
              values are the canonmical column names.  All should be stripped
              of leading and trailing whitespace, converted to lower case,
              and have internal non-alphanumeric characters replaced with
              underscores.
            - pd.Index: all_columns, the column Index associated with the column
              map -- it includes all of the columns which might be present in
              all of the years of data, for use in setting the column index of
              the raw dataframe which is ultimately extracted, so we can
              ensure that they all have the same columns, even if we're only
              loading a limited number of years.
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
                    years=pc.working_years['eia860']):
    """Reads a table from several years of EIA860 data, returns a DataFrame.

    Args:
        page (str): The string label indicating which page of the EIA860 we
            are attempting to read in. The page argument must be exactly one of
            the following strings:

                - 'boiler_generator_assn'
                - 'utility'
                - 'plant'
                - 'generator_existing'
                - 'generator_proposed'
                - 'generator_retired'
                - 'ownership'
        eia860_xlsx (pandas.io.excel.ExcelFile): xlsx file of EIA Form 860 for
            input year or years
        years (list): The set of years to read into the DataFrame.

    Returns:
        pandas.DataFrame: A DataFrame of EIA 860 data from selected page, years.

    Raises:
        AssertionError: If the page string is not among the list of recognized
            EIA 860 page strings.
        AssertionError: If the year is not in the list of years that work for
            EIA 860.
    """
    if page not in pc.tab_map_eia860.columns and page != 'year_index':
        raise AssertionError(
            f"Unrecognized EIA 860 page: {page}\n"
            f"Acceptable EIA 860 pages: {pc.tab_map_eia860.columns}\n"
        )

    df = pd.DataFrame()
    for yr in years:
        if yr not in pc.working_years['eia860']:
            raise AssertionError(
                f"Requested non-working EIA 860 year: {yr}.\n"
                f"EIA 860 works for {pc.working_years['eia860']}\n"
            )
        logger.info(f"Converting EIA 860 spreadsheet tab {page} to pandas "
                    f"DataFrame for {yr}.")
        sheet_name, skiprows, column_map, all_columns = get_eia860_column_map(
            page, yr)
        newdata = pd.read_excel(eia860_xlsx[yr],
                                sheet_name=sheet_name,
                                skiprows=skiprows)
        newdata = pudl.helpers.simplify_columns(newdata)

        # boiler_generator_assn tab is missing a YEAR column. Add it!
        if 'report_year' not in newdata.columns:
            newdata['report_year'] = yr

        newdata = newdata.rename(columns=column_map)

        df = df.append(newdata)

    # We need to ensure that ALL possible columns show up in the dataframe
    # that's being returned, even if they are empty, so that we know we have a
    # consistent set of columns to work with in the transform step of ETL, and
    # the columns match up with the database definition.
    missing_cols = all_columns.difference(df.columns)
    empty_cols = pd.DataFrame(columns=missing_cols)
    df = pd.concat([df, empty_cols], sort=True)
    return df


def _create_dfs_eia860(files, eia860_years, data_dir):
    """Create a dict of pages (keys) to DataDrames (values) from EIA 860 tabs.

    Args:
        files (list): a list of eia860 files
        eia860_years (list): a list of years
        data_dir (str): Top level datastore directory.

    Returns:
        dict: A dictionary of pages (key) to DataFrames (values)

    """
    # Prep for ingesting EIA860
    # Create excel objects
    eia860_dfs = {}
    for f in files:
        eia860_xlsx = get_eia860_xlsx(eia860_years, f, data_dir)
        # Create DataFrames
        pages = pc.file_pages_eia860[f]

        for page in pages:
            eia860_dfs[page] = get_eia860_page(page, eia860_xlsx,
                                               years=eia860_years)
    return eia860_dfs


def extract(eia860_years, data_dir):
    """Creates a dictionary of DataFrames containing all the EIA 860 tables.

    Args:
        eia860_years (list): a list of data_years
        data_dir (str): Top level datastore directory.

    Returns:
        dict: A dictionary of EIA 860 pages (keys) and DataFrames (values)
    """
    # Prep for ingesting EIA860
    # create raw 860 dfs from spreadsheets
    eia860_raw_dfs = {}
    if not eia860_years:
        logger.info('Not performing ETL for EIA 860.')
        return eia860_raw_dfs

    logger.info('Beginning ETL for EIA 860.')
    eia860_raw_dfs = _create_dfs_eia860(
        files=pc.files_eia860,
        eia860_years=eia860_years,
        data_dir=data_dir)
    return eia860_raw_dfs
