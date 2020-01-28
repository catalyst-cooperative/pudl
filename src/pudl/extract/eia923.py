"""
Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
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
# Administration (EIA) Form 923 data.
###########################################################################


def get_eia923_file(yr, data_dir):
    """
    Construct the appopriate path for a given year's EIA923 Excel file.

    Args:
        year (int): The year that we're trying to read data for.
        data_dir (str): Top level datastore directory.

    Returns:
        str: path to EIA 923 spreadsheets corresponding to a given year.

    """
    if yr < min(pc.working_years['eia923']):
        raise ValueError(
            f"EIA923 file selection only works for 2009 & later "
            f"but file for {yr} was requested."
        )

    eia923_dir = datastore.path('eia923', year=yr, file=False,
                                data_dir=data_dir)
    eia923_globs = glob.glob(os.path.join(eia923_dir, '*2_3_4*'))

    # There can only be one!
    if len(eia923_globs) > 1:
        raise AssertionError(
            f'Multiple matching EIA923 spreadsheets found for {yr}!'
        )

    return eia923_globs[0]


def get_eia923_column_map(page, year):
    """
    Given a year and EIA923 page, returns info needed to slurp it from Excel.

    The format of the EIA923 has changed slightly over the years, and so it
    is not completely straightforward to pull information from the spreadsheets
    into our analytical framework. This function looks up a map of the various
    tabs in the spreadsheet by year and page, and returns the information
    needed to name the data fields in a standardized way, and pull the right
    cells from each year & page into our database.

    Args:
        page (str): The string label indicating which page of the EIA923 we
            are attempting to read in. Must be one of the following:
            'generation_fuel', 'stocks', 'boiler_fuel', 'generator',
            'fuel_receipts_costs', 'plant_frame'.
        year (int): The year that we're trying to read data for.

    Returns:
        tuple: A tuple containing:
            - int: sheet_name (int): An integer indicating which page in the
              worksheet the data should be pulled from. 0 is the first page,
              1 is the second page, etc. For use by :func:`pandas.read_excel`
            - int: skiprows, an integer indicating how many rows should be
              skipped at the top of the sheet being read in, before the
              header row that contains the strings which will be converted
              into column names in the dataframe which is created by
              :func:`pandas.read_excel`
            - int: skiprows, an integer indicating how many rows should be
              skipped at the top of the sheet being read in, before the header
              row that contains the strings which will be converted into column
              names in the dataframe which is created by
              :func:`pandas.read_excel`
            - dict: column_map, a dictionary that maps the names of the columns
              in the year being read in, to the canonical EIA923 column names.
              This dictionary will be used by :func:`pandas.DataFrame.rename`.
              The keys are the column names in the dataframe as read from older
              years, and the values are the canonmical column names. All
              should be stripped of leading and trailing whitespace, converted
              to lower case, and have internal non-alphanumeric characters
              replaced with underscores.

    """
    sheet_name = pc.tab_map_eia923.at[year, page]
    skiprows = pc.skiprows_eia923.at[year, page]

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

    all_columns = page_to_df[page].columns
    all_columns = all_columns.drop(
        ['reserved_2', 'reserved_1', 'reserved'], errors='ignore')

    return (sheet_name, skiprows, column_map, all_columns)


def get_eia923_page(page, eia923_xlsx,
                    years=pc.working_years['eia923']):
    """Reads a table from given years of EIA923 data, returns a DataFrame.

    Args:
        page (str): The string label indicating which page of the EIA923 we
            are attempting to read in. The page argument must be one of the
            strings listed in :func:`pudl.constants.working_pages_eia923`.
        eia923_xlsx (:class:`pandas.io.excel.ExcelFile`): xlsx file of EIA Form
            923 for input year(s).
        years (list): The set of years to read into the dataframe.

    Returns:
        :class:`pandas.DataFrame`: A dataframe containing the data from the
        selected page and selected years from EIA 923.

    """
    if min(years) < min(pc.working_years['eia923']):
        raise ValueError(
            f"EIA923 only works for 2009 and later. {min(years)} requested."
        )
    if (page not in pc.tab_map_eia923.columns) or (page == 'year_index'):
        raise ValueError(f"Unrecognized EIA 923 page: {page}")

    df = pd.DataFrame()
    for yr in years:
        logger.info(f"Converting EIA 923 {page} spreadsheet tab from {yr} "
                    f"into a pandas DataFrame")
        sheet_name, skiprows, column_map, all_columns = get_eia923_column_map(
            page, yr)
        newdata = pd.read_excel(eia923_xlsx[yr],
                                sheet_name=sheet_name,
                                skiprows=skiprows)
        newdata = pudl.helpers.simplify_columns(newdata)

        # Drop columns that start with "reserved" because they are empty
        to_drop = [c for c in newdata.columns if c[:8] == 'reserved']
        newdata.drop(to_drop, axis=1, inplace=True)

        # stocks tab is missing a YEAR column for some reason. Add it!
        if page == 'stocks':
            newdata['report_year'] = yr

        newdata = newdata.rename(columns=column_map)
        if page == 'stocks':
            newdata = newdata.rename(columns={
                'unnamed_0': 'census_division_and_state'})

        # Drop the fields with plant_id_eia 99999 or 999999.
        # These are state index
        if page != 'stocks':
            newdata = newdata[~newdata.plant_id_eia.isin([99999, 999999])]

        df = df.append(newdata, sort=True)
    # We need to ensure that ALL possible columns show up in the dataframe
    # that's being returned, even if they are empty, so that we know we have a
    # consistent set of columns to work with in the transform step of ETL, and
    # the columns match up with the database definition.
    missing_cols = all_columns.difference(df.columns)
    empty_cols = pd.DataFrame(columns=missing_cols)
    df = pd.concat([df, empty_cols], sort=True)
    return df


def get_eia923_xlsx(years, data_dir):
    """
    Reads in Excel files to create Excel objects.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years (list): The years that we're trying to read data for.
        data_dir (str): Top level datastore directory.

    Returns:
        :class:`pandas.io.excel.ExcelFile`: xlsx file of EIA Form 923 for input
        year(s)

    """
    eia923_xlsx = {}
    for yr in years:
        logger.info(f"Extracting EIA 923 spreadsheets for {yr}.")
        eia923_xlsx[yr] = pd.ExcelFile(get_eia923_file(yr, data_dir))
    return eia923_xlsx


def extract(eia923_years, data_dir):
    """
    Creates a dictionary of DataFrames containing all the EIA 923 tables.

    Args:
        eia923_years (list): a list of data_years
        data_dir (str): Top level datastore directory.

    Returns:
        dict: A dictionary containing the names of EIA 923 pages (keys) and
        :class:`pandas.DataFrame` instances filled with the data from each page
        (values).

    """
    eia923_raw_dfs = {}
    if not eia923_years:
        logger.info('No years given. Not extracting EIA 923 spreadsheet data.')
        return eia923_raw_dfs

    # Prep for ingesting EIA923
    # Create excel objects
    eia923_xlsx = get_eia923_xlsx(eia923_years, data_dir)

    # Create DataFrames
    for page in pc.working_pages_eia923:
        if page != "plant_frame":
            eia923_raw_dfs[page] = get_eia923_page(page,
                                                   eia923_xlsx,
                                                   years=eia923_years)

    return eia923_raw_dfs
