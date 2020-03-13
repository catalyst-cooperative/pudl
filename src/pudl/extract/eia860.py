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
metadata = pudl.extract.excelmetadata.ExcelMetadata('eia860')

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
    """
    Read in Excel files to create Excel objects from EIA860 spreadsheets.

    Rather than reading in the same Excel files several times, we can just
    read them each in once (one per year) and use the ExcelFile object to
    refer back to the data in memory.

    Args:
        years (list): The years that we're trying to read data for.
        filename (str): ['enviro_assn', 'utilities', 'plants', 'generators']
        data_dir (path-like): Path to PUDL input datastore directory.

    Returns:
        :mod:`pandas.io.excel.ExcelFile`: xlsx file of EIA Form 860 for input
        year(s).

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
    if page not in metadata.all_pages() and page != 'year_index':
        raise AssertionError(
            f"Unrecognized EIA 860 page: {page}\n"
            f"Acceptable EIA 860 pages: {metadata.all_pages()}\n"
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
        dtype = {'plant_id_eia': pd.Int64Dtype()}
        if 'zip_code' in metadata.all_columns(page):
            dtype['zip_code'] = pc.column_dtypes['eia']['zip_code']

        newdata = pd.read_excel(eia860_xlsx[yr],
                                sheet_name=metadata.sheet_name(yr, page),
                                skiprows=metadata.skiprows(yr, page),
                                dtype=dtype,
                                )
        newdata = pudl.helpers.simplify_columns(newdata)

        # boiler_generator_assn tab is missing a YEAR column. Add it!
        if 'report_year' not in newdata.columns:
            newdata['report_year'] = yr

        newdata = newdata.rename(columns=metadata.column_map(yr, page))

        df = df.append(newdata, sort=True)

    # We need to ensure that ALL possible columns show up in the dataframe
    # that's being returned, even if they are empty, so that we know we have a
    # consistent set of columns to work with in the transform step of ETL, and
    # the columns match up with the database definition.
    missing_cols = metadata.all_columns(page).difference(df.columns)
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
