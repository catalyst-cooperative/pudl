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
metadata = pudl.extract.excelmetadata.ExcelMetadata(
    'eia923', drop_columns=['reserved_1', 'reserved_2', 'reserved'])
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
    if (page not in metadata.all_pages()) or (page == 'year_index'):
        raise ValueError(f"Unrecognized EIA 923 page: {page}")

    df = pd.DataFrame()
    for yr in years:
        logger.info(f"Converting EIA 923 {page} spreadsheet tab from {yr} "
                    f"into a pandas DataFrame")
        newdata = pd.read_excel(eia923_xlsx[yr],
                                sheet_name=metadata.sheet_name(yr, page),
                                skiprows=metadata.skiprows(yr, page))
        newdata = pudl.helpers.simplify_columns(newdata)

        # Drop columns that start with "reserved" because they are empty
        to_drop = [c for c in newdata.columns if c[:8] == 'reserved']
        newdata.drop(to_drop, axis=1, inplace=True)

        # stocks tab is missing a YEAR column for some reason. Add it!
        if page == 'stocks':
            newdata['report_year'] = yr

        newdata = newdata.rename(columns=metadata.column_map(yr, page))
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
    missing_cols = metadata.all_columns(page).difference(df.columns)
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
