"""
Retrieve data from EPA's Integrated Planning Model (IPM) v6

Unlike most of the PUDL data sources, IPM is not an annual timeseries. This file
assumes that only v6 will be used as an input, so there are a limited number
of files.
"""

import logging
from pathlib import Path
import pandas as pd
from pudl.settings import SETTINGS
import pudl.constants as pc

logger = logging.getLogger(__name__)

datadir = Path(SETTINGS['epaipm_data_dir'])
pudl_datadir = Path(SETTINGS['pudl_data_dir'])


def get_epaipm_name(file):
    """
    Return the appopriate EPA IPM excel file.

    Args:
        file (str): The file that we're trying to read data for.
    Returns:
        path to EPA IPM spreadsheet.
    """
    if sorted(datadir.glob(file)):
        name = sorted(datadir.glob(file))[0]
    elif sorted(pudl_datadir.glob(file)):
        name = sorted(pudl_datadir.glob(file))[0]
    else:
        raise FileNotFoundError(
            f'No files matching the pattern "{file}" were found.'
        )

    return name


def get_epaipm_file(filename, read_file_args):
    """
    Read in files to create dataframes. No need to use ExcelFile
    objects with the IPM files because each file is only a single sheet.

    Args:
        filename: ['single_transmission', 'joint_transmission']
        read_file_args: dictionary of arguments for pandas read_*

    Returns:
        xlsx file of EPA IPM data
    """
    epaipm_file = {}
    pattern = pc.files_dict_epaipm[filename]
    logger.info(
        f"Extracting data from EPA IPM {filename} spreadsheet.")

    full_filename = get_epaipm_name(pattern)
    suffix = full_filename.suffix

    if suffix == '.xlsx':
        epaipm_file = pd.read_excel(
            full_filename,
            **read_file_args
        )
    elif suffix == '.csv':
        epaipm_file = pd.read_csv(
            full_filename,
            **read_file_args
        )
    # if filename == 'transmission_single':
    #     epaipm_xlsx = epaipm_xlsx.reset_index()
    return epaipm_file


def create_dfs_epaipm(files=pc.files_epaipm):
    """
    Create a dictionary of pages (keys) to dataframes (values) from epaipm
    tabs.

    Args:
        a list of epaipm files

    Returns:
        dictionary of pages (key) to dataframes (values)

    """
    # Prep for ingesting epaipm
    # Create excel objects
    epaipm_dfs = {}
    for f in files:
        # NEEDS is the only IPM data file with multiple sheets. Keeping the overall
        # code simpler but adding this if statement to read both sheets (active and
        # retired by 2021).
        if f == 'plant_region_map_ipm':
            epaipm_dfs['plant_region_map_ipm_active'] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict['plant_region_map_ipm_active']
            )
            epaipm_dfs['plant_region_map_ipm_retired'] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict['plant_region_map_ipm_retired']
            )
        else:
            epaipm_dfs[f] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict[f]
            )

    return epaipm_dfs


def extract(epaipm_tables=pc.epaipm_pudl_tables):
    """
    Extract data from IPM files.

    arga
    ----------
    epaipm_tables (iterable): A tuple or list of table names to extract

    Returns:
    -------
    dict
        dictionary of dataframes with extracted (but not yet transformed) data
        from each file.
    """
    # Prep for ingesting EPA IPM
    # create raw ipm dfs from spreadsheets

    logger.info('Beginning ETL for EPA IPM.')

    files = {
        table: pattern for table, pattern in pc.files_dict_epaipm.items()
        if table in epaipm_tables
    }

    epaipm_raw_dfs = create_dfs_epaipm(
        files=files
    )
    return epaipm_raw_dfs
