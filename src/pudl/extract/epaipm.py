"""
Retrieve data from EPA's Integrated Planning Model (IPM) v6.

Unlike most of the PUDL data sources, IPM is not an annual timeseries. This
file assumes that only v6 will be used as an input, so there are a limited
number of files.

This module was written by :user:`gschivley`

"""

import importlib
import logging
from pathlib import Path

import pandas as pd

import pudl.constants as pc
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


def get_epaipm_name(file, data_dir):
    """Returns the appropriate EPA IPM excel file.

    Args:
        file (str): The file that we're trying to read data for.
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        str: The path to EPA IPM spreadsheet.

    """
    # Access the CSV scraped from a PDF & distributed with PUDL:
    if file == 'transmission_joint_epaipm':
        with importlib.resources.path(
            'pudl.package_data.epa.ipm', 'table_3-5_transmission_joint_ipm.csv'
        ) as p:
            name = p
    else:
        epaipm_dir = Path(datastore.path(
            'epaipm', file=False, year=None, data_dir=data_dir))
        pattern = pc.files_dict_epaipm[file]
        name = sorted(epaipm_dir.glob(pattern))[0]

    return name


def get_epaipm_file(filename, read_file_args, data_dir):
    """Reads in files to create dataframes.

    No need to use ExcelFile objects with the IPM files because each file
    is only a single sheet.

    Args:
        filename (str): ['single_transmission', 'joint_transmission']
        read_file_args (dict): dictionary of arguments for pandas read_*
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        :class:`pandas.io.excel.ExcelFile`: an xlsx file of EPA IPM data.

    """
    epaipm_file = {}
    logger.info(
        f"Extracting data from EPA IPM {filename} spreadsheet.")

    full_filename = get_epaipm_name(filename, data_dir)
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

    return epaipm_file


def create_dfs_epaipm(files, data_dir):
    """Makes dictionary of pages (keys) to dataframes (values) for epaipm tabs.

    Args:
        files (list): a list of epaipm files
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        dict: dictionary of pages (key) to dataframes (values)

    """
    # Prep for ingesting epaipm
    # Create excel objects
    epaipm_dfs = {}
    for f in files:
        # NEEDS is the only IPM data file with multiple sheets. Keeping the overall
        # code simpler but adding this if statement to read both sheets (active and
        # retired by 2021).
        if f == 'plant_region_map_epaipm':
            epaipm_dfs['plant_region_map_epaipm_active'] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict['plant_region_map_epaipm_active'],
                data_dir
            )
            epaipm_dfs['plant_region_map_epaipm_retired'] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict['plant_region_map_epaipm_retired'],
                data_dir
            )
        else:
            epaipm_dfs[f] = get_epaipm_file(
                f,
                pc.read_excel_epaipm_dict[f],
                data_dir
            )

    return epaipm_dfs


def extract(epaipm_tables, data_dir):
    """Extracts data from IPM files.

    Args:
        epaipm_tables (iterable): A tuple or list of table names to extract
        data_dir (path-like): Path to the top directory of the PUDL datastore.

    Returns:
        dict: dictionary of DataFrames with extracted (but not yet transformed)
        data from each file.

    """
    # Prep for ingesting EPA IPM
    # create raw ipm dfs from spreadsheets

    logger.info('Beginning ETL for EPA IPM.')

    # files = {
    #    table: pattern for table, pattern in pc.files_dict_epaipm.items()
    #    if table in epaipm_tables
    # }

    epaipm_raw_dfs = create_dfs_epaipm(files=epaipm_tables, data_dir=data_dir)
    return epaipm_raw_dfs
