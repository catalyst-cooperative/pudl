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


def get_epaipm_file(file):
    """
    Return the appopriate EPA IPM excel file.

    Args:
        file (str): The file that we're trying to read data for.
    Returns:
        path to EPA IPM spreadsheet.
    """

    return sorted(datadir.glob(file))[0]


def get_epaipm_xlsx(filename, read_excel_args):
    """
    Read in Excel files to create dataframes. No need to use ExcelFile
    objects with the IPM files because each file is only a single sheet.

    Args:
        filename: ['single_transmission', 'joint_transmission']
        read_excel_args: dictionary of arguments for pandas read_excel

    Returns:
        xlsx file of EPA IPM data
    """
    epaipm_xlsx = {}
    pattern = pc.files_dict_epaipm[filename]
    logger.info(
        f"Extracting data from EPA IPM {filename} spreadsheet.")
    epaipm_xlsx = pd.read_excel(
        get_epaipm_file(pattern),
        **read_excel_args
    )
    if filename == 'transmission_single':
        epaipm_xlsx = epaipm_xlsx.reset_index()
    return epaipm_xlsx


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
        epaipm_dfs[f] = get_epaipm_xlsx(
            f,
            pc.read_excel_epaipm_dict[f]
        )

    return epaipm_dfs


def extract():
    # Prep for ingesting EPA IPM
    # create raw ipm dfs from spreadsheets

    logger.info('Beginning ETL for EPA IPM.')
    epaipm_raw_dfs = create_dfs_epaipm(
        files=pc.files_dict_epaipm
    )
    return epaipm_raw_dfs
