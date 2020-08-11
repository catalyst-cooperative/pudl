"""
Retrieve data from EPA's Integrated Planning Model (IPM) v6.

Unlike most of the PUDL data sources, IPM is not an annual timeseries. This
file assumes that only v6 will be used as an input, so there are a limited
number of files.

This module was written by :user:`gschivley`
"""
import logging
from pathlib import Path

import pandas as pd

from pudl import constants as pc
from pudl.workspace import datastore as datastore

logger = logging.getLogger(__name__)


class EpaIpmDatastore(datastore.Datastore):
    """Provide thin wrapper of Datastore."""

    table_filename = {
        "transmission_single_epaipm":
            "table_3-21_annual_transmission_capabilities_of_u.s._model_regions_in_epa_platform_v6_-_2021.xlsx",
        "transmission_joint_epaipm": "table_3-5_transmission_joint_ipm.csv",
        "load_curves_epaipm":
            "table_2-2_load_duration_curves_used_in_epa_platform_v6.xlsx",
        "plant_region_map_epaipm":
            "needs_v6_november_2018_reference_case_0.xlsx"
    }

    def get_dataframe(self, table_name, pandas_args):
        """
        Retrieve the specified file from the epaipm archive.

        Args:
            table_name: table name, from self.table_filename
            pandas_args: pandas arguments for parsing the file
        Returns:
             Pandas dataframe of EPA IPM data.
        """
        def resource_path():
            """Get the path of the requested file, from the datastore."""
            filename = self.table_filename[table_name]
            resources = self.get_resources("epaipm")

            for r in resources:
                if r["name"] == filename:
                    return Path(r["path"])

            raise ValueError(
                "%s is not available in the epaipm archive" % filename)

        logger.debug("Dataframe %s requested", table_name)
        path = resource_path()

        if path.suffix == ".xlsx":
            logger.debug("Dataframe from excel: %s" % path)
            return pd.read_excel(path, **pandas_args)

        if path.suffix == ".csv":
            logger.debug("Dataframe from csv: %s" % path)
            return pd.read_csv(path, **pandas_args)

        raise ValueError("%s: unknown file format on %s" % (path.suffix, path))


def create_dfs_epaipm(files, ds):
    """Makes dictionary of pages (keys) to dataframes (values) for epaipm tabs.

    Args:
        files (list): a list of epaipm files
        ds (:class:`EpaIpmDatastore`): Initialized datastore

    Returns:
        dict: dictionary of pages (key) to dataframes (values)

    """
    epaipm_dfs = {}
    for f in files:
        # NEEDS is the only IPM data file with multiple sheets. Keeping the overall
        # code simpler but adding this if statement to read both sheets (active and
        # retired by 2021).
        if f == 'plant_region_map_epaipm':
            epaipm_dfs['plant_region_map_epaipm_active'] = ds.get_dataframe(
                f, pc.read_excel_epaipm_dict['plant_region_map_epaipm_active'])

            epaipm_dfs['plant_region_map_epaipm_retired'] = ds.get_dataframe(
                f, pc.read_excel_epaipm_dict['plant_region_map_epaipm_retired'])
        else:
            epaipm_dfs[f] = ds.get_dataframe(f, pc.read_excel_epaipm_dict[f])

    return epaipm_dfs


def extract(epaipm_tables, ds):
    """Extracts data from IPM files.

    Args:
        epaipm_tables (iterable): A tuple or list of table names to extract
        ds (:class:`EpaIpmDatastore`): Initialized datastore

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

    epaipm_raw_dfs = create_dfs_epaipm(epaipm_tables, ds)
    return epaipm_raw_dfs
