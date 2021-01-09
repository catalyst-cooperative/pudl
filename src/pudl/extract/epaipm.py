"""
Retrieve data from EPA's Integrated Planning Model (IPM) v6.

Unlike most of the PUDL data sources, IPM is not an annual timeseries. This
file assumes that only v6 will be used as an input, so there are a limited
number of files.

This module was written by :user:`gschivley`
"""
import io
import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple

import pandas as pd

from pudl.workspace.datastore import Datastore

# $ from pudl.workspace import datastore as datastore


logger = logging.getLogger(__name__)


class TableSettings(NamedTuple):
    """Contains information for how to access and load EpaIpm dataframes."""

    table_name: str
    file: str
    excel_settings: Dict[str, Any] = {}


class EpaIpmDatastore:
    """Helper for extracting EpaIpm dataframes from Datastore."""

    SETTINGS = (
        TableSettings(
            table_name="transmission_single_epaipm",
            file="table_3-21_annual_transmission_capabilities_of_u.s._model_regions_in_epa_platform_v6_-_2021.xlsx",
            excel_settings=dict(
                skiprows=3,
                usecols='B:F',
                index_col=[0, 1])
        ),
        TableSettings(
            table_name="transmission_joint_epaipm",
            file="table_3-5_transmission_joint_ipm.csv"
        ),
        TableSettings(
            table_name="load_curves_epaipm",
            file="table_2-2_load_duration_curves_used_in_epa_platform_v6.xlsx",
            excel_settings=dict(
                skiprows=3,
                usecols='B:AB')
        ),
        TableSettings(
            table_name="plant_region_map_epaipm_active",
            file="needs_v6_november_2018_reference_case_0.xlsx",
            excel_settings=dict(
                sheet_name='NEEDS v6_Active',
                usecols='C,I')
        ),
        TableSettings(
            table_name="plant_region_map_epaipm_retired",
            file="needs_v6_november_2018_reference_case_0.xlsx",
            excel_settings=dict(
                sheet_name='NEEDS v6_Retired_Through2021',
                usecols='C,I')
        ),
    )

    def __init__(self, datastore: Datastore):
        """Creates new instance of EpaIpmDatastore wrapper."""
        self.datastore = datastore

    def get_table_settings(self, table_name: str) -> TableSettings:
        """Returns TableSettings for a given table_name."""
        for s in self.SETTINGS:
            if s.table_name == table_name:
                return s
        raise ValueError(f"Table {table_name} not found in EpaIpmDatastore.SETTINGS.")

    def get_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Retrieve the specified file from the epaipm archive.

        Args:
            table_name: table name, from self.table_filename
            pandas_args: pandas arguments for parsing the file
        Returns:
             Pandas dataframe of EPA IPM data.
        """
        table_settings = self.get_table_settings(table_name)
        f = io.BytesIO(self.datastore.get_unique_resource(
            "epaipm",
            name=table_settings.file))

        path = Path(table_settings.file)
        if path.suffix == ".xlsx":
            return pd.read_excel(f, table_settings.excel_settings)

        if path.suffix == ".csv":
            return pd.read_csv(f)

        raise ValueError(f"{path.suffix}: unknown file format for {path}")


def extract(epaipm_tables: List[str], ds: Datastore) -> Dict[str, pd.DataFrame]:
    """Extracts data from IPM files.

    Args:
        epaipm_tables (iterable): A tuple or list of table names to extract
        ds (:class:`EpaIpmDatastore`): Initialized datastore

    Returns:
        dict: dictionary of DataFrames with extracted (but not yet transformed)
        data from each file.

    """
    # Prep for ingesting EPA IPM
    logger.info('Beginning ETL for EPA IPM.')
    ds = EpaIpmDatastore(ds)

    if "plant_region_map_epaipm" in epaipm_tables:
        # NEEDS is the only IPM data file with multiple sheets. Keeping the overall
        # code simpler but adding this if statement to read both sheets (active and
        # retired by 2021).
        epaipm_tables.remove("plant_region_map_epaipm")
        epaipm_tables.extend([
            "plant_region_map_epaipm_active",
            "plant_region_map_epaipm_retired"])

    return {f: ds.get_dataframe(f) for f in epaipm_tables}
