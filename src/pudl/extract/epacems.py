"""
Retrieve data from EPA CEMS hourly zipped CSVs.

This modules pulls data from EPA's published CSV files.
"""
import logging
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

import pandas as pd

from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


class EpaCemsPartition(NamedTuple):
    """Represents EpaCems partition identifying unique resource file."""

    year: str
    state: str

    def get_key(self):
        """Returns hashable key for use with EpaCemsDatastore."""
        return (self.year, self.state.lower())

    def get_filters(self):
        """Returns filters for retrieving given partition resource from Datastore."""
        return dict(year=self.year, state=self.state.lower())

    def get_monthly_file(self, month: int) -> Path:
        """Returns path to the file that contains data for a given month.

        This file is stored inside the zip file resource for this partition.
        """
        return Path(f"{self.year}{self.state.lower()}{month:02}")

    def __str__(self):
        """Returns string representation of the partition (YEAR-STATE)."""
        return f"{self.year}-{self.state}"


class EpaCemsDatastore:
    """Helper class to extract EpaCems resources from datastore.

    EpaCems resources are identified by a year and a state. Each of these zip files
    contain monthly zip files that in turn contain csv files. This class implements
    get_data_frame method that will concatenate tables for a given state and month
    across all months.
    """

    IGNORE_COLS = {
        "FACILITY_NAME",
        "SO2_RATE (lbs/mmBtu)",
        "SO2_RATE",
        "SO2_RATE_MEASURE_FLG",
        "CO2_RATE (tons/mmBtu)",
        "CO2_RATE",
        "CO2_RATE_MEASURE_FLG",
    }
    """set: The set of EPA CEMS columns to ignore when reading data."""

    # Specify dtypes to for reading the CEMS CSVs
    CSV_DTYPES = {
        "STATE": pd.StringDtype(),
        # "FACILITY_NAME": str,  # Not reading from CSV
        "ORISPL_CODE": pd.Int64Dtype(),
        "UNITID": pd.StringDtype(),
        # These op_date, op_hour, and op_time variables get converted to
        # operating_date, operating_datetime and operating_time_interval in
        # transform/epacems.py
        "OP_DATE": pd.StringDtype(),
        "OP_HOUR": pd.Int64Dtype(),
        "OP_TIME": float,
        "GLOAD (MW)": float,
        "GLOAD": float,
        "SLOAD (1000 lbs)": float,
        "SLOAD (1000lb/hr)": float,
        "SLOAD": float,
        "SO2_MASS (lbs)": float,
        "SO2_MASS": float,
        "SO2_MASS_MEASURE_FLG": pd.StringDtype(),
        # "SO2_RATE (lbs/mmBtu)": float,  # Not reading from CSV
        # "SO2_RATE": float,  # Not reading from CSV
        # "SO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
        "NOX_RATE (lbs/mmBtu)": float,
        "NOX_RATE": float,
        "NOX_RATE_MEASURE_FLG": pd.StringDtype(),
        "NOX_MASS (lbs)": float,
        "NOX_MASS": float,
        "NOX_MASS_MEASURE_FLG": pd.StringDtype(),
        "CO2_MASS (tons)": float,
        "CO2_MASS": float,
        "CO2_MASS_MEASURE_FLG": pd.StringDtype(),
        # "CO2_RATE (tons/mmBtu)": float,  # Not reading from CSV
        # "CO2_RATE": float,  # Not reading from CSV
        # "CO2_RATE_MEASURE_FLG": str,  # Not reading from CSV
        "HEAT_INPUT (mmBtu)": float,
        "HEAT_INPUT": float,
        "FAC_ID": pd.Int64Dtype(),
        "UNIT_ID": pd.Int64Dtype(),
    }
    """dict: A dictionary containing column names (keys) and data types (values)
    for EPA CEMS.
    """

    RENAME_DICT = {
        "STATE": "state",
        # "FACILITY_NAME": "plant_name",  # Not reading from CSV
        "ORISPL_CODE": "plant_id_eia",
        "UNITID": "unitid",
        # These op_date, op_hour, and op_time variables get converted to
        # operating_date, operating_datetime and operating_time_interval in
        # transform/epacems.py
        "OP_DATE": "op_date",
        "OP_HOUR": "op_hour",
        "OP_TIME": "operating_time_hours",
        "GLOAD (MW)": "gross_load_mw",
        "GLOAD": "gross_load_mw",
        "SLOAD (1000 lbs)": "steam_load_1000_lbs",
        "SLOAD (1000lb/hr)": "steam_load_1000_lbs",
        "SLOAD": "steam_load_1000_lbs",
        "SO2_MASS (lbs)": "so2_mass_lbs",
        "SO2_MASS": "so2_mass_lbs",
        "SO2_MASS_MEASURE_FLG": "so2_mass_measurement_code",
        # "SO2_RATE (lbs/mmBtu)": "so2_rate_lbs_mmbtu",  # Not reading from CSV
        # "SO2_RATE": "so2_rate_lbs_mmbtu",  # Not reading from CSV
        # "SO2_RATE_MEASURE_FLG": "so2_rate_measure_flg",  # Not reading from CSV
        "NOX_RATE (lbs/mmBtu)": "nox_rate_lbs_mmbtu",
        "NOX_RATE": "nox_rate_lbs_mmbtu",
        "NOX_RATE_MEASURE_FLG": "nox_rate_measurement_code",
        "NOX_MASS (lbs)": "nox_mass_lbs",
        "NOX_MASS": "nox_mass_lbs",
        "NOX_MASS_MEASURE_FLG": "nox_mass_measurement_code",
        "CO2_MASS (tons)": "co2_mass_tons",
        "CO2_MASS": "co2_mass_tons",
        "CO2_MASS_MEASURE_FLG": "co2_mass_measurement_code",
        # "CO2_RATE (tons/mmBtu)": "co2_rate_tons_mmbtu",  # Not reading from CSV
        # "CO2_RATE": "co2_rate_tons_mmbtu",  # Not reading from CSV
        # "CO2_RATE_MEASURE_FLG": "co2_rate_measure_flg",  # Not reading from CSV
        "HEAT_INPUT (mmBtu)": "heat_content_mmbtu",
        "HEAT_INPUT": "heat_content_mmbtu",
        "FAC_ID": "facility_id",
        "UNIT_ID": "unit_id_epa",
    }
    """dict: A dictionary containing EPA CEMS column names (keys) and replacement
        names to use when reading those columns into PUDL (values).
    """

    def __init__(self, datastore: Datastore):
        """Create new EpaCemsDatastore wrapper."""
        self.datastore = datastore

    def get_data_frame(self, partition: EpaCemsPartition) -> pd.DataFrame:
        """Constructs dataframe holding data for a given (year, state) partition."""
        archive = self.datastore.get_zipfile_resource(
            "epacems", **partition.get_filters())
        dfs = []
        for month in range(1, 13):
            mf = partition.get_monthly_file(month)
            with archive.open(str(mf.with_suffix(".zip")), "r") as mzip:
                with ZipFile(mzip, "r").open(str(mf.with_suffix(".csv")), "r") as csv_file:
                    dfs.append(self._csv_to_dataframe(csv_file))
        return pd.concat(dfs, sort=True, copy=False, ignore_index=True)

    def _csv_to_dataframe(self, csv_file) -> pd.DataFrame:
        """
        Convert a CEMS csv file into a :class:`pandas.DataFrame`.

        Note that some columns are not read. See
        :mod:`pudl.constants.epacems_columns_to_ignore`. Data types for the columns
        are specified in :mod:`pudl.constants.epacems_csv_dtypes` and names of the
        output columns are set by :mod:`pudl.constants.epacems_rename_dict`.

        Args:
            csv_file: (file-like object): data to be read
        Returns:
            pandas.DataFrame: A DataFrame containing the contents of the
            CSV file.
        """
        df = pd.read_csv(
            csv_file,
            index_col=False,
            usecols=lambda col: col not in self.IGNORE_COLS,
        )
        dtypes = {}
        for col in df.columns:
            if col in self.CSV_DTYPES:
                dtypes[col] = self.CSV_DTYPES[col]

        # Preserve only those dtypes that are present in df
        for col in list(dtypes):
            if col not in df.columns:
                del dtypes[col]
        return df.astype(dtypes).rename(columns=self.RENAME_DICT)


def extract_epacems(partition: EpaCemsPartition) -> pd.DataFrame:
    """Extracts epacems dataframe for given year and state.

    Args:
        partition (EpacemsPartition): defines which partition (year, state)
        should be loaded.

    Returns:
        {fragment_name: pandas.DataFrame}
    """
    return EpaCemsDatastore(Datastore.from_prefect_context()).get_data_frame(partition)
