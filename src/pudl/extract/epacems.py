"""Retrieve data from EPA CEMS hourly zipped CSVs.

Presently, this module is where the CEMS columns are renamed and dropped. Any columns in
the IGNORE_COLS dictionary are excluded from the final output. All of these columns are
calculable rates, measurement flags, or descriptors (like facility name) that can be
accessed by merging this data with the EIA860 plants entity table. We also remove the
`FACILITY_ID` field because it is internal to the EPA's business accounting database and
`UNIT_ID` field because it's a unique (calculable) identifier for plant_id and
emissions_unit_id (previously `UNITID`) groupings. It took a minute to verify the
difference between the `UNITID` and `UNIT_ID` fields, but coorespondance with the EPA's
CAMD team cleared this up.

Pre-transform, the `plant_id_epa` field is a close but not perfect indicator for
`plant_id_eia`. In the raw data it's called `ORISPL_CODE` but that's not entirely
accurate. The epacamd_eia crosswalk will show that the mapping between `ORISPL_CODE` as
it appears in CEMS and the `plant_id_eia` field used in EIA data. Hense, we've called it
`plant_id_epa` until it gets transformed into `plant_id_eia` during the transform
process with help from the crosswalk.
"""
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

import pandas as pd

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)

# EPA CEMS constants #####
RENAME_DICT = {
    "STATE": "state",
    "FACILITY_NAME": "plant_name",  # Not reading from CSV
    "ORISPL_CODE": "plant_id_epa",  # Not quite the same as plant_id_eia
    "UNITID": "emissions_unit_id_epa",
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
    "SO2_RATE (lbs/mmBtu)": "so2_rate_lbs_mmbtu",  # Not reading from CSV
    "SO2_RATE": "so2_rate_lbs_mmbtu",  # Not reading from CSV
    "SO2_RATE_MEASURE_FLG": "so2_rate_measure_flg",  # Not reading from CSV
    "NOX_RATE (lbs/mmBtu)": "nox_rate_lbs_mmbtu",
    "NOX_RATE": "nox_rate_lbs_mmbtu",  # Not reading from CSV
    "NOX_RATE_MEASURE_FLG": "nox_rate_measurement_code",  # Not reading from CSV
    "NOX_MASS (lbs)": "nox_mass_lbs",
    "NOX_MASS": "nox_mass_lbs",
    "NOX_MASS_MEASURE_FLG": "nox_mass_measurement_code",
    "CO2_MASS (tons)": "co2_mass_tons",
    "CO2_MASS": "co2_mass_tons",
    "CO2_MASS_MEASURE_FLG": "co2_mass_measurement_code",
    "CO2_RATE (tons/mmBtu)": "co2_rate_tons_mmbtu",  # Not reading from CSV
    "CO2_RATE": "co2_rate_tons_mmbtu",  # Not reading from CSV
    "CO2_RATE_MEASURE_FLG": "co2_rate_measure_flg",  # Not reading from CSV
    "HEAT_INPUT (mmBtu)": "heat_content_mmbtu",
    "HEAT_INPUT": "heat_content_mmbtu",
    "FAC_ID": "facility_id",  # unique facility id for internal EPA database management
    "UNIT_ID": "unit_id_what",  # unique unit id for internal EPA database management
}
"""dict: A dictionary containing EPA CEMS column names (keys) and replacement
    names to use when reading those columns into PUDL (values). There are some
    duplicate rename values because the column names change year to year.
"""

# Any column that exactly matches one of these won't be read
IGNORE_COLS = {
    "FACILITY_NAME",
    "SO2_RATE (lbs/mmBtu)",
    "SO2_RATE",
    "SO2_RATE_MEASURE_FLG",
    "CO2_RATE (tons/mmBtu)",
    "CO2_RATE",
    "CO2_RATE_MEASURE_FLG",
    "NOX_RATE_MEASURE_FLG",
    "NOX_RATE",
    "NOX_RATE (lbs/mmBtu)",
    "FAC_ID",
    "UNIT_ID",
}
"""set: The set of EPA CEMS columns to ignore when reading data."""


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
        """Returns the filename (without suffix) that contains the monthly data."""
        return Path(f"{self.year}{self.state.lower()}{month:02}")


class EpaCemsDatastore:
    """Helper class to extract EpaCems resources from datastore.

    EpaCems resources are identified by a year and a state. Each of these zip files
    contain monthly zip files that in turn contain csv files. This class implements
    get_data_frame method that will concatenate tables for a given state and month
    across all months.
    """

    def __init__(self, datastore: Datastore):
        """Construct datastore wrapper for loading raw EPA CEMS data into dataframes."""
        self.datastore = datastore

    def get_data_frame(self, partition: EpaCemsPartition) -> pd.DataFrame:
        """Constructs dataframe holding data for a given (year, state) partition."""
        archive = self.datastore.get_zipfile_resource(
            "epacems", **partition.get_filters()
        )
        dfs = []
        for month in range(1, 13):
            mf = partition.get_monthly_file(month)
            with archive.open(str(mf.with_suffix(".zip")), "r") as mzip:
                with ZipFile(mzip, "r").open(
                    str(mf.with_suffix(".csv")), "r"
                ) as csv_file:
                    dfs.append(self._csv_to_dataframe(csv_file))
        return pd.concat(dfs, sort=True, copy=False, ignore_index=True)

    def _csv_to_dataframe(self, csv_file) -> pd.DataFrame:
        """Convert a CEMS csv file into a :class:`pandas.DataFrame`.

        Args:
            csv (file-like object): data to be read

        Returns:
            A DataFrame containing the contents of the CSV file.
        """
        return pd.read_csv(
            csv_file,
            index_col=False,
            usecols=lambda col: col not in IGNORE_COLS,
            low_memory=False,
        ).rename(columns=RENAME_DICT)


def extract(year: int, state: str, ds: Datastore):
    """Coordinate the extraction of EPA CEMS hourly DataFrames.

    Args:
        year: report year of the data to extract
        state: report state of the data to extract
        ds: Initialized datastore
    Yields:
        pandas.DataFrame: A single state-year of EPA CEMS hourly emissions data.
    """
    ds = EpaCemsDatastore(ds)
    partition = EpaCemsPartition(state=state, year=year)
    # We have to assign the reporting year for partitioning purposes
    return ds.get_data_frame(partition).assign(year=year)
