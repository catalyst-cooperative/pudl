"""Retrieve data from EPA CEMS hourly zipped CSVs.

Prior to August 2023, this data was retrieved from an FTP server. After August 2023,
this data is now retrieved from the CEMS API. The format of the files has changed from
monthly CSVs for each state to one CSV per state per year. The names of the columns
have also changed. Column name compatibility was determined by reading the CEMS API
documentation on column names.

Presently, this module is where the CEMS columns are renamed and dropped. Any columns in
the IGNORE_COLS dictionary are excluded from the final output. All of these columns are
calculable rates, measurement flags, or descriptors (like facility name) that can be
accessed by merging this data with the EIA860 plants entity table. We also remove the
`FACILITY_ID` field because it is internal to the EPA's business accounting database.

Pre-transform, the `plant_id_epa` field is a close but not perfect indicator for
`plant_id_eia`. In the raw data it's called `Facility ID` (ORISPL code) but that's not
entirely accurate. The epacamd_eia crosswalk will show that the mapping between
`Facility ID` as it appears in CEMS and the `plant_id_eia` field used in EIA data.
Hence, we've called it `plant_id_epa` until it gets transformed into `plant_id_eia`
during the transform process with help from the crosswalk.
"""
from pathlib import Path
from typing import NamedTuple

import pandas as pd

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)

########################################################################################
# EPA CEMS constants for API CSV files #####

API_RENAME_DICT = {
    "State": "state",
    "Facility Name": "plant_name",  # Not reading from CSV
    "Facility ID": "plant_id_epa",  # unique facility id for internal EPA database management (ORIS code)
    "Unit ID": "emissions_unit_id_epa",
    "Associated Stacks": "associated_stacks",
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "Date": "op_date",
    "Hour": "op_hour",
    "Operating Time": "operating_time_hours",
    "Gross Load (MW)": "gross_load_mw",
    "Steam Load (1000 lb/hr)": "steam_load_1000_lbs",
    "SO2 Mass (lbs)": "so2_mass_lbs",
    "SO2 Mass Measure Indicator": "so2_mass_measurement_code",
    "SO2 Rate (lbs/mmBtu)": "so2_rate_lbs_mmbtu",  # Not reading from CSV
    "SO2 Rate Measure Indicator": "so2_rate_measure_flg",  # Not reading from CSV
    "NOx Rate (lbs/mmBtu)": "nox_rate_lbs_mmbtu",  # Not reading from CSV
    "NOx Rate Measure Indicator": "nox_rate_measurement_code",  # Not reading from CSV
    "NOx Mass (lbs)": "nox_mass_lbs",
    "NOx Mass Measure Indicator": "nox_mass_measurement_code",
    "CO2 Mass (short tons)": "co2_mass_tons",
    "CO2 Mass Measure Indicator": "co2_mass_measurement_code",
    "CO2 Rate (short tons/mmBtu)": "co2_rate_tons_mmbtu",  # Not reading from CSV
    "CO2 Rate Measure Indicator": "co2_rate_measure_flg",  # Not reading from CSV
    "Heat Input (mmBtu)": "heat_content_mmbtu",
    "Heat Input Measure Indicator": "heat_content_measure_flg",
    "Primary Fuel Type": "primary_fuel_type",
    "Secondary Fuel Type": "secondary_fuel_type",
    "Unit Type": "unit_type",
    "SO2 Controls": "so2_controls",
    "NOx Controls": "nox_controls",
    "PM Controls": "pm_controls",
    "Hg Controls": "hg_controls",
    "Program Code": "program_code",
}
"""Dict: A dictionary containing EPA CEMS column names (keys) and replacement names to
use when reading those columns into PUDL (values).

There are some duplicate rename values because the column names change year to year.
"""

# Any column that exactly matches one of these won't be read
API_IGNORE_COLS = {
    "Facility Name",
    "SO2 Rate (lbs/mmBtu)",
    "SO2 Rate Measure Indicator",
    "CO2 Rate (tons/mmBtu)",
    "CO2 Rate Measure Indicator",
    "NOx Rate (lbs/mmBtu)",
    "NOX Rate Measure Indicator",
    "Primary Fuel Type",
    "Secondary Fuel Type",
    "Unit Type",
    "SO2 Controls",
    "NOx Controls",
    "PM Controls",
    "Hg Controls",
    "Program Code",
}
"""Set: The set of EPA CEMS columns to ignore when reading data."""


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

    def get_annual_file(self) -> Path:
        """Return the name of the CSV file that holds annual hourly data."""
        return Path(f"epacems-{self.year}-{self.state.lower()}.csv")


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
        """Constructs dataframe from a zipfile for a given (year, state) partition."""
        archive = self.datastore.get_zipfile_resource(
            "epacems", **partition.get_filters()
        )

        # Get names of files in zip file
        files = self.datastore.get_zipfile_file_names(archive)
        logger.info(files)
        # If archive has one csv file in it, this is a yearly CSV (archived after 08/23)
        # and this CSV does not need to be concatenated.
        if len(files) == 1 and files[0].endswith(".csv"):
            with archive.open(str(partition.get_annual_file()), "r") as csv_file:
                df = self._csv_to_dataframe(
                    csv_file, ignore_cols=API_IGNORE_COLS, rename_dict=API_RENAME_DICT
                )
            return df
        else:
            raise AssertionError(f"Unexpected archive format. Found files: {files}.")

    def _csv_to_dataframe(
        self, csv_file: Path, ignore_cols: dict[str, str], rename_dict: dict[str, str]
    ) -> pd.DataFrame:
        """Convert a CEMS csv file into a :class:`pandas.DataFrame`.

        Args:
            csv (file-like object): data to be read

        Returns:
            A DataFrame containing the contents of the CSV file.
        """
        return pd.read_csv(
            csv_file,
            index_col=False,
            usecols=lambda col: col not in ignore_cols,
            low_memory=False,
        ).rename(columns=rename_dict)


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
