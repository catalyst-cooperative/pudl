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
entirely accurate. The core_epa__assn_eia_epacamd crosswalk will show that the mapping between
`Facility ID` as it appears in CEMS and the `plant_id_eia` field used in EIA data.
Hence, we've called it `plant_id_epa` until it gets transformed into `plant_id_eia`
during the transform process with help from the crosswalk.
"""

from pathlib import Path
from typing import Annotated

import pandas as pd
import polars as pl
from pydantic import BaseModel, StringConstraints

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)

########################################################################################
# EPA CEMS constants for API CSV files #####

API_RENAME_DICT = {
    "State": "state",
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
    "NOx Mass (lbs)": "nox_mass_lbs",
    "NOx Mass Measure Indicator": "nox_mass_measurement_code",
    "CO2 Mass (short tons)": "co2_mass_tons",
    "CO2 Mass Measure Indicator": "co2_mass_measurement_code",
    "Heat Input (mmBtu)": "heat_content_mmbtu",
    "Heat Input Measure Indicator": "heat_content_measure_flg",
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
    "CO2 Rate (short tons/mmBtu)",
    "CO2 Rate Measure Indicator",
    "NOx Rate (lbs/mmBtu)",
    "NOx Rate Measure Indicator",
    "Secondary Fuel Type",
    "Unit Type",
    "SO2 Controls",
    "NOx Controls",
    "PM Controls",
    "Hg Controls",
    "Program Code",
}
"""Set: The set of EPA CEMS columns to ignore when reading data."""

API_DTYPE_DICT = {
    "State": pl.datatypes.Categorical,
    "Facility ID": pl.datatypes.Int32,  # unique facility id for internal EPA database management (ORIS code)
    "Unit ID": pl.datatypes.String,
    "Associated Stacks": pl.datatypes.String,
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "Date": pl.datatypes.Date,
    "Hour": pl.datatypes.Int16,
    "Operating Time": pl.datatypes.Float32,
    "Gross Load (MW)": pl.datatypes.Float32,
    "Steam Load (1000 lb/hr)": pl.datatypes.Float32,
    "SO2 Mass (lbs)": pl.datatypes.Float32,
    "SO2 Mass Measure Indicator": pl.datatypes.Categorical,
    "NOx Mass (lbs)": pl.datatypes.Float32,
    "NOx Mass Measure Indicator": pl.datatypes.Categorical,
    "CO2 Mass (short tons)": pl.datatypes.Float32,
    "CO2 Mass Measure Indicator": pl.datatypes.Categorical,
    "Heat Input (mmBtu)": pl.datatypes.Float32,
    "Heat Input Measure Indicator": pl.datatypes.Categorical,
}


class EpaCemsPartition(BaseModel):
    """Represents EpaCems partition identifying unique resource file."""

    year_quarter: Annotated[
        str, StringConstraints(strict=True, pattern=r"^(19|20)\d{2}[q][1-4]$")
    ]

    @property
    def year(self):
        """Return the year associated with the year_quarter."""
        return pd.to_datetime(self.year_quarter).year

    def get_filters(self):
        """Returns filters for retrieving given partition resource from Datastore."""
        return {"year_quarter": self.year_quarter}

    def get_quarterly_file(self) -> Path:
        """Return the name of the CSV file that holds annual hourly data."""
        return Path(
            f"epacems-{self.year}q{pd.to_datetime(self.year_quarter).quarter}.csv"
        )


class EpaCemsDatastore:
    """Helper class to extract EpaCems resources from datastore.

    EpaCems resources are identified by a year and a quarter. Each of these zip files
    contains one csv file. This class implements get_data_frame method that will
    rename columns for a quarterly CSV file.
    """

    def __init__(self, datastore: Datastore):
        """Construct datastore wrapper for loading raw EPA CEMS data into dataframes."""
        self.datastore = datastore

    def get_data_frame(self, partition: EpaCemsPartition) -> pl.LazyFrame:
        """Constructs dataframe from a zipfile for a given (year_quarter) partition."""
        with (
            self.datastore.get_zipfile_resource(
                "epacems", **partition.get_filters()
            ) as zf,
            zf.open(str(partition.get_quarterly_file()), "r") as csv_file,
        ):
            lf = pl.scan_csv(csv_file, schema_overrides=API_DTYPE_DICT)
            lf = (
                lf.drop(list(set(lf.columns) - set(API_RENAME_DICT.keys())))
                .cast(API_DTYPE_DICT, strict=False)
                .rename(API_RENAME_DICT, strict=False)
            )
            return lf
