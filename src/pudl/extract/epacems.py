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
from pydantic import BaseModel, StringConstraints

import pudl.logging_helpers
from pudl.metadata.classes import Resource
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

API_DTYPE_DICT = {
    "State": pd.CategoricalDtype(),
    "Facility Name": pd.StringDtype(),  # Not reading from CSV
    "Facility ID": pd.Int32Dtype(),  # unique facility id for internal EPA database management (ORIS code)
    "Unit ID": pd.StringDtype(),
    "Associated Stacks": pd.StringDtype(),
    # These op_date, op_hour, and op_time variables get converted to
    # operating_date, operating_datetime and operating_time_interval in
    # transform/epacems.py
    "Date": pd.StringDtype(),
    "Hour": pd.Int16Dtype(),
    "Operating Time": pd.Float32Dtype(),
    "Gross Load (MW)": pd.Float32Dtype(),
    "Steam Load (1000 lb/hr)": pd.Float32Dtype(),
    "SO2 Mass (lbs)": pd.Float32Dtype(),
    "SO2 Mass Measure Indicator": pd.CategoricalDtype(),
    "SO2 Rate (lbs/mmBtu)": pd.Float32Dtype(),  # Not reading from CSV
    "SO2 Rate Measure Indicator": pd.CategoricalDtype(),  # Not reading from CSV
    "NOx Rate (lbs/mmBtu)": pd.Float32Dtype(),  # Not reading from CSV
    "NOx Rate Measure Indicator": pd.CategoricalDtype(),  # Not reading from CSV
    "NOx Mass (lbs)": pd.Float32Dtype(),
    "NOx Mass Measure Indicator": pd.CategoricalDtype(),
    "CO2 Mass (short tons)": pd.Float32Dtype(),
    "CO2 Mass Measure Indicator": pd.CategoricalDtype(),
    "CO2 Rate (short tons/mmBtu)": pd.Float32Dtype(),  # Not reading from CSV
    "CO2 Rate Measure Indicator": pd.CategoricalDtype(),  # Not reading from CSV
    "Heat Input (mmBtu)": pd.Float32Dtype(),
    "Heat Input Measure Indicator": pd.CategoricalDtype(),
    "Primary Fuel Type": pd.CategoricalDtype(),
    "Secondary Fuel Type": pd.CategoricalDtype(),
    "Unit Type": pd.CategoricalDtype(),
    "SO2 Controls": pd.CategoricalDtype(),
    "NOx Controls": pd.CategoricalDtype(),
    "PM Controls": pd.CategoricalDtype(),
    "Hg Controls": pd.CategoricalDtype(),
    "Program Code": pd.CategoricalDtype(),
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

    def get_data_frame(self, partition: EpaCemsPartition) -> pd.DataFrame:
        """Constructs dataframe from a zipfile for a given (year_quarter) partition."""
        with (
            self.datastore.get_zipfile_resource(
                "epacems", **partition.get_filters()
            ) as zf,
            zf.open(str(partition.get_quarterly_file()), "r") as csv_file,
        ):
            df = self._csv_to_dataframe(
                csv_file,
                ignore_cols=API_IGNORE_COLS,
                rename_dict=API_RENAME_DICT,
                dtype_dict=API_DTYPE_DICT,
            )
        return df

    def _csv_to_dataframe(
        self,
        csv_path: Path,
        ignore_cols: dict[str, str],
        rename_dict: dict[str, str],
        dtype_dict: dict[str, type],
        chunksize: int = 100_000,
    ) -> pd.DataFrame:
        """Convert a CEMS csv file into a :class:`pandas.DataFrame`.

        Args:
            csv_path: Path to CSV file containing data to read.

        Returns:
            A DataFrame containing the filtered and dtyped contents of the CSV file.
        """
        chunk_iter = pd.read_csv(
            csv_path,
            index_col=False,
            usecols=lambda col: col not in ignore_cols,
            dtype=dtype_dict,
            chunksize=chunksize,
            low_memory=True,
            parse_dates=["Date"],
        )
        df = pd.concat(chunk_iter)
        dtypes = {k: v for k, v in dtype_dict.items() if k in df.columns}
        return df.astype(dtypes).rename(columns=rename_dict)


def extract(year_quarter: str, ds: Datastore) -> pd.DataFrame:
    """Coordinate the extraction of EPA CEMS hourly DataFrames.

    Args:
        year_quarter: report year and quarter of the data to extract
        ds: Initialized datastore
    Yields:
        A single quarter of EPA CEMS hourly emissions data.
    """
    ds = EpaCemsDatastore(ds)
    partition = EpaCemsPartition(year_quarter=year_quarter)
    year = partition.year
    # We have to assign the reporting year for partitioning purposes
    try:
        logger.info(f"Extracting data frame for {year_quarter}")
        df = ds.get_data_frame(partition).assign(year=year)
    # If the requested quarter is not found, return an empty df with expected columns:
    except KeyError:
        logger.warning(f"No data found for {year_quarter}. Returning empty dataframe.")
        res = Resource.from_id("core_epacems__hourly_emissions")
        df = res.format_df(pd.DataFrame())
    return df
