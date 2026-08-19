"""Retrieve data from EPA MATS hourly zipped CSVs.

The MATS data structure is similar to EPA CEMS but with different pollutants.
Each year's data is stored in a zip file (e.g., epamats-2015.zip) containing
quarterly CSV files (e.g., epamats-2015q1.csv). The data tracks hourly
Hg (mercury), HCl (hydrochloric acid), and HF (hydrogen fluoride) emissions
from coal-fired power plants.

Similar to CEMS, the `plant_id_epa` field (Facility ID in raw data) needs to be
mapped to `plant_id_eia` during transformation using the core_epa__assn_eia_epacamd
crosswalk.
"""

from pathlib import Path
from typing import Annotated

import pandas as pd
import polars as pl
from dagster import asset
from pydantic import BaseModel, StringConstraints

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)

########################################################################################
# EPA MATS column handling #####

RENAME_DICT = {
    "State": "state",
    "Facility Name": "plant_name_epa",
    "Facility ID": "plant_id_epa",  # EPA facility identifier (ORIS code)
    "Unit ID": "emissions_unit_id_epa",
    "Date": "op_date",
    "Hour": "op_hour",
    "Operating Time": "operating_time_hours",
    "MATS Gross Load (MW)": "gross_load_mw",
    "MATS Heat Input (mmBtu)": "heat_content_mmbtu",
    # Mercury (Hg)
    "Hg Output Rate (lb/GWh)": "hg_output_rate_lb_per_gwh",
    "Hg Input Rate (lb/TBtu)": "hg_input_rate_lb_per_tbtu",
    "Hg Mass (lbs)": "hg_mass_lbs",
    "Hg Mass Measure Indicator": "hg_mass_measurement_code",
    # Hydrogen Chloride (HCl)
    "HCl Output Rate (lb/MWh)": "hcl_output_rate_lb_per_mwh",
    "HCL Input Rate (lb/mmBtu)": "hcl_input_rate_lb_per_mmbtu",
    "HCl Mass (lbs)": "hcl_mass_lbs",
    "HCl Mass Measure Indicator": "hcl_mass_measurement_code",
    # Hydrogen Fluoride (HF)
    "HF Output Rate (lb/MWh)": "hf_output_rate_lb_per_mwh",
    "HF Input Rate (lb/mmBtu)": "hf_input_rate_lb_per_mmbtu",
    "HF Mass (lbs)": "hf_mass_lbs",
    "HF Mass Measure Indicator": "hf_mass_measurement_code",
    # Additional metadata
    "Associated Stacks": "associated_stacks",
    "Steam Load (1000 lb/hr)": "steam_load_1000_lbs",
    "Primary Fuel Type": "primary_fuel_type",
    "Secondary Fuel Type": "secondary_fuel_type",
    "Unit Type": "unit_type",
    "SO2 Controls": "so2_controls",
    "NOx Controls": "nox_controls",
    "PM Controls": "pm_controls",
    "Hg Controls": "hg_controls",
}
"""Dict: Mapping from raw EPA MATS column names to PUDL column names."""

DTYPE_DICT = {
    "State": pl.datatypes.Categorical,
    "Facility Name": pl.datatypes.String,
    "Facility ID": pl.datatypes.Int64,
    "Unit ID": pl.datatypes.String,
    "Date": pl.datatypes.Date,
    "Hour": pl.datatypes.Int16,
    "Operating Time": pl.datatypes.Float64,
    "MATS Gross Load (MW)": pl.datatypes.Float64,
    "MATS Heat Input (mmBtu)": pl.datatypes.Float64,
    "Hg Output Rate (lb/GWh)": pl.datatypes.Float64,
    "Hg Input Rate (lb/TBtu)": pl.datatypes.Float64,
    "Hg Mass (lbs)": pl.datatypes.Float64,
    "Hg Mass Measure Indicator": pl.datatypes.Categorical,
    "HCl Output Rate (lb/MWh)": pl.datatypes.Float64,
    "HCL Input Rate (lb/mmBtu)": pl.datatypes.Float64,
    "HCl Mass (lbs)": pl.datatypes.Float64,
    "HCl Mass Measure Indicator": pl.datatypes.Categorical,
    "HF Output Rate (lb/MWh)": pl.datatypes.Float64,
    "HF Input Rate (lb/mmBtu)": pl.datatypes.Float64,
    "HF Mass (lbs)": pl.datatypes.Float64,
    "HF Mass Measure Indicator": pl.datatypes.Categorical,
    "Associated Stacks": pl.datatypes.String,
    "Steam Load (1000 lb/hr)": pl.datatypes.Float64,
    "Primary Fuel Type": pl.datatypes.Categorical,
    "Secondary Fuel Type": pl.datatypes.Categorical,
    "Unit Type": pl.datatypes.Categorical,
    "SO2 Controls": pl.datatypes.Categorical,
    "NOx Controls": pl.datatypes.Categorical,
    "PM Controls": pl.datatypes.Categorical,
    "Hg Controls": pl.datatypes.Categorical,
}
"""Dict: Data types for EPA MATS columns."""


class EpaMatsPartition(BaseModel):
    """Represents a MATS partition identifying a unique quarterly resource file."""

    year_quarter: Annotated[
        str, StringConstraints(strict=True, pattern=r"^(19|20)\d{2}[q][1-4]$")
    ]

    @property
    def year(self):
        """Return the year associated with the year_quarter."""
        return pd.to_datetime(self.year_quarter).year

    @property
    def quarter(self):
        """Return the quarter associated with the year_quarter."""
        return pd.to_datetime(self.year_quarter).quarter

    def get_filters(self):
        """Returns filters for retrieving given partition resource from Datastore."""
        return {"year_quarter": self.year_quarter}

    def get_quarterly_file(self) -> Path:
        """Return the name of the CSV file within the zip that holds quarterly data."""
        return Path(f"epamats-{self.year}q{self.quarter}.csv")


class EpaMatsDatastore:
    """Helper class to extract MATS resources from datastore.

    MATS resources are identified by a year and a quarter. Each year's data is in
    a zip file containing 4 quarterly CSV files. This class implements get_data_frame
    method that will rename columns for a quarterly CSV file.
    """

    def __init__(self, datastore: Datastore):
        """Construct datastore wrapper for loading raw EPA MATS data into dataframes."""
        self.datastore = datastore

    def get_data_frame(self, partition: EpaMatsPartition) -> pl.LazyFrame:
        """Constructs dataframe from a zipfile for a given (year_quarter) partition."""
        with (
            self.datastore.get_zipfile_resource(
                "epamats", **partition.get_filters()
            ) as zf,
            zf.open(str(partition.get_quarterly_file()), "r") as csv_file,
        ):
            lf = pl.scan_csv(csv_file, low_memory=True, schema_overrides=DTYPE_DICT)
            lf = (
                lf.select(list(RENAME_DICT))
                # dict[str, ...] can't satisfy Mapping's invariant key type
                # against the union `.cast()` declares, even though `str` is
                # one of the union members -- a typeshed limitation, not a
                # real type mismatch.
                .cast(DTYPE_DICT, strict=False)  # type: ignore[bad-argument-type]
                .rename(RENAME_DICT, strict=False)
            )

        return lf


@asset(
    required_resource_keys={"datastore", "global_data_config"},
)
def raw_epamats__hourly_emissions(context) -> pd.DataFrame:
    """Extract raw EPA MATS hourly emissions data and return as a pandas DataFrame."""
    mats_config = context.resources.global_data_config.pudl.epamats
    epamats_datastore = EpaMatsDatastore(context.resources.datastore)
    frames = []
    for yq in mats_config.year_quarters:
        partition = EpaMatsPartition(year_quarter=yq)
        try:
            frames.append(
                epamats_datastore.get_data_frame(partition=partition)
                .with_columns(year=partition.year)
                .collect()
            )
        except KeyError:
            logger.warning(f"No data found for {yq}. Skipping.")
            continue
    return pl.concat(frames).to_pandas()
