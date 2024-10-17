"""Extract VCE Resource Adequacy Renewable Energy (RARE) Power Dataset.

This dataset has 1,000s of columns, so we don't want to manually specify a rename on
import because we'll pivot these to a column in the transform step. We adapt the
standard extraction infrastructure to simply read in the data.

Each annual zip folder contains a folder with three files:
Wind_Power_140m_Offshore_county.csv
Wind_Power_100m_Onshore_county.csv
Fixed_SolarPV_Lat_UPV_county.csv

The drive also contains one more CSV file: vce_county_lat_long_fips_table.csv. This gets
read in when the fips partition is set to True.
"""

from collections import defaultdict
from io import BytesIO

import numpy as np
import pandas as pd
from dagster import AssetsDefinition, Output, asset

from pudl import logging_helpers
from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory

logger = logging_helpers.get_logger(__name__)

VCERARE_PAGES = [
    "offshore_wind_power_140m",
    "onshore_wind_power_100m",
    "fixed_solar_pv_lat_upv",
]


class VCERareMetadata(GenericMetadata):
    """Special metadata class for VCE RARE Power Dataset."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        super().__init__(*args, **kwargs)
        self._file_name = self._load_csv(self._pkg, "file_map.csv")

    def _load_column_maps(self, column_map_pkg) -> dict:
        """There are no column maps to load, so return an empty dictionary."""
        return {}

    def get_all_pages(self) -> list[str]:
        """Hard code the page names, which usually are pulled from column rename spreadsheets."""
        return VCERARE_PAGES

    def get_file_name(self, page, **partition):
        """Returns file name of given partition and page."""
        return self._file_name.loc[page, str(self._get_partition_selection(partition))]


class Extractor(CsvExtractor):
    """Extractor for VCE RARE Power Dataset."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = VCERareMetadata("vcerare")
        super().__init__(*args, **kwargs)

    def get_column_map(self, page, **partition):
        """Return empty dictionary, we don't rename these files."""
        return {}

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the CSV file name as it will appear in the archive.

        The files are nested in an additional folder with the year name inside of the
        zipfile, so we add a prefix folder based on the yearly partition to the source
        filename.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            string name of the CSV file
        """
        return f"{partition['year']}/{self._metadata.get_file_name(page, **partition)}"

    def load_source(self, page: str, **partition: PartitionSelection) -> pd.DataFrame:
        """Produce the dataframe object for the given partition.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "data"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            pd.DataFrame instance containing CSV data
        """
        with (
            self.ds.get_zipfile_resource(self._dataset_name, **partition) as zf,
        ):
            # Get list of file names in the zipfile
            files = zf.namelist()
            # Get the particular file of interest
            file = next(
                (x for x in files if self.source_filename(page, **partition) in x), None
            )

            # Read it in using pandas
            # Set all dtypes except for the first unnamed hours column
            # to be float32 to reduce memory on read-in
            dtype_dict = defaultdict(lambda: np.float32)
            dtype_dict["Unnamed: 0"] = (
                "int"  # Set first unnamed column (hours) to be an integer.
            )

            df = pd.read_csv(BytesIO(zf.read(file)), dtype=dtype_dict)

        return df

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Append report year to df to distinguish data from other years."""
        self.cols_added.append("report_year")
        selection = self._metadata._get_partition_selection(partition)
        return df.assign(report_year=selection)

    def validate(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Skip this step, as we aren't renaming any columns."""
        return df

    def combine(self, dfs: list[pd.DataFrame], page: str) -> pd.DataFrame:
        """Concatenate dataframes into one, take any special steps for processing final page."""
        df = pd.concat(dfs, sort=True, ignore_index=True)

        return self.process_final_page(df, page)


raw_vcerare__all_dfs = raw_df_factory(Extractor, name="vcerare")


def raw_vcerare_asset_factory(part: str) -> AssetsDefinition:
    """An asset factory for VCE RARE Power Dataset."""
    asset_kwargs = {
        "name": f"raw_vcerare__{part}",
        "required_resource_keys": {"datastore", "dataset_settings"},
    }

    @asset(**asset_kwargs)
    def _extract(context, raw_vcerare__all_dfs):
        """Extract VCE RARE Power Dataset.

        Args:
            context: dagster keyword that provides access to resources and config.
        """
        return Output(value=raw_vcerare__all_dfs[part])

    return _extract


raw_vcerare_assets = [raw_vcerare_asset_factory(part) for part in VCERARE_PAGES]


@asset(required_resource_keys={"datastore", "dataset_settings"})
def raw_vcerare__lat_lon_fips(context) -> pd.DataFrame:
    """Extract lat/lon to FIPS and county mapping CSV.

    This dataframe is static, so it has a distinct partition from the other datasets and
    its extraction is controlled by a boolean in the ETL run.
    """
    ds = context.resources.datastore
    partition_settings = context.resources.dataset_settings.vcerare
    if partition_settings.fips:
        return pd.read_csv(
            BytesIO(ds.get_unique_resource("vcerare", fips=partition_settings.fips))
        )
    return pd.DataFrame()
