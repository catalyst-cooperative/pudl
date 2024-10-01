"""Extract VCE renewable generation profile data from CSVs.

This dataset has 1,000s of columns, so we don't want to manually specify a rename on
import because we'll pivot these to a column. We adapt the standard extraction
infrastructure to simply read in the data.

Each zip folder contains a folder with three files:
Wind_Power_140m_Offshore_county.csv
Wind_Power_100m_Onshore_county.csv
Fixed_SolarPV_Lat_UPV_county.csv

The drive also contains one more file: RA_county_lat_long_FIPS_table.csv.
"""

import dask
from dagster import Output, asset
from dask import dataframe as dd

from pudl import logging_helpers
from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory

logger = logging_helpers.get_logger(__name__)


class VCEMetadata(GenericMetadata):
    """Special metadata class for VCE renewable generation profiles."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        super().__init__(*args, **kwargs)
        self._file_name = self._load_csv(self._pkg, "file_map.csv")

    def get_all_pages(self) -> list[str]:
        """Hard code the page names, which usually are pulled from column rename spreadsheets."""
        return [
            "offshore_wind_power_140m",
            "onshore_wind_power_100m",
            "fixed_solar_pv_lat_upv",
        ]

    def get_file_name(self, page, **partition):
        """Returns file name of given partition and page."""
        return self._file_name.loc[page, str(self._get_partition_selection(partition))]


class Extractor(CsvExtractor):
    """Extractor for VCE renewable generation profiles."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = VCEMetadata("vceregen")
        super().__init__(*args, **kwargs)

    def get_column_map(self, page, **partition):
        """Return empty dictionary, we don't rename these files."""
        return {}

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the CSV file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            string name of the CSV file
        """
        logger.warn(
            f"{partition['year']}/{self._metadata.get_file_name(page, **partition)}"
        )
        return f"{partition['year']}/{self._metadata.get_file_name(page, **partition)}"

    def load_source(self, page: str, **partition: PartitionSelection) -> dask.dataframe:
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
        filename = f"{partition['year']}/{self.source_filename(page, **partition)}"
        logger.warn(f"Opening file {filename}")

        with (
            self.ds.get_zipfile_resource(self._dataset_name, **partition) as zf,
        ):
            files = zf.namelist()
            file = next((x for x in files if filename in x), None)
            logger.warn(
                x for x in files if {self.source_filename(page, **partition)} in x
            )
            logger.warn(file)
            df = dd.read_csv(file, **self.READ_CSV_KWARGS)

        return df

    def process_raw(
        self, df: dask.dataframe, page: str, **partition: PartitionSelection
    ) -> dask.dataframe:
        """Append report year to df to distinguish data from other years."""
        self.cols_added.append("report_year")
        selection = self._metadata._get_partition_selection(partition)
        return df.assign(report_year=selection)

    def validate(
        self, df: dask.dataframe, page: str, **partition: PartitionSelection
    ) -> dask.dataframe:
        """Skip this step, as we aren't renaming any columns."""
        return df

    def combine(self, dfs: list[dask.dataframe], page: str) -> dask.dataframe:
        """Concatenate dataframes into one, take any special steps for processing final page."""
        df = dd.concat(dfs, sort=True, ignore_index=True)

        return self.process_final_page(df, page)


raw_vceregen__all_dfs = raw_df_factory(Extractor, name="vceregen")


@asset
def raw_vceregen__fixed_solar_pv_lat_upv(raw_vceregen__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_vceregen__all_dfs["fixed_solar_pv_lat_upv"])
