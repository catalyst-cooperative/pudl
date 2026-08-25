"""Routines used for extracting the raw NREL ATB data."""

import pandas as pd
from dagster import Output, asset

from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory
from pudl.extract.parquet import ParquetExtractor


class Extractor(ParquetExtractor):
    """Extractor for NREL ATB."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("nrelatb")
        super().__init__(*args, **kwargs)

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Get the file name for the right page and part.

        In this instance we are using the same methodology from the excel metadata extractor.
        """
        _file_name = self.METADATA._load_csv(self.METADATA._pkg, "file_map.csv")
        return _file_name.loc[
            str(self.METADATA._get_partition_selection(partition)), page
        ]

    def load_source(self, page: str, **partition):
        """Fetch the electricity parquet file from the NREL ATB zip archive.

        This is based on the csv extraction framework.
        """
        filename = self.source_filename(page, **partition)

        for resource_key, zf in self.ds.get_zipfile_resources(
            self._dataset_name, **partition
        ):
            archive_name = str(resource_key).lower()
            if "electricity" not in archive_name:
                continue

            with zf.open(filename) as f:
                return pd.read_parquet(f)

        raise FileNotFoundError(
            f"No electricity parquet file found for {self._dataset_name} {partition}"
        )


raw_nrelatb__all_dfs = raw_df_factory(Extractor, name="nrelatb")


@asset
def raw_nrelatb__data(raw_nrelatb__all_dfs):
    """Extract raw NREL ATB data from annual parquet files to one dataframe.

    Returns:
        An extracted NREL ATB dataframe.
    """
    return Output(value=raw_nrelatb__all_dfs["data"])
