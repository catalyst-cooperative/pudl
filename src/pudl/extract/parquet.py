"""Extractor for Parquet data."""

import io

import pandas as pd

import pudl.logging_helpers
from pudl.extract.extractor import GenericExtractor, PartitionSelection

logger = pudl.logging_helpers.get_logger(__name__)


class ParquetExtractor(GenericExtractor):
    """Class for extracting dataframes from parquet files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the source Parquet file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "data"
            partition: partition to load. Examples:
                {'year': 2009}

        Returns:
            string name of the parquet file
        """
        partition_selection = self._metadata._get_partition_selection(partition)
        return f"{self._dataset_name}-{partition_selection}.parquet"

    def load_source(self, page: str, **partition: PartitionSelection) -> pd.DataFrame:
        """Produce the dataframe object for the given partition.

        This method assumes that the archive includes one unzipped file per partition.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "data"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            pd.DataFrame instance containing CSV data
        """
        res = self.ds.get_unique_resource(self._dataset_name, **partition)
        df = pd.read_parquet(io.BytesIO(res))
        return df
