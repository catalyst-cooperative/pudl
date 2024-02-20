"""Extractor for CSV data."""
import pandas as pd

import pudl.logging_helpers
from pudl.extract.extractor import GenericExtractor, PartitionSelection

logger = pudl.logging_helpers.get_logger(__name__)


class CsvExtractor(GenericExtractor):
    """Class for extracting dataframes from CSV files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the source CSV file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "data"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            string name of the CSV file
        """
        partition_selection = self._metadata._get_partition_selection(partition)
        return f"{self._dataset_name}-{partition_selection}.csv"

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
        filename = self.source_filename(page, **partition)

        with self.ds.get_zipfile_resource(self._dataset_name) as zf, zf.open(
            filename
        ) as f:
            df = pd.read_csv(f)

        return df
