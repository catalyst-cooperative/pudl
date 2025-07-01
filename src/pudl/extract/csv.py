"""Extractor for CSV data."""

# This module name clashes with the Python standard library CSV module!
# ruff: noqa: A005

from typing import Any

import pandas as pd

import pudl.logging_helpers
from pudl.extract.extractor import GenericExtractor, PartitionSelection

logger = pudl.logging_helpers.get_logger(__name__)


class CsvExtractor(GenericExtractor):
    """Class for extracting dataframes from CSV files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    READ_CSV_KWARGS: dict[str, Any] = {}
    """Keyword arguments that are passed to :meth:`pandas.read_csv`.

    These allow customization of the CSV parsing process. For example, you can specify
    the column delimiter, data types, date parsing, etc. This can greatly reduce peak
    memory usage and speed up the extraction process. Unfortunately you must refer to
    the column headers using their original names as they appear in the CSV.

    TODO[zaneselvans] 2024-04-19: it would be useful to be able to specify different CSV
    reading options for different pages within the same dataset. At the moment the same
    arguments will be applied to all pages. This still allows some flexibility because
    some :meth:`pandas.read_csv` arguments like ``dtype`` don't raise errors if the
    columns they apply to aren't present.
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
        return f"{self._dataset_name}_{partition_selection}.csv"

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

        with (
            self.ds.get_zipfile_resource(self._dataset_name, **partition) as zf,
            zf.open(filename) as f,
        ):
            df = pd.read_csv(f, **self.READ_CSV_KWARGS)

        return df
