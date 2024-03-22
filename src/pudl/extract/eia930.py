"""Extract EIA Form 930 data from CSVs."""

import pandas as pd
from dagster import Output, asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for EIA form 930."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("eia930")
        super().__init__(*args, **kwargs)

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the source file name as it will appear in the archive.

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
        return f"{self._dataset_name}-{partition_selection}-{page}.csv"

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Transforms raw dataframe and rename columns."""
        return df.rename(columns=self.METADATA.get_column_map(page, **partition))


raw_eia930__all_dfs = raw_df_factory(Extractor, name="eia930")


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia930__balance(raw_eia930__all_dfs):
    """Extract raw EIA 930 balance data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 930 balance dataframe.
    """
    return Output(value=raw_eia930__all_dfs["balance"])


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia930__interchange(raw_eia930__all_dfs):
    """Extract raw EIA 930 interchange data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 930 interchange dataframe.
    """
    return Output(value=raw_eia930__all_dfs["interchange"])


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia930__subregion(raw_eia930__all_dfs):
    """Extract raw EIA 930 subregion data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 930 subregion dataframe.
    """
    return Output(value=raw_eia930__all_dfs["subregion"])
