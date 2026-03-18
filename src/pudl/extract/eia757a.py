"""Extract EIA Form 757a data from CSVs."""

import pandas as pd
from dagster import Output, asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for EIA form 757a."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("eia757a")
        super().__init__(*args, **kwargs)

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Append report year and rename columns."""
        self.cols_added.append("report_year")
        selection = self._metadata._get_partition_selection(partition)
        return df.assign(report_year=selection).rename(
            columns=self._metadata.get_column_map(page=page, year="any_year")
        )


raw_eia757a__all_dfs = raw_df_factory(Extractor, name="eia757a")


@asset
def raw_eia757a__data(raw_eia757a__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 757a dataframe.
    """
    return Output(value=raw_eia757a__all_dfs["data"])
