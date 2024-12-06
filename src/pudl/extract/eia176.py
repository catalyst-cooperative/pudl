"""Extract EIA Form 176 data from CSVs."""

import pandas as pd
from dagster import Output, asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for EIA form 176."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("eia176")
        super().__init__(*args, **kwargs)

    def get_page_cols(self, page: str, partition_key: str) -> list[str]:
        """Get the columns for a particular page and partition key.

        EIA 176 data has the same set of columns for all years,
        so regardless of the partition key provided we select the same columns here.
        """
        return super().get_page_cols(page, "any_year")

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Append report year to df to distinguish data from other years."""
        self.cols_added.append("report_year")
        selection = self._metadata._get_partition_selection(partition)
        return df.assign(report_year=selection)

    def process_renamed(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Strip and lowercase raw text fields (except ID)."""
        text_fields = ["area", "atype", "company", "item"]
        for tf in text_fields:
            df[tf] = df[tf].str.strip().str.lower()
        return df


raw_eia176__all_dfs = raw_df_factory(Extractor, name="eia176")


@asset
def raw_eia176__data(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["data"])
