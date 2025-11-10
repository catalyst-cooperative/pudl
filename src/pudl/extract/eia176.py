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

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Append report year to df to distinguish data from other years."""
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        self.cols_added.append("report_year")
        selection = self._metadata._get_partition_selection(partition)
        return df.assign(report_year=selection)


raw_eia176__all_dfs = raw_df_factory(Extractor, name="eia176")


@asset
def raw_eia176__data(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["data"])
