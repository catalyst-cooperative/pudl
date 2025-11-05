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

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the source file name as it will appear in the archive.

        Expects a string for page, and additionally a keyword argument dictionary
        specifying which particular partition to extract. Examples: {'year': 2009},
        {'year_month': '2020-08'}.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn", "data"

        Returns:
            string name of the CSV file
        """
        partition_selection = self._metadata._get_partition_selection(partition)
        # Company list doesn't exist in certain years
        if page == "company_list" and (
            int(partition_selection[0:4]) in [2000, 2001, 2019]
        ):
            return "-1"
        return f"{self._dataset_name}_{partition_selection}_{page}.csv"

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
def raw_eia176__custom(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["custom"])


@asset
def raw_eia176__company_list(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["company_list"])


@asset
def raw_eia176__operation_types_and_sector_items(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["types_of_operations_and_sector_items"])


@asset
def raw_eia176__continuation_text_lines(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["continuation_text_lines"])
