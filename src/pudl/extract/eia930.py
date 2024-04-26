"""Extract EIA Form 930 data from CSVs."""

import pandas as pd
from dagster import asset

import pudl.logging_helpers
from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(CsvExtractor):
    """Extractor for EIA form 930."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("eia930")
        self.READ_CSV_KWARGS = {
            "thousands": ",",
            # Note that all of these date columns happen to be present in all of the
            # pages within the EIA-930 dataset, so we can specify them without needing
            # to specify separate arguments for each page (which isn't currently
            # implemented)
            "parse_dates": [
                "Data Date",
                "Local Time at End of Hour",
                "UTC Time at End of Hour",
            ],
            "dtype": {
                "Balancing Authority": "string",
                "Region": "string",
                "DIBA_Region": "string",
                "Directly Interconnected Balancing Authority": "string",
                "Sub-Region": "string",
            },
            "cache_dates": True,
        }
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
        # Subregion doesn't exist prior to 2018 half 2
        if page == "subregion" and (
            int(partition_selection[0:4]) < 2019 or partition_selection == "2018half1"
        ):
            return "-1"
        return f"{self._dataset_name}-{partition_selection}-{page}.csv"

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Transforms raw dataframe and rename columns."""
        return df.rename(columns=self.METADATA.get_column_map(page, **partition))


def raw_eia930_asset_factory(page: str):
    """Asset factory for individual raw EIA 930 dataframes."""

    @asset(
        name=f"raw_eia930__{page}",
        op_tags={"memory-use": "high"},
        compute_kind="pandas",
    )
    def _extract_raw_eia930(
        raw_eia930__all_dfs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Select a specific EIA 930 dataframe from the extracted raw dataframes.

        Returns:
            An extracted EIA 930 dataframe.
        """
        return raw_eia930__all_dfs[page]

    return _extract_raw_eia930


raw_eia930__all_dfs = raw_df_factory(Extractor, name="eia930")
raw_eia930_assets = [
    raw_eia930_asset_factory(page) for page in ["balance", "interchange", "subregion"]
]
