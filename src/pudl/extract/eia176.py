"""Extract EIA Form 176 data from CSVs."""

from dagster import Output, asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, raw_df_factory


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
        """Get the columns for a particular page and partition key."""
        # The page columns for EIA176 do not vary by year
        return super().get_page_cols(page, "any_year")


raw_eia176__all_dfs = raw_df_factory(Extractor, name="eia176")


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia176__data(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        An extracted EIA 176 dataframe.
    """
    return Output(value=raw_eia176__all_dfs["data"])
