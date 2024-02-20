"""Extract EIA Form 176 data from CSVs."""

from dagster import AssetOut, Output, multi_asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, raw_df_factory

raw_table_names = "raw_eia176__data"


class Extractor(CsvExtractor):
    """Extractor for the CSV dataset EIA176."""

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


@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia176__data(raw_eia176__all_dfs):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    # create descriptive table_names
    raw_eia176__all_dfs = {
        "raw_eia860__" + table_name: df
        for table_name, df in raw_eia176__all_dfs.items()
    }
    raw_eia176__all_dfs = dict(sorted(raw_eia176__all_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_eia176__all_dfs.items()
    )
