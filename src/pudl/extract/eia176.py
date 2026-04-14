"""Extract EIA Form 176 data from CSVs."""

import pandas as pd
from dagster import asset

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
        """Produce the source file name as it will appear in the ZIP archive.

        For this archive in particular, we control the naming of the CSV files because
        they are created by scraping EIA's natural gas query viewer interface. Rather
        than creating a file_map.csv like the other EIA extractors, we handle the one
        the missing company_list file in certain years here, returning "-1" since that
        mirrors the behavior of the other extractors that do rely on file_map.csv.

        Args:
            page: the name of the "page" within the dataset to extract. For EIA-176
                this is the descriptive portion of the name of one of the CSV files in
                the ZIP archive, e.g. "natural_gas_deliveries".
            partition: a dictionary uniquely identifying a partition to extract, e.g.
                {"year": "2019", "format": "by_report"}

        Returns:
            Full name of the CSV file within the ZIP archive as a string.
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
        # Some but not all of our input data has a report_year column,
        # so we assign it from the partition where it is not provided.
        if "report_year" not in df.columns:
            selection = self._metadata._get_partition_selection(partition)
            df = df.assign(report_year=selection)
            self.cols_added.append("report_year")
        return df


raw_eia176__all_dfs = raw_df_factory(Extractor, name="eia176")


def raw_eia176_asset_factory(in_page: str, out_page: str | None = None):
    """Create raw EIA 176 asset for a specific page."""
    out_page = in_page if out_page is None else out_page

    @asset(name=f"raw_eia176__{out_page}")
    def _raw_eia176__page(raw_eia176__all_dfs: dict[str, pd.DataFrame]):
        """Extract raw EIA 176 data from CSV sheets into dataframes.

        Returns:
            An extracted EIA 176 dataframe.
        """
        return raw_eia176__all_dfs[in_page]

    return _raw_eia176__page


raw_eia176_assets = [
    raw_eia176_asset_factory(in_page=in_page, out_page=out_page)
    for in_page, out_page in {
        "company_list": None,
        "continuation_text_lines": None,
        "custom": "numeric_data",
        "natural_gas_deliveries": None,
        "natural_gas_other_disposition_items": None,
        "natural_gas_supply_items": None,
        "operation_types_and_sector_items": None,
    }.items()
]
