"""Extract USDA RUS Form 7 data from CSVs."""

import pandas as pd
from dagster import asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for USDA RUS Form 7."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("rus7")
        # add this index_col arg bc if not read_csv assumes the first col
        # is the index and weirdly shifts the header names over its insufferable
        self.READ_CSV_KWARGS = {"index_col": False}
        super().__init__(*args, **kwargs)

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Get the file name for the right page and part.

        In this instance we are using the same methodology from the excel metadata extractor.
        """
        _file_name = self.METADATA._load_csv(self.METADATA._pkg, "file_map.csv")
        return _file_name.loc[
            str(self.METADATA._get_partition_selection(partition)), page
        ]


raw_rus7__all_dfs = raw_df_factory(Extractor, name="rus7")


def raw_rus7_asset_factory(in_page: str, out_page: str | None = None):
    """Create raw RUS 7 asset for a specific page."""
    out_page = in_page if out_page is None else out_page

    @asset(name=f"raw_rus7__{out_page}")
    def _raw_rus7__page(raw_rus7__all_dfs: dict[str, pd.DataFrame]):
        """Extract raw EIA 176 data from CSV sheets into dataframes.

        Returns:
            An extracted EIA 176 dataframe.
        """
        return raw_rus7__all_dfs[in_page]

    return _raw_rus7__page


raw_rus7_assets = [
    raw_rus7_asset_factory(in_page=in_page, out_page=out_page)
    for in_page, out_page in {
        "statement_of_operations": None,
        "balance_sheet": None,
        "borrowers": None,
        "employee_statistics": None,
        "patronage_capital": None,
        "meeting_and_board": None,
        "long_term_debt": None,
        "power_requirements": None,
        "energy_efficiency": None,
    }.items()
]
