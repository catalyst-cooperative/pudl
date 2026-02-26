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

    def process_raw(self, df, page, **partition):
        """Adds source column and report_year column if missing."""
        df = super().process_raw(df, page, **partition)
        if "report_year" not in df.columns:
            df["report_year"] = int(list(partition.values())[0])
            self.cols_added.append("report_year")
        return df


raw_rus7__all_dfs = raw_df_factory(Extractor, name="rus7")


def raw_rus7_asset_factory(table_name: str):
    """Create raw RUS Form 7 asset for a specific page."""

    @asset(name=f"raw_rus7__{table_name}")
    def _raw_rus7__page(raw_rus7__all_dfs: dict[str, pd.DataFrame]):
        """Extract raw RUS Form 7 data from CSV sheets into dataframes.

        Returns:
            An extracted RUS Form 7 dataframe.
        """
        return raw_rus7__all_dfs[table_name]

    return _raw_rus7__page


raw_rus7_assets = [
    raw_rus7_asset_factory(table_name=table_name)
    for table_name in [
        "statement_of_operations",
        "balance_sheet",
        "borrowers",
        "employee_statistics",
        "patronage_capital",
        "meeting_and_board",
        "long_term_debt",
        "power_requirements",
        "energy_efficiency",
        "investments",
        "ratio",
        "loan_guarantees",
        "transmission_and_distribution",
        "service_interruptions",
        "owed_by_customers",
        "long_term_leases",
        "distribution_loans",
        "utility_plant_changes",
    ]
]
