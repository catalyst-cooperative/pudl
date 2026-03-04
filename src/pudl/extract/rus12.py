"""Extract USDA RUS Form 12 data from CSVs."""

import pandas as pd
from dagster import asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, PartitionSelection, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for USDA RUS Form 12."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("rus12")
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

    def load_source(self, page: str, **partition: PartitionSelection) -> pd.DataFrame:
        """Produce the dataframe object for the given partition.

        In this instance we need to specify both year and geography to get the right zipfile.
        In 2010 there is a zipfile at the federal and state level, but for now we only want
        to use the federal-level data.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "data"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            pd.DataFrame instance containing CSV data
        """
        filename = self.source_filename(page, **partition)

        with (
            self.ds.get_zipfile_resource(self._dataset_name, **partition) as zf,
            zf.open(filename) as f,
        ):
            df = pd.read_csv(f, **self.READ_CSV_KWARGS)

        return df


raw_rus12__all_dfs = raw_df_factory(Extractor, name="rus12")


def raw_rus12_asset_factory(table_name: str):
    """Create raw RUS 12 asset for a specific page."""

    @asset(name=f"raw_rus12__{table_name}")
    def _raw_rus12__page(raw_rus12__all_dfs: dict[str, pd.DataFrame]):
        """Extract raw RUS Form 12 data from CSV sheets into dataframes.

        Returns:
            An extracted RUS Form 12 dataframe.
        """
        return raw_rus12__all_dfs[table_name]

    return _raw_rus12__page


raw_rus12_assets = [
    raw_rus12_asset_factory(table_name=table_name)
    for table_name in [
        "statement_of_operations",
        "balance_sheet",
        "sources_and_distribution",
        "renewable_plants",
        "plant_labor",
        "plant_factors_and_maximum_demand",
        "loans",
        "long_term_debt",
        "meeting_and_board",
        "lines_and_stations_labor_materials",
        "borrowers",
        "steam_plant_operations",
        "steam_plant_costs",
        "hydroelectric_plant_operations",
        "hydroelectric_plant_cost",
        "combined_cycle_plant_operations",
        "combined_cycle_plant_costs",
        "internal_combustion_plant_operations",
        "internal_combustion_plant_costs",
        "nuclear_plant_operations",
        "nuclear_plant_costs",
        "demand_and_energy_at_delivery_points",
        "demand_and_energy_at_power_sources",
        "investments",
        "loan_guarantees",
        "non_utility_plant",
        "ratio",
        "utility_plant",
        "utility_plant_accumulated_depreciation",
    ]
]
