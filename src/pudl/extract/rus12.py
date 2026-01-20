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


raw_rus12__all_dfs = raw_df_factory(Extractor, name="rus12")


def raw_rus12_asset_factory(in_page: str, out_page: str | None = None):
    """Create raw RUS 12 asset for a specific page."""
    out_page = in_page if out_page is None else out_page

    @asset(name=f"raw_rus12__{out_page}")
    def _raw_rus12__page(raw_rus12__all_dfs: dict[str, pd.DataFrame]):
        """Extract raw RUS 12 data from CSV sheets into dataframes.

        Returns:
            An extracted RUS 12 dataframe.
        """
        return raw_rus12__all_dfs[in_page]

    return _raw_rus12__page


raw_rus12_assets = [
    raw_rus12_asset_factory(in_page=in_page, out_page=out_page)
    for in_page, out_page in {
        "statement_of_operations": None,
        "balance_sheet": None,
        "sources_and_distribution": None,
        "renewable_plants": None,
        "plant_labor": None,
        "loans": None,
        "long_term_debt": None,
        "meeting_and_board": None,
        "lines_and_stations_labor_materials": None,
        "borrowers": None,
        "steam_plant_operations": None,
        "steam_plant_costs": None,
<<<<<<< HEAD
=======
        "hydroelectric_plant_operations": None,
        "hydroelectric_plant_cost": None,
        "combined_cycle_plant_operations": None,
        "combined_cycle_plant_costs": None,
        "internal_combustion_plant_operations": None,
        "internal_combustion_plant_costs": None,
        "nuclear_plant_operations": None,
        "nuclear_plant_costs": None,
<<<<<<< HEAD
        "demand_output_delivery_points": None,
        "demand_output_power_sources": None,
>>>>>>> eb417e254 (Rename extraction)
=======
        "demand_and_energy_at_delivery_points": None,
        "demand_and_energy_at_power_sources": None,
>>>>>>> 8c286aefc (Drop net, rename demand_and_energy tables, drop monthly prefix, clean up mills_per_kwh)
    }.items()
]
