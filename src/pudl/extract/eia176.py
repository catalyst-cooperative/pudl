"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

import pandas as pd
from dagster import AssetsDefinition, asset

from pudl.extract.csv import CsvExtractor, get_table_file_map

DATASET = "eia176"


def eia_176_asset_factory(
    table_name: str,
) -> AssetsDefinition:
    """Build bulk electricity tables from raw data."""

    @asset(
        name=f"raw_eia176__yearly_{table_name}", required_resource_keys={"datastore"}
    )
    def eia_176_table(context) -> pd.DataFrame:
        """Extract raw EIA company data from CSV sheets into dataframes.

        Args:
            context: dagster keyword that provides access to resources and config.

        Returns:
            An extracted EIA dataframe.
        """
        zipfile = context.resources.datastore.get_zipfile_resource(DATASET)
        table_file_map = get_table_file_map(DATASET)
        extractor = CsvExtractor(zipfile, table_file_map)
        return extractor.extract_one(table_name)

    return eia_176_table


eia_176_assets = [
    eia_176_asset_factory(table_name=table_name)
    for table_name in ["company", "data", "other"]
]
