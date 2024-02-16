"""Extract EIA Form 176 data from CSVs."""

from dagster import AssetOut, Output, multi_asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import raw_df_factory

raw_table_names = "raw_eia176__data"


raw_eia176__all_dfs = raw_df_factory(CsvExtractor, name="eia176")


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
