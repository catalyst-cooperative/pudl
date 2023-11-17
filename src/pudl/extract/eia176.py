"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

from dagster import AssetOut, Output, multi_asset

from pudl.extract.csv import CsvExtractor, raw_df_factory

DATASET = "eia176"


# TODO (davidmudrauskas): Add this information to the metadata
raw_table_names = (f"raw_{DATASET}__company",)

eia176_raw_dfs = raw_df_factory(CsvExtractor, name=DATASET)


@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia176(context, eia176_raw_dfs):
    """Extract EIA-176 data from CSV source and return dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    eia176_raw_dfs = {
        f"raw_{DATASET}__" + table_name: df for table_name, df in eia176_raw_dfs.items()
    }
    eia176_raw_dfs = dict(sorted(eia176_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia176_raw_dfs.items()
    )
