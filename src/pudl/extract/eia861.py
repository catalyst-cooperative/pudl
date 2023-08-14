"""Retrieve data from EIA Form 861 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 861 data.
"""
import warnings

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA861."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia861")
        self.cols_added = []
        super().__init__(*args, **kwargs)
        warnings.warn(
            "Integration of EIA 861 into PUDL is still experimental and incomplete.\n"
            "The data has not yet been validated, and the structure may change."
        )

    def process_raw(self, df, page, **partition):
        """Rename columns with location."""
        column_map_numeric = self._metadata.get_column_map(page, **partition)
        df = df.rename(
            columns=dict(
                zip(
                    df.columns[list(column_map_numeric.keys())],
                    list(column_map_numeric.values()),
                )
            )
        )
        self.cols_added = []
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        return df

    @staticmethod
    def process_renamed(df, page, **partition):
        """Adds report_year column if missing."""
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
            "zip_code": pd.StringDtype(),
        }


@multi_asset(
    outs={
        table_name: AssetOut()
        for table_name in sorted(
            (  # is there some way to programmatically generate this list?
                "raw_eia861__advanced_metering_infrastructure",
                "raw_eia861__balancing_authority",
                "raw_eia861__delivery_companies",
                "raw_eia861__demand_response",
                "raw_eia861__demand_side_management",
                "raw_eia861__distributed_generation",
                "raw_eia861__distribution_systems",
                "raw_eia861__dynamic_pricing",
                "raw_eia861__energy_efficiency",
                "raw_eia861__frame",
                "raw_eia861__green_pricing",
                "raw_eia861__mergers",
                "raw_eia861__net_metering",
                "raw_eia861__non_net_metering",
                "raw_eia861__operational_data",
                "raw_eia861__reliability",
                "raw_eia861__sales",
                "raw_eia861__service_territory",
                "raw_eia861__short_form",
                "raw_eia861__utility_data",
            )
        )
    },
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia861(context):
    """Extract raw EIA-861 data from Excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA-861 dataframes.
    """
    eia_settings = context.resources.dataset_settings.eia
    ds = context.resources.datastore
    eia861_raw_dfs = Extractor(ds).extract(year=eia_settings.eia861.years)

    eia861_raw_dfs = {
        "raw_" + table_name: df for table_name, df in eia861_raw_dfs.items()
    }
    eia861_raw_dfs = dict(sorted(eia861_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia861_raw_dfs.items()
    )
