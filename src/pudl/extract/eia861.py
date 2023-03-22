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
                "raw_advanced_metering_infrastructure_eia861",
                "raw_balancing_authority_eia861",
                "raw_delivery_companies_eia861",
                "raw_demand_response_eia861",
                "raw_demand_side_management_eia861",
                "raw_distributed_generation_eia861",
                "raw_distribution_systems_eia861",
                "raw_dynamic_pricing_eia861",
                "raw_energy_efficiency_eia861",
                "raw_frame_eia861",
                "raw_green_pricing_eia861",
                "raw_mergers_eia861",
                "raw_net_metering_eia861",
                "raw_non_net_metering_eia861",
                "raw_operational_data_eia861",
                "raw_reliability_eia861",
                "raw_sales_eia861",
                "raw_service_territory_eia861",
                "raw_short_form_eia861",
                "raw_utility_data_eia861",
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
