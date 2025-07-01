"""Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl
import pudl.logging_helpers
from pudl.extract import excel
from pudl.extract.extractor import raw_df_factory
from pudl.helpers import remove_leading_zeros_from_numeric_strings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.ExcelExtractor):
    """Extractor for the excel dataset EIA860."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.ExcelMetadata("eia860")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe.

        * Rename columns based on our compiled spreadsheet metadata
        * Add report_year if it is missing
        * Add a flag indicating if record came from EIA 860, or EIA 860M
        * Fix any generator_id values with leading zeroes.
        """
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        self.cols_added = ["report_year"]
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }


# TODO (bendnorman): Add this information to the metadata
raw_table_names = (
    "raw_eia860__boiler_cooling",
    "raw_eia860__boiler_generator_assn",
    "raw_eia860__boiler_info",
    "raw_eia860__boiler_mercury",
    "raw_eia860__boiler_nox",
    "raw_eia860__boiler_particulate",
    "raw_eia860__boiler_so2",
    "raw_eia860__boiler_stack_flue",
    "raw_eia860__cooling_equipment",
    "raw_eia860__emission_control_strategies",
    "raw_eia860__emissions_control_equipment",
    "raw_eia860__fgd_equipment",
    "raw_eia860__fgp_equipment",
    "raw_eia860__generator",
    "raw_eia860__generator_existing",
    "raw_eia860__generator_proposed",
    "raw_eia860__generator_retired",
    "raw_eia860__generator_energy_storage_existing",
    "raw_eia860__generator_energy_storage_proposed",
    "raw_eia860__generator_energy_storage_retired",
    "raw_eia860__generator_solar_existing",
    "raw_eia860__generator_solar_retired",
    "raw_eia860__generator_wind_existing",
    "raw_eia860__generator_wind_retired",
    "raw_eia860__multifuel_existing",
    "raw_eia860__multifuel_proposed",
    "raw_eia860__multifuel_retired",
    "raw_eia860__ownership",
    "raw_eia860__plant",
    "raw_eia860__stack_flue_equipment",
    "raw_eia860__utility",
)


raw_eia860__all_dfs = raw_df_factory(Extractor, name="eia860")


# TODO (bendnorman): Figure out type hint for context keyword and multi_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia860(context, raw_eia860__all_dfs):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    eia_settings = context.resources.dataset_settings.eia
    ds = context.resources.datastore

    if eia_settings.eia860.eia860m:
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            year_month=eia_settings.eia860.eia860m_year_months
        )
        raw_eia860__all_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=raw_eia860__all_dfs, eia860m_raw_dfs=eia860m_raw_dfs
        )

    # create descriptive table_names
    raw_eia860__all_dfs = {
        "raw_eia860__" + table_name: df
        for table_name, df in raw_eia860__all_dfs.items()
    }
    raw_eia860__all_dfs = dict(sorted(raw_eia860__all_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_eia860__all_dfs.items()
    )
