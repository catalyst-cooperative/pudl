"""Retrieves data from EIA Form 923 spreadsheets for analysis.

This module pulls data from archived copies of EIA's published Excel spreadsheets.
"""

import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.extract.extractor import raw_df_factory
from pudl.helpers import remove_leading_zeros_from_numeric_strings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.ExcelExtractor):
    """Extractor for EIA form 923."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.ExcelMetadata("eia923")
        self.BLACKLISTED_PAGES = ["plant_frame"]
        self.cols_added = []
        super().__init__(*args, **kwargs)

    # Pages not supported by the metadata:
    # oil_stocks, coal_stocks, petcoke_stocks

    def process_raw(self, df, page, **partition):
        """Drops reserved columns."""
        to_drop = [c for c in df.columns if c[:8] == "reserved"]
        df = df.drop(to_drop, axis=1)
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        self.cols_added = []
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        # Fill in blank reporting_frequency_code for monthly data
        if "reporting_frequency_code" in df.columns:
            df.loc[
                df["data_maturity"] == "incremental_ytd", "reporting_frequency_code"
            ] = "M"
        # the 2021 early release data had some ding dang "."'s and nulls in the year column
        if "report_year" in df.columns:
            mask = (df.report_year == ".") | df.report_year.isnull()
            logger.debug(
                f"{page}: replacing {len(df[mask])} nulls/bad values in `report_year` "
                f"column with {partition['year']}"
            )
            df.loc[mask, "report_year"] = partition["year"]
        return df

    @staticmethod
    def process_renamed(df, page, **partition):
        """Cleans up unnamed_0 column in stocks page, drops invalid plan_id_eia rows."""
        if page == "stocks":
            df = df.rename(columns={"unnamed_0": "census_division_and_state"})
        # Drop the fields with plant_id_eia 99999 or 999999.
        # These are state index
        # Add leading zeros to county FIPS in fuel_receipts_costs
        else:
            if page == "fuel_receipts_costs":
                df.county_id_fips = df.county_id_fips.str.rjust(3, "0")
            df = df[~df.plant_id_eia.isin([99999, 999999])]
        return df

    def process_final_page(self, df, page):
        """Removes reserved columns from the final dataframe."""
        to_drop = [c for c in df.columns if c[:8] == "reserved"]
        df = df.drop(columns=to_drop, errors="ignore")
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns and county FIPS column."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
            "Coalmine County": pd.StringDtype(),
            "CoalMine_County": pd.StringDtype(),
            "Coalmine\nCounty": pd.StringDtype(),
        }


raw_eia923__all_dfs = raw_df_factory(Extractor, name="eia923")


@multi_asset(
    outs={
        table_name: AssetOut()
        for table_name in sorted(
            (
                "raw_eia923__boiler_fuel",
                "raw_eia923__energy_storage",
                "raw_eia923__fuel_receipts_costs",
                "raw_eia923__generation_fuel",
                "raw_eia923__generator",
                "raw_eia923__stocks",
                "raw_eia923__emissions_control",
                "raw_eia923__byproduct_disposition",
                "raw_eia923__byproduct_expenses_and_revenues",
                "raw_eia923__fgd_operation_maintenance",
                "raw_eia923__cooling_system_information",
                "raw_eia923__boiler_nox_operation",
                "raw_eia923__fgp_operation",
                "raw_eia923__puerto_rico_generation_fuel",
                "raw_eia923__puerto_rico_plant_frame",
            )
        )
    },
)
def extract_eia923(raw_eia923__all_dfs):
    """Extract raw EIA-923 data from excel sheets into dataframes."""
    # create descriptive table_names
    raw_eia923__all_dfs = {
        "raw_eia923__" + table_name: df
        for table_name, df in raw_eia923__all_dfs.items()
    }

    raw_eia923__all_dfs = dict(sorted(raw_eia923__all_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_eia923__all_dfs.items()
    )
