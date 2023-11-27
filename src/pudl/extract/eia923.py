"""Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only years 2009-2016 work, as
they share nearly identical file formatting.
"""
import pandas as pd
from dagster import AssetOut, Output, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for EIA form 923."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia923")
        # There's an issue with the EIA-923 archive for 2018 which prevents this table
        # from being extracted currently. When we update to a new DOI this problem will
        # probably fix itself. See comments on this issue:
        # https://github.com/catalyst-cooperative/pudl/issues/2448
        self.BLACKLISTED_PAGES = ["plant_frame", "emissions_control"]
        self.cols_added = []
        super().__init__(*args, **kwargs)

    # Pages not supported by the metadata:
    # puerto_rico, github issue #457
    # energy_storage, github issue #458
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
        df = self.add_data_maturity(df, page, **partition)
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

    @staticmethod
    def process_final_page(df, page):
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


# TODO (bendnorman): Add this information to the metadata
eia_raw_table_names = (
    "raw_eia923__boiler_fuel",
    "raw_eia923__fuel_receipts_costs",
    "raw_eia923__generation_fuel",
    "raw_eia923__generator",
    "raw_eia923__stocks",
    # There's an issue with the EIA-923 archive for 2018 which prevents this table
    # from being extracted currently. When we update to a new DOI this problem will
    # probably fix itself. See comments on this issue:
    # https://github.com/catalyst-cooperative/pudl/issues/2448
    # "raw_emissions_control_eia923",
)


eia923_raw_dfs = excel.raw_df_factory(Extractor, name="eia923")


# TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(eia_raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia923(context, eia923_raw_dfs):
    """Extract raw EIA-923 data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    # create descriptive table_names
    eia923_raw_dfs = {
        "raw_eia923__" + table_name: df for table_name, df in eia923_raw_dfs.items()
    }

    eia923_raw_dfs = dict(sorted(eia923_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia923_raw_dfs.items()
        # There's an issue with the EIA-923 archive for 2018 which prevents this table
        # from being extracted currently. When we update to a new DOI this problem will
        # probably fix itself. See comments on this issue:
        # https://github.com/catalyst-cooperative/pudl/issues/2448
        if table_name != "raw_eia923__emissions_control"
    )
