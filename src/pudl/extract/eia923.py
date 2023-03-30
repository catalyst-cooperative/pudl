"""Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
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
        df.drop(to_drop, axis=1, inplace=True)
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        self.cols_added = []
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
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
        df.drop(columns=to_drop, inplace=True, errors="ignore")
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
    "raw_boiler_fuel_eia923",
    "raw_fuel_receipts_costs_eia923",
    "raw_generation_fuel_eia923",
    "raw_generator_eia923",
    "raw_stocks_eia923",
    # There's an issue with the EIA-923 archive for 2018 which prevents this table
    # from being extracted currently. When we update to a new DOI this problem will
    # probably fix itself. See comments on this issue:
    # https://github.com/catalyst-cooperative/pudl/issues/2448
    # "raw_emissions_control_eia923",
)


# TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(eia_raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia923(context):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    eia_settings = context.resources.dataset_settings.eia

    ds = context.resources.datastore
    eia923_raw_dfs = Extractor(ds).extract(year=eia_settings.eia923.years)

    # create descriptive table_names
    eia923_raw_dfs = {
        "raw_" + table_name + "_eia923": df for table_name, df in eia923_raw_dfs.items()
    }

    eia923_raw_dfs = dict(sorted(eia923_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia923_raw_dfs.items()
        # There's an issue with the EIA-923 archive for 2018 which prevents this table
        # from being extracted currently. When we update to a new DOI this problem will
        # probably fix itself. See comments on this issue:
        # https://github.com/catalyst-cooperative/pudl/issues/2448
        if table_name != "raw_emissions_control_eia923"
    )
