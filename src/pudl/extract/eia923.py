"""Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
"""
import pandas as pd

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.settings import Eia923Settings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for EIA form 923."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia923")
        self.BLACKLISTED_PAGES = ["plant_frame"]
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

    def extract(self, settings: Eia923Settings = Eia923Settings()):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            settings: Object containing validated settings
                relevant to EIA 923. Contains the tables and years to be loaded
                into PUDL.
        """
        return super().extract(year=settings.years)

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
