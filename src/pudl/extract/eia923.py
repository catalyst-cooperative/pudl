"""
Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
"""
import logging

import pandas as pd

from pudl.extract import excel as excel

logger = logging.getLogger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for EIA form 923."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata('eia923')
        self.BLACKLISTED_PAGES = ['plant_frame']
        super().__init__(*args, **kwargs)

    # Pages not supported by the metadata:
    # puerto_rico, github issue #457
    # energy_storage, github issue #458
    # oil_stocks, coal_stocks, petcoke_stocks

    def process_raw(self, df, year, page):
        """Drops reserved columns."""
        to_drop = [c for c in df.columns if c[:8] == 'reserved']
        df.drop(to_drop, axis=1, inplace=True)
        df = df.rename(columns=self._metadata.get_column_map(year, page))
        return df

    @staticmethod
    def process_renamed(df, year, page):
        """Cleans up unnamed_0 column in stocks page, drops invalid plan_id_eia rows."""
        if page == 'stocks':
            df = df.rename(columns={'unnamed_0': 'census_division_and_state'})
        # Drop the fields with plant_id_eia 99999 or 999999.
        # These are state index
        if page != 'stocks':
            df = df[~df.plant_id_eia.isin([99999, 999999])]
        return df

    @staticmethod
    def process_final_page(df, page):
        """Removes reserved columns from the final dataframe."""
        to_drop = [c for c in df.columns if c[:8] == 'reserved']
        df.drop(columns=to_drop, inplace=True, errors='ignore')
        return df

    @staticmethod
    def get_dtypes(year, page):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }
