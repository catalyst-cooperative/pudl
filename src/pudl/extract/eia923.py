"""
Retrieves data from EIA Form 923 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 923 data. Currenly only
years 2009-2016 work, as they share nearly identical file formatting.
"""

import logging

import pudl.extract.excel as excel

logger = logging.getLogger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for EIA923 dataset.

    This loads Energy Information Administration (EIA) form 923
    """
    DATASET = 'eia923'
    BLACKLISTED_PAGES = ['plant_frame']

    def file_basename_glob(self, year, page):
        return '*2_3_4*'

    def process_raw(self, year, page, df):
        """Drop reserved columns."""
        to_drop = [c for c in df.columns if c[:8] == 'reserved']
        df.drop(to_drop, axis=1, inplace=True)
        return df

    def process_renamed(self, year, page, df):
        """Clean up stock column unnamed_0 and drop invalid plan_id_eia rows."""
        if page == 'stocks':
            df = df.rename(columns={'unnamed_0': 'census_division_and_state'})
        # Drop the fields with plant_id_eia 99999 or 999999.
        # These are state index
        if page != 'stocks':
            df = df[~df.plant_id_eia.isin([99999, 999999])]
        return df

    def process_final_page(self, page, df):
        """Removes reserved columns from the final dataframe."""
        to_drop = [c for c in df.columns if c[:8] == 'reserved']
        df.drop(columns=to_drop, inplace=True, errors='ignore')
        return df
