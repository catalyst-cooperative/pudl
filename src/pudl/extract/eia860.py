"""
Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""
import logging

import pandas as pd

from pudl.extract import excel as excel

logger = logging.getLogger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA860."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata('eia860')
        super().__init__(*args, **kwargs)

    def file_basename_glob(self, year, page):
        """Returns corresponding glob pattern for a page."""
        return self.PAGE_GLOBS[page]

    def process_raw(self, df, year, page):
        """Adds report_year column if missing."""
        df = df.rename(columns=self._metadata.get_column_map(year, page))
        if 'report_year' not in df.columns:
            df['report_year'] = year
        return df

    @staticmethod
    def get_dtypes(year, page):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }
