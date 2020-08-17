"""
Retrieve data from EIA Form 861 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 861 data.

"""
import logging
import warnings

import pandas as pd

from pudl import constants as pc
from pudl.extract import excel as excel

logger = logging.getLogger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA861."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata('eia861')
        super().__init__(*args, **kwargs)

    def file_basename_glob(self, year, page):
        """Returns corresponding glob pattern for a page."""
        return self.PAGE_GLOBS[page]

    def process_raw(self, df, yr, page):
        """Rename columns with location."""
        warnings.warn(
            "Integration of EIA 861 into PUDL is still experimental and incomplete.\n"
            "The data has not yet been validated, and the structure may change."
        )
        column_map_numeric = self._metadata.get_column_map(yr, page)
        df = df.rename(
            columns=dict(zip(df.columns[list(column_map_numeric.keys())],
                             list(column_map_numeric.values()))))
        return df

    @staticmethod
    def process_renamed(df, year, page):
        """Adds report_year column if missing."""
        if 'report_year' not in df.columns:
            df['report_year'] = year
        return df

    @staticmethod
    def get_dtypes(year, page):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
            "zip_code": pc.column_dtypes['eia']['zip_code']}
