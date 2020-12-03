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

    def process_raw(self, df, page, **partition):
        """Adds data_source column and report_year column if missing."""
        df = df.rename(
            columns=self._metadata.get_column_map(page, **partition))
        if 'report_year' not in df.columns:
            df['report_year'] = list(partition.values())[0]
        self.cols_added = ['report_year']
        # if this is one of the EIA860M pages, add data_source
        meta_eia860m = excel.Metadata('eia860m')
        pages_eia860m = meta_eia860m.get_all_pages()
        if page in pages_eia860m:
            df = df.assign(data_source='eia860')
            self.cols_added.append('data_source')
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }
