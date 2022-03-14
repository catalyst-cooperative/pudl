"""
Retrieve data from EIA Form 860 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860 data.
"""
import logging

import pandas as pd

from pudl.extract import excel
from pudl.helpers import fix_leading_zero_gen_ids
from pudl.settings import Eia860Settings

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
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """
        Apply necessary pre-processing to the dataframe.

        * Rename columns based on our compiled spreadsheet metadata
        * Add report_year if it is missing
        * Add a flag indicating if record came from EIA 860, or EIA 860M
        * Fix any generator_id values with leading zeroes.

        """
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
        df = fix_leading_zero_gen_ids(df)
        return df

    def extract(self, settings: Eia860Settings = Eia860Settings()):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            settings: Object containing validated settings
                relevant to EIA 860. Contains the tables and years to be loaded
                into PUDL.
        """
        return super().extract(year=settings.years)

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }
