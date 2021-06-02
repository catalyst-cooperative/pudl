"""
Retrieve data from EIA Form 860M spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860M data. EIA 860M is only used in
conjunction with EIA 860. This module boths extracts EIA 860M and appends
the extracted EIA 860M dataframes to the extracted EIA 860 dataframes. Example
setup with pre-genrated `eia860_raw_dfs` and datastore as `ds`:

eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
    pc.working_partitions['eia860m']['year_month'])
eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
    eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)

"""
import logging
from datetime import datetime

import pandas as pd

from pudl.extract import excel
from pudl.helpers import fix_leading_zero_gen_ids

logger = logging.getLogger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA860M."""

    def __init__(self, *args, **kwargs):
        """
        Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata('eia860m')
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Adds source column and report_year column if missing."""
        df = df.rename(
            columns=self._metadata.get_column_map(page, **partition))
        if 'report_year' not in df.columns:
            df['report_year'] = datetime.strptime(
                list(partition.values())[0], "%Y-%m").year
        df = df.assign(data_source='eia860m')
        self.cols_added = ['data_source', 'report_year']
        df = fix_leading_zero_gen_ids(df)
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
        }


def append_eia860m(eia860_raw_dfs, eia860m_raw_dfs):
    """
    Append EIA 860M to the pages to.

    Args:
        eia860_raw_dfs (dictionary): dictionary of pandas.Dataframe's from EIA
            860 raw tables. Restult of
            pudl.extract.eia860.Extractor().extract()
        eia860m_raw_dfs (dictionary): dictionary of pandas.Dataframe's from EIA
            860M raw tables. Restult of
            pudl.extract.eia860m.Extractor().extract()

    Return:
        dictionary: augumented eia860_raw_dfs dictionary of pandas.DataFrame's.
        Each raw page stored in eia860m_raw_dfs appened to its eia860_raw_dfs
        counterpart.

    """
    meta_eia860m = excel.Metadata('eia860m')
    pages_eia860m = meta_eia860m.get_all_pages()
    # page names in 860m and 860 are the same.
    for page in pages_eia860m:
        eia860_raw_dfs[page] = eia860_raw_dfs[page].append(
            eia860m_raw_dfs[page], ignore_index=True, sort=True)
    return eia860_raw_dfs
