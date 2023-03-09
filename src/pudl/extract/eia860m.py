"""Retrieve data from EIA Form 860M spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 860M data. EIA 860M is only used in
conjunction with EIA 860. This module boths extracts EIA 860M and appends
the extracted EIA 860M dataframes to the extracted EIA 860 dataframes. Example
setup with pre-genrated `eia860_raw_dfs` and datastore as `ds`:

eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
    Eia860Settings.eia860m_date)
eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
    eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs)
"""
from datetime import datetime

import pandas as pd

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.settings import Eia860Settings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA860M."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia860m")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Adds source column and report_year column if missing."""
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = datetime.strptime(
                list(partition.values())[0], "%Y-%m"
            ).year
        df = self.add_data_maturity(df, page, **partition)
        self.cols_added.append("report_year")
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        return df

    def extract(self, settings: Eia860Settings = Eia860Settings()):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            settings: Object containing validated settings
                relevant to EIA 860m. Contains the tables and date to be loaded
                into PUDL.
        """
        return super().extract(year_month=settings.eia860m_date)

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
        }


def append_eia860m(eia860_raw_dfs, eia860m_raw_dfs):
    """Append EIA 860M to the pages to.

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
    meta_eia860m = excel.Metadata("eia860m")
    pages_eia860m = meta_eia860m.get_all_pages()
    # page names in 860m and 860 are the same.
    for page in pages_eia860m:
        eia860_raw_dfs[page] = pd.concat(
            [eia860_raw_dfs[page], eia860m_raw_dfs[page]],
            ignore_index=True,
            sort=True,
        )
    return eia860_raw_dfs
