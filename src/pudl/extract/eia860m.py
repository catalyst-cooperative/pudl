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
from dagster import AssetOut, Output, asset, multi_asset

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings

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
            df["report_date"] = pd.to_datetime(
                list(partition.values())[0], format="%Y-%m", exact=False
            )
        df = self.add_data_maturity(df, page, **partition)
        self.cols_added.append("report_year")
        self.cols_added.append("report_date")
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        return df

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
            [eia860_raw_dfs[page], eia860m_raw_dfs[page].drop(columns=["report_date"])],
            ignore_index=True,
            sort=True,
        )
    return eia860_raw_dfs


@asset(
    required_resource_keys={"datastore", "dataset_settings"},
)
def raw_eia860m__all_dfs(context):
    """Extract raw EIAm data from excel sheets into dict of dataframes."""
    eia_settings = context.resources.dataset_settings.eia
    ds = context.resources.datastore

    eia860m_extractor = Extractor(ds=ds)
    raw_eia860m__all_dfs = eia860m_extractor.extract(
        year_month=eia_settings.eia860m.year_months
    )
    return raw_eia860m__all_dfs


raw_table_names = (
    "raw_eia860m__generator_existing",
    "raw_eia860m__generator_proposed",
    "raw_eia860m__generator_retired",
)


@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia860m(raw_eia860m__all_dfs):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    # create descriptive table_names
    raw_eia860m__all_dfs = {
        "raw_eia860m__" + table_name: df
        for table_name, df in raw_eia860m__all_dfs.items()
    }
    raw_eia860m__all_dfs = dict(sorted(raw_eia860m__all_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in raw_eia860m__all_dfs.items()
    )
