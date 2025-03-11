"""Retrieve data from Census PEP spreadsheets."""

import pandas as pd
from dagster import Output, asset

import pudl
import pudl.logging_helpers
from pudl.extract import excel
from pudl.extract.extractor import raw_df_factory

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.ExcelExtractor):
    """Extractor for the excel dataset Census PEP FIPS Codes."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.ExcelMetadata("censuspep")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns nullable string dtypes for all columns.

        FIPS codes often have leading zeros. This preserves them.
        """
        return pd.StringDtype()

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe."""
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
            self.cols_added.append("report_year")
        return df


raw_censuspep__all_dfs = raw_df_factory(Extractor, name="censuspep")


@asset
def raw_censuspep__geocodes(raw_censuspep__all_dfs):
    """Extract raw Census PEP FIPS codes data into dataframes.

    Returns:
        An extracted Census PEP FIPS codes dataframe.
    """
    return Output(value=raw_censuspep__all_dfs["geocodes"])
