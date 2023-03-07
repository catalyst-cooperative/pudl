"""Retrieve data from EIA Form 861 spreadsheets for analysis.

This modules pulls data from EIA's published Excel spreadsheets.

This code is for use analyzing EIA Form 861 data.
"""
import warnings

import pandas as pd

import pudl.logging_helpers
from pudl.extract import excel
from pudl.helpers import remove_leading_zeros_from_numeric_strings
from pudl.settings import Eia861Settings

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA861."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("eia861")
        self.cols_added = []
        super().__init__(*args, **kwargs)
        warnings.warn(
            "Integration of EIA 861 into PUDL is still experimental and incomplete.\n"
            "The data has not yet been validated, and the structure may change."
        )

    def process_raw(self, df, page, **partition):
        """Rename columns with location."""
        column_map_numeric = self._metadata.get_column_map(page, **partition)
        df = df.rename(
            columns=dict(
                zip(
                    df.columns[list(column_map_numeric.keys())],
                    list(column_map_numeric.values()),
                )
            )
        )
        self.cols_added = []
        # Eventually we should probably make this a transform
        for col in ["generator_id", "boiler_id"]:
            if col in df.columns:
                df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        return df

    def extract(self, settings: Eia861Settings = Eia861Settings()):
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            settings: Object containing validated settings
                relevant to EIA 861. Contains the tables and years to be loaded
                into PUDL.
        """
        return super().extract(year=settings.years)

    @staticmethod
    def process_renamed(df, page, **partition):
        """Adds report_year column if missing."""
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
            "zip_code": pd.StringDtype(),
        }
