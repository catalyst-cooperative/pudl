"""Retrieve data from Census PEP spreadsheets."""

import pudl
import pudl.logging_helpers
from pudl.extract import excel

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

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe."""
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        self.cols_added = ["report_year"]
        return df
