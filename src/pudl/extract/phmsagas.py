"""Retrieves data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA's published Excel spreadsheets.
"""
import zipfile as zf

import pandas as pd

import pudl.logging_helpers
from pudl.extract import excel

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset PHMSA."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("phmsagas")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe.

        * Rename columns based on our compiled spreadsheet metadata
        * Add report_year if it is missing
        """
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]  # Fix
        self.cols_added = ["report_year"]
        # Eventually we should probably make this a transform
        # for col in ["generator_id", "boiler_id"]:
        #     if col in df.columns:
        #         df = remove_leading_zeros_from_numeric_strings(df=df, col_name=col)
        df = self.add_data_maturity(df, page, **partition)
        return df

    @staticmethod
    def get_dtypes(page, **partition):
        """Returns dtypes for plant id columns."""
        return {
            "Plant ID": pd.Int64Dtype(),
            "Plant Id": pd.Int64Dtype(),
        }

    def load_excel_file(self, page, **partition):
        """Produce the ExcelFile object for the given (partition, page).

        We adapt this method because PHMSA has multiple files per partition.

        Args:
            page (str): pudl name for the dataset contents, eg
                  "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or
                "2020-08" for year_month partition)

        Returns:
            pd.ExcelFile instance with the parsed excel spreadsheet frame
        """
        # Get all zipfiles for partitions
        files = self.ds.get_zipfile_resources(self._dataset_name, **partition)

        # For each zipfile, get a list of file names.
        for file_name, file in files:
            file_names = self.ds.get_zipfile_file_names(file)
            for xlsx_filename in file_names:
                if xlsx_filename not in self._file_cache and file.endswith(".xlsx"):
                    excel_file = pd.ExcelFile(zf.read(xlsx_filename))
                    self._file_cache[xlsx_filename] = excel_file

        return self._file_cache[xlsx_filename]  # FIX THIS, obviously.


# TODO (bendnorman): Add this information to the metadata
raw_table_names = ("raw_phmsagas__distribution",)

# phmsa_raw_dfs = excel.raw_df_factory(Extractor, name="phmsagas")


# # TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
# @multi_asset(
#     outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
#     required_resource_keys={"datastore", "dataset_settings"},
# )
# def extract_phmsagas(context, phmsa_raw_dfs):
#     """Extract raw PHMSA gas data from excel sheets into dataframes.

#     Args:
#         context: dagster keyword that provides access to resources and config.

#     Returns:
#         A tuple of extracted PHMSA gas dataframes.
#     """
#     ds = context.resources.datastore

#     # create descriptive table_names
#     phmsa_raw_dfs = {
#         "raw_phmsagas__" + table_name: df for table_name, df in phmsa_raw_dfs.items()
#     }
#     phmsa_raw_dfs = dict(sorted(phmsa_raw_dfs.items()))

#     return (
#         Output(output_name=table_name, value=df)
#         for table_name, df in phmsa_raw_dfs.items()
#     )
