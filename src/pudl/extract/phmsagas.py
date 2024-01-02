"""Retrieves data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA's published Excel spreadsheets.
"""

import pathlib
from io import BytesIO

import dbfread
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
        pkg = f"pudl.package_data.{self._dataset_name}"
        self.METADATA = excel.Metadata("phmsagas")
        self.cols_added = []
        self._form_name = self._load_csv(pkg, "form_map.csv")
        super().__init__(*args, **kwargs)

    def load_excel_file(self, page, **partition):
        """Produce the Excel file, loading the zipfile using the form and year.

        Args:
            page (str): pudl name for the dataset contents, eg
                  "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. (ex: 2009 for year partition or
                "2020-08" for year_month partition)

        Returns:
            pd.ExcelFile instance with the parsed excel spreadsheet frame
        """
        xlsx_filename = self.excel_filename(page, **partition)
        logger.info(xlsx_filename)

        if xlsx_filename not in self._file_cache:
            excel_file = None
            zf = self.ds.get_zipfile_resource(
                self._dataset_name, form=self._form_name[page], **partition
            )

            # If loading the excel file from the zip fails then try to open a dbf file.
            extension = pathlib.Path(xlsx_filename).suffix.lower()
            if extension == ".dbf":
                dbf_filepath = zf.open(xlsx_filename)
                df = pd.DataFrame(
                    iter(dbfread.DBF(xlsx_filename, filedata=dbf_filepath))
                )
                excel_file = pudl.helpers.convert_df_to_excel_file(df, index=False)
            else:
                excel_file = pd.ExcelFile(BytesIO(zf.read(xlsx_filename)))
            self._file_cache[xlsx_filename] = excel_file
        # TODO(rousik): this _file_cache could be replaced with @cache or @memoize annotations
        return self._file_cache[xlsx_filename]


# TODO (bendnorman): Add this information to the metadata
# raw_table_names = ("raw_phmsagas__distribution", "raw_phmsagas__transmission")

# phmsa_raw_dfs = excel.raw_df_factory(Extractor, name="phmsagas")

# @asset(out={"raw_phmsagas__distribution": AssetOut()},
#     required_resource_keys={"datastore", "dataset_settings"},)


# # TODO (bendnorman): Figure out type hint for context keyword and multi_asset return
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
