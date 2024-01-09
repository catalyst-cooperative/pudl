"""Retrieves data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA's published Excel spreadsheets.
"""


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
