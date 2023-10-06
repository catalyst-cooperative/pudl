"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

from pudl.extract.csv import CsvExtractor


class Eia176CsvExtractor(CsvExtractor):
    """Extractor for EIA Form 176 data."""

    DATASET = "eia176"
    DATABASE_NAME = "eia176.sqlite"


# TODO: This was an alternative avenue of exploration; reconcile, clean-up
# def extract_eia176(context):
#     """Extract raw EIA data from excel sheets into dataframes.
#
#     Args:
#         context: dagster keyword that provides access to resources and config.
#
#     Returns:
#         A tuple of extracted EIA dataframes.
#     """
#     ds = context.resources.datastore
#     # TODO: Should I use this?
#     eia_settings = context.resources.dataset_settings.eia
#
#     for filename in EIA176_FILES:
#         raw_df = pudl.extract.csv.Extractor(ds).extract(
#             year_month=eia860m_date
#         )
#         eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
#             eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs
#         )
#
#     # create descriptive table_names
#     eia860_raw_dfs = {
#         "raw_eia860__" + table_name: df for table_name, df in eia860_raw_dfs.items()
#     }
#     eia860_raw_dfs = dict(sorted(eia860_raw_dfs.items()))
#
#     return (
#         Output(output_name=table_name, value=df)
#         for table_name, df in eia860_raw_dfs.items()
#     )
