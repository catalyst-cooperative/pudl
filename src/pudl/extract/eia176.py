"""Extract EIA Form 176 data from CSVs.

The EIA Form 176 archive also contains CSVs for EIA Form 191 and EIA Form 757.
"""

from dagster import asset

from pudl.extract.csv import CsvExtractor, get_table_file_map

DATASET = "eia176"


@asset(required_resource_keys={"datastore"})
def raw_eia176__company(context):
    """Extract raw EIA company data from CSV sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        An extracted EIA dataframe with company data.
    """
    zipfile = context.resources.datastore.get_zipfile_resource(DATASET)
    table_file_map = get_table_file_map(DATASET)
    extractor = CsvExtractor(zipfile, table_file_map)
    extractor.extract_one("company")
