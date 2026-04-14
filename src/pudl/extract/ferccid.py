"""This module extracts the raw FERC Company Identifier (CID) table."""

from io import BytesIO

import pandas as pd
from dagster import asset

from pudl.metadata.classes import DataSource


@asset(
    required_resource_keys={"datastore"},
)
def raw_ferc__entity_companies(context):
    """Extract raw FERC CID data from CSV files to one dataframe.

    Returns:
        An extracted FERC CID dataframe.
    """
    partition = DataSource.from_id("ferccid").working_partitions
    zf = context.resources.datastore.get_unique_resource("ferccid", **partition)
    df = pd.read_csv(BytesIO(zf))
    return df
