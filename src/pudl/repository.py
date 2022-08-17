"""PUDL dagster repositories."""
from dagster import repository

from pudl.etl import dagster_etl
from pudl.extract.ferc1 import ferc1_to_sqlite


@repository
def pudl():
    """Define a dagster repository to hold pudl jobs."""
    return [dagster_etl, ferc1_to_sqlite]
