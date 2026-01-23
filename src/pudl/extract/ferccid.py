"""This module extracts the raw FERC Company Identifier (CID) table."""

from io import BytesIO

import pandas as pd
from dagster import Output, asset

from pudl.extract.extractor import GenericExtractor, GenericMetadata
from pudl.metadata.classes import DataSource


class Extractor(GenericExtractor):
    """Extractor for FERC Company Identifier table."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("ferccid")
        super().__init__(*args, **kwargs)

    def source_filename(self):  # noqa: D102
        pass

    def load_source(self, partition: dict[str, str]):
        """Produce the raw CSV data for the FERC CID table."""
        zf = self.ds.get_unique_resource(self._dataset_name, **partition)
        df = pd.read_csv(BytesIO(zf))
        return df

    def extract(self, partition: dict[str, str] = {"data_set": "data_table"}):
        """Extract FERC CID data table as a dataframe.

        Args:
            ds: DataStore object
            partition: Dictionary containing the data set partition to extract.

        Returns:
            DataFrame with the FERC CID table.
        """
        df = self.load_source(partition)
        partition_value = list(partition.values())[0]
        self.validate(df, page=partition_value, partition=partition_value)
        return df


@asset(
    required_resource_keys={"datastore"},
)
def raw_ferccid__data(
    context,
):
    """Extract raw FERC CID data from CSV files to one dataframe.

    Returns:
        An extracted FERC CID dataframe.
    """
    ds = context.resources.datastore
    partition = DataSource.from_id("ferccid").working_partitions
    df = Extractor(ds=ds).extract(partition=partition)
    return Output(value=df)
