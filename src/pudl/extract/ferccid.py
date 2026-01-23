"""This module extracts the raw FERC Company Identifier (CID) table."""

from io import BytesIO

import pandas as pd
from dagster import Output, asset

import pudl
from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(CsvExtractor):
    """Extractor for FERC CID."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("ferccid")
        super().__init__(*args, **kwargs)

    def load_source(
        self, partition: dict[str, str] = {"data_set": "data_table"}
    ) -> pd.DataFrame:
        """Produce the dataframe object.

        Args:
            partition: the data set partition to load from Zenodo

        Returns:
            pd.DataFrame instance containing CSV data
        """
        zf = self.ds.get_unique_resource(self._dataset_name, **partition)
        df = pd.read_csv(BytesIO(zf), **self.READ_CSV_KWARGS)
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
    df = Extractor(ds=ds).load_source()
    return Output(value=df)
