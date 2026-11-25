"""Routines used for extracting the raw NREL ATB data."""

import pandas as pd
from dagster import Output, asset

from pudl.extract.extractor import (
    GenericExtractor,
    GenericMetadata,
    PartitionSelection,
    raw_df_factory,
)
from pudl.workspace.datastore import Datastore


class Extractor(GenericExtractor[GenericMetadata]):
    """Extractor for NREL ATB."""

    def __init__(self, ds: Datastore):
        """Initialize the module.

        Args:
            ds: Initialized datastore.
        """
        super().__init__(ds, GenericMetadata("nrelatb"))

    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Get the file name for the right page and part."""
        return self._metadata.get_file_name(page, **partition)

    def load_source(self, page: str, **partition):
        """Fetch the electricity parquet file from the NREL ATB zip archive.

        This is based on the csv extraction framework.
        """
        filename = self.source_filename(page, **partition)

        with (
            self.ds.get_zipfile_resource(
                self._dataset_name, sector="electricity", **partition
            ) as zf,
            zf.open(filename) as f,
        ):
            return pd.read_parquet(f)


raw_nrelatb__all_dfs = raw_df_factory(Extractor, name="nrelatb")


@asset
def raw_nrelatb__data(raw_nrelatb__all_dfs):
    """Extract raw NREL ATB data from annual parquet files to one dataframe.

    Returns:
        An extracted NREL ATB dataframe.
    """
    return Output(value=raw_nrelatb__all_dfs["data"])
