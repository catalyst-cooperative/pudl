"""Routines used for extracting the raw NREL ATB data."""

from dagster import Output, asset

from pudl.extract.extractor import GenericMetadata, raw_df_factory
from pudl.extract.parquet import ParquetExtractor


class Extractor(ParquetExtractor):
    """Extractor for NREL ATB."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("nrelatb")
        super().__init__(*args, **kwargs)


raw_nrelatb__all_dfs = raw_df_factory(Extractor, name="nrelatb")


@asset
def raw_nrelatb__data(raw_nrelatb__all_dfs):
    """Extract raw NREL ATB data from annual parquet files to one dataframe.

    Returns:
        An extracted NREL ATB dataframe.
    """
    return Output(value=raw_nrelatb__all_dfs["data"])
