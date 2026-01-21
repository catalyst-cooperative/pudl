"""This module extracts the raw FERC Company Identifier (CID) table."""

from dagster import Output, asset

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata, raw_df_factory


class Extractor(CsvExtractor):
    """Extractor for FERC CID."""

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = GenericMetadata("ferccid")
        super().__init__(*args, **kwargs)


raw_ferccid__all_dfs = raw_df_factory(Extractor, name="ferccid")


@asset
def raw_ferccid__data(context, raw_ferccid__all_dfs):
    """Extract raw FERC CID data from CSV files to one dataframe.

    Returns:
        An extracted FERC CID dataframe.
    """
    # put a breakpoint here to see what the key values are
    # look at implementing source_filename in the Extractor class
    context.pdb.set_trace()
    return Output(value=raw_ferccid__all_dfs["data"])
