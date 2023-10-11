"""Retrieve data from PHMSA natural gas spreadsheets for analysis.

This modules pulls data from PHMSA's published Excel spreadsheets.

This code is for use analyzing PHMSA data.
"""
from collections import defaultdict

import pandas as pd
from dagster import (
    AssetOut,
    DynamicOut,
    DynamicOutput,
    Output,
    graph_asset,
    multi_asset,
    op,
)

import pudl
import pudl.logging_helpers
from pudl.extract import excel

logger = pudl.logging_helpers.get_logger(__name__)


class Extractor(excel.GenericExtractor):
    """Extractor for the excel dataset EIA860."""

    # TODO (e-belfer): Handle partitions, which aren't yearly.

    def __init__(self, *args, **kwargs):
        """Initialize the module.

        Args:
            ds (:class:datastore.Datastore): Initialized datastore.
        """
        self.METADATA = excel.Metadata("phmsagas")
        self.cols_added = []
        super().__init__(*args, **kwargs)

    def process_raw(self, df, page, **partition):
        """Apply necessary pre-processing to the dataframe.

        * Rename columns based on our compiled spreadsheet metadata
        * Add report_year if it is missing
        """
        df = df.rename(columns=self._metadata.get_column_map(page, **partition))
        if "report_year" not in df.columns:
            df["report_year"] = list(partition.values())[0]
        self.cols_added = ["report_year"]
        return df


# TODO (bendnorman): Add this information to the metadata
raw_table_names = ("raw_phmsa__distribution",)


@op(
    out=DynamicOut(),
    required_resource_keys={"dataset_settings"},
)
def phmsa_years_from_settings(context):
    """Return set of years for PHMSA in settings.

    These will be used to kick off worker processes to load each year of data in
    parallel.
    """
    phmsa_settings = context.resources.dataset_settings.phmsagas
    for year in phmsa_settings.years:
        yield DynamicOutput(year, mapping_key=str(year))


@op(
    required_resource_keys={"datastore", "dataset_settings"},
)
def load_single_phmsa_year(context, year: int) -> dict[str, pd.DataFrame]:
    """Load a single year of PHMSA data from file.

    Args:
        context:
            context: dagster keyword that provides access to resources and config.
        year:
            Year to load.

    Returns:
        Loaded data in a dataframe.
    """
    ds = context.resources.datastore
    return Extractor(ds).extract(year=[year])


@op
def merge_phmsa_years(
    yearly_dfs: list[dict[str, pd.DataFrame]]
) -> dict[str, pd.DataFrame]:
    """Merge yearly PHMSA dataframes."""
    merged = defaultdict(list)
    for dfs in yearly_dfs:
        for page in dfs:
            merged[page].append(dfs[page])

    for page in merged:
        merged[page] = pd.concat(merged[page])

    return merged


@graph_asset
def phmsa_raw_dfs() -> dict[str, pd.DataFrame]:
    """All loaded PHMSA dataframes.

    This asset creates a dynamic graph of ops to load EIA860 data in parallel.
    """
    years = phmsa_years_from_settings()
    dfs = years.map(lambda year: load_single_phmsa_year(year))
    return merge_phmsa_years(dfs.collect())


# TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_phmsa(context, phmsa_raw_dfs):
    """Extract raw PHMSA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    # create descriptive table_names
    phmsa_raw_dfs = {
        "raw_phmsa__" + table_name: df for table_name, df in phmsa_raw_dfs.items()
    }
    dict(sorted(phmsa_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in phmsa_raw_dfs.items()
    )
