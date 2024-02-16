"""Generic functionality for extractors."""
from collections import defaultdict

import pandas as pd
from dagster import (
    AssetsDefinition,
    DynamicOut,
    DynamicOutput,
    OpDefinition,
    graph_asset,
    op,
)


# TODO: Consider abstract class
class GenericExtractor:
    """Generic extractor base class."""

    def __init__(self, ds):
        """Create an instance of GenericExtractor.

        Args:
            ds (datastore.Datastore): An initialized datastore, or subclass
        """
        self.ds = ds


@op
def concat_pages(paged_dfs: list[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    """Concatenate similar pages of data from different years into single dataframes.

    Transform a list of dictionaries of dataframes into a single dictionary of
    dataframes, where each dataframe is the concatenation of dataframes with identical
    keys from the input list.

    Args:
        paged_dfs: A list of dictionaries whose keys are page names, and values are
            extracted DataFrames. Each element of the list corresponds to a single
            year of the dataset being extracted.

    Returns:
        A dictionary of DataFrames keyed by page name, where the DataFrame contains that
        page's data from all extracted years concatenated together.
    """
    # Transform the list of dictionaries of dataframes into a dictionary of lists of
    # dataframes, in which all dataframes in each list represent different instances of
    # the same page of data from different years
    all_data = defaultdict(list)
    for dfs in paged_dfs:
        for page in dfs:
            all_data[page].append(dfs[page])

    # concatenate the dataframes in each list in the dictionary into a single dataframe
    for page in all_data:
        all_data[page] = pd.concat(all_data[page]).reset_index(drop=True)

    return all_data


def year_extractor_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> OpDefinition:
    """Construct a Dagster op that extracts one year of data, given an extractor class.

    Args:
        extractor_cls: Class of type :class:`Extractor` used to extract the data.
        name: Name of an Excel based dataset (e.g. "eia860").
    """

    def extract_single_year(context, year: int) -> dict[str, pd.DataFrame]:
        """A function that extracts a year of spreadsheet data from an Excel file.

        This function will be decorated with a Dagster op and returned.

        Args:
            context: Dagster keyword that provides access to resources and config.
            year: Year of data to extract.

        Returns:
            A dictionary of DataFrames extracted from Excel, keyed by page name.
        """
        ds = context.resources.datastore
        return extractor_cls(ds).extract(year=[year])

    return op(
        required_resource_keys={"datastore", "dataset_settings"},
        name=f"extract_single_{name}_year",
    )(extract_single_year)


def years_from_settings_factory(name: str) -> OpDefinition:
    """Construct a Dagster op to get target years from settings in the Dagster context.

    Args:
        name: Name of an Excel based dataset (e.g. "eia860").

    """

    def years_from_settings(context) -> DynamicOutput:
        """Produce target years for the given dataset from the dataset settings object.

        These will be used to kick off worker processes to extract each year of data in
        parallel.

        Yields:
            A Dagster :class:`DynamicOutput` object representing the year to be
            extracted. See the Dagster API documentation for more details:
            https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        """
        if "eia" in name:  # Account for nested settings if EIA
            year_settings = context.resources.dataset_settings.eia
        else:
            year_settings = context.resources.dataset_settings
        for year in getattr(year_settings, name).years:
            yield DynamicOutput(year, mapping_key=str(year))

    return op(
        out=DynamicOut(),
        required_resource_keys={"dataset_settings"},
        name=f"{name}_years_from_settings",
    )(years_from_settings)


def raw_df_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> AssetsDefinition:
    """Return a dagster graph asset to extract a set of raw DataFrames from Excel files.

    Args:
        extractor_cls: The dataset-specific Excel extractor used to extract the data.
            Needs to correspond to the dataset identified by ``name``.
        name: Name of an Excel based dataset (e.g. "eia860").
    """
    # Build a Dagster op that can extract a single year of data
    year_extractor = year_extractor_factory(extractor_cls, name)
    # Get the list of target years to extract from the PUDL ETL settings object which is
    # stored in the Dagster context that is available to all ops.
    years_from_settings = years_from_settings_factory(name)

    def raw_dfs() -> dict[str, pd.DataFrame]:
        """Produce a dictionary of extracted dataframes."""
        years = years_from_settings()
        # Clone dagster op for each year using DynamicOut.map()
        # See https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        dfs = years.map(lambda year: year_extractor(year))
        # Collect the results from all of those cloned ops and concatenate the
        # individual years of data into a single multi-year dataframe for each different
        # page in the spreadsheet based dataset using DynamicOut.collect()
        return concat_pages(dfs.collect())

    return graph_asset(name=f"raw_{name}__all_dfs")(raw_dfs)
