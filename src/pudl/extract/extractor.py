"""Generic functionality for extractors."""

import importlib.resources
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any

import pandas as pd
from dagster import (
    AssetsDefinition,
    DagsterType,
    DynamicOut,
    DynamicOutput,
    In,
    OpDefinition,
    TypeCheckContext,
    graph_asset,
    op,
)

import pudl
from pudl.workspace.datastore import Datastore

StrInt = str | int
PartitionSelection = list[StrInt] | tuple[StrInt] | StrInt

logger = pudl.logging_helpers.get_logger(__name__)


class GenericMetadata:
    """Load generic metadata from Python package data.

    When metadata object is instantiated, it is given ${dataset} name and it
    will attempt to load csv files from pudl.package_data.${dataset} package.

    It expects the following kinds of files:

    * column_map/${page}.csv currently informs us how to translate input column
      names to standardized pudl names for given (partition, input_col_name). Relevant
      page is encoded in the filename.
    """

    def __init__(self, dataset_name: str):
        """Create Metadata object and load metadata from python package.

        Args:
            dataset_name: Name of the package/dataset to load the metadata from.
            Files will be loaded from pudl.package_data.${dataset_name}
        """
        self._dataset_name = dataset_name
        self._pkg = f"pudl.package_data.{dataset_name}"
        column_map_pkg = self._pkg + ".column_maps"
        self._column_map = self._load_column_maps(column_map_pkg)

    def get_dataset_name(self) -> str:
        """Returns the name of the dataset described by this metadata."""
        return self._dataset_name

    def _load_csv(self, package: str, filename: str) -> pd.DataFrame:
        """Load metadata from a filename that is found in a package."""
        return pd.read_csv(
            importlib.resources.files(package) / filename, index_col=0, comment="#"
        )

    def _load_column_maps(self, column_map_pkg: str) -> dict:
        """Create a dictionary of all column mapping CSVs to use in get_column_map()."""
        column_dict = {}
        for res_path in importlib.resources.files(column_map_pkg).iterdir():
            # res_path is expected to end with ${page}.csv
            if res_path.suffix == ".csv":
                try:
                    column_map = self._load_csv(column_map_pkg, res_path.name)
                except pd.errors.ParserError as e:
                    raise AssertionError(
                        f"Expected well-formed column map file at {column_map_pkg} {res_path.name}"
                    ) from e
                column_dict[res_path.stem] = column_map
        return column_dict

    def _get_partition_selection(self, partition: dict[str, PartitionSelection]) -> str:
        """Grab the partition key."""
        partition_names = list(partition.keys())
        if len(partition_names) != 1:
            raise AssertionError(
                f"Expecting exactly one attribute to define this partition (found: {partition})"
            )

        partition_name = partition_names[0]
        partition_selection = partition[partition_name]
        if isinstance(partition_selection, list | tuple):
            raise AssertionError(
                f"Expecting exactly one non-container value for this partition attribute (found: {partition})"
            )
        return str(partition_selection)

    def get_all_pages(self) -> list[str]:
        """Returns list of all known pages."""
        return sorted(self._column_map.keys())

    def get_all_columns(self, page) -> list[str]:
        """Returns list of all pudl columns for a given page across all partitions."""
        return sorted(self._column_map[page].T.columns)

    def get_column_map(self, page, **partition):
        """Return dictionary for renaming columns in a given partition and page."""
        return {
            v: k
            for k, v in self._column_map[page]
            .T.loc[str(self._get_partition_selection(partition))]
            .to_dict()
            .items()
            if v != -1
        }


class GenericExtractor(ABC):
    """Generic extractor base class."""

    METADATA: GenericMetadata = None
    """Instance of metadata object to use with this extractor."""

    BLACKLISTED_PAGES = []
    """List of supported pages that should not be extracted."""

    def __init__(self, ds: Datastore):
        """Create new extractor object and load metadata.

        Args:
            ds: An initialized datastore, or subclass
        """
        if not self.METADATA:
            raise NotImplementedError("self.METADATA must be set.")
        self._metadata = self.METADATA
        self._dataset_name = self._metadata.get_dataset_name()
        self.ds = ds
        self.cols_added: list[str] = []

    @abstractmethod
    def source_filename(self, page: str, **partition: PartitionSelection) -> str:
        """Produce the source file name as it will appear in the archive.

        Args:
            page: pudl name for the dataset contents, eg "boiler_generator_assn" or
                "coal_stocks"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            string name of the source file
        """
        ...

    @abstractmethod
    def load_source(self, page: str, **partition: PartitionSelection) -> pd.DataFrame:
        """Produce the source data for the given page and partition(s).

        Args:
            page: pudl name for the dataset contents, eg
                "boiler_generator_assn" or "coal_stocks"
            partition: partition to load. Examples:
                {'year': 2009}
                {'year_month': '2020-08'}

        Returns:
            pd.DataFrame instance with the source data
        """
        ...

    def process_raw(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Takes any special steps for processing raw data and renaming columns."""
        return df.rename(columns=self._metadata.get_column_map(page, **partition))

    def process_renamed(
        self, df: pd.DataFrame, page: str, **partition: PartitionSelection
    ) -> pd.DataFrame:
        """Takes any special steps for processing data after columns are renamed."""
        return df

    def get_page_cols(self, page: str, partition_selection: str) -> pd.RangeIndex:
        """Get the columns for a particular page and partition key."""
        col_map = self._metadata._column_map[page]
        return col_map.loc[
            (col_map[partition_selection].notnull())
            & (col_map[partition_selection] != -1),
            [partition_selection],
        ].index

    def validate(self, df: pd.DataFrame, page: str, **partition: PartitionSelection):
        """Check if there are any missing or extra columns."""
        partition_selection = self._metadata._get_partition_selection(partition)
        page_cols = self.get_page_cols(page, partition_selection)
        expected_cols = page_cols.union(self.cols_added)
        if set(df.columns) != set(expected_cols):
            # Ensure that expected and actually extracted columns match
            extra_raw_cols = set(df.columns).difference(expected_cols)
            missing_raw_cols = set(expected_cols).difference(df.columns)
            if extra_raw_cols:
                raise ValueError(
                    f"{page}/{partition_selection}: Extra columns found in extracted table:"
                    f"\n{extra_raw_cols}"
                )
            if missing_raw_cols:
                raise ValueError(
                    f"{page}/{partition_selection}: Expected columns not found in extracted table:"
                    f"\n{missing_raw_cols}"
                )

    def process_final_page(self, df: pd.DataFrame, page: str) -> pd.DataFrame:
        """Final processing stage applied to a page DataFrame."""
        return df

    def combine(self, dfs: list[pd.DataFrame], page: str) -> pd.DataFrame:
        """Concatenate dataframes into one, take any special steps for processing final page."""
        df = pd.concat(dfs, sort=True, ignore_index=True)

        # After all years are loaded, add empty columns that could appear
        # in other years so that df matches the database schema
        missing_cols = list(
            set(self._metadata.get_all_columns(page)).difference(df.columns)
        )
        df = pd.concat([df, pd.DataFrame(columns=missing_cols)], sort=True)

        return self.process_final_page(df, page)

    def extract(self, **partitions: PartitionSelection) -> dict[str, pd.DataFrame]:
        """Extracts dataframes.

        Returns dict where keys are page names and values are
        DataFrames containing data across given years.

        Args:
            partitions: keyword argument dictionary specifying how the source is partitioned and which
                particular partitions to extract. Examples:
                {'years': [2009, 2010]}
                {'year_month': '2020-08'}
                {'form': 'gas_distribution', 'year'='2020'}
        """
        all_page_dfs = {}
        if not partitions:
            logger.warning(
                f"No partitions were given. Not extracting {self._dataset_name} "
                "spreadsheet data."
            )
            return all_page_dfs
        logger.info(f"Extracting {self._dataset_name} spreadsheet data.")

        for page in self._metadata.get_all_pages():
            if page in self.BLACKLISTED_PAGES:
                logger.debug(f"Skipping blacklisted page {page}.")
                continue
            current_page_dfs = [
                pd.DataFrame(),
            ]
            for partition in pudl.helpers.iterate_multivalue_dict(**partitions):
                # we are going to skip
                if self.source_filename(page, **partition) == "-1":
                    logger.debug(f"No page for {self._dataset_name} {page} {partition}")
                    continue
                logger.debug(
                    f"Loading dataframe for {self._dataset_name} {page} {partition}"
                )
                df = self.load_source(page, **partition)
                df = pudl.helpers.simplify_columns(df)
                df = self.process_raw(df, page, **partition)
                df = self.process_renamed(df, page, **partition)
                self.validate(df, page, **partition)
                current_page_dfs.append(df)

            all_page_dfs[page] = self.combine(current_page_dfs, page)
        return all_page_dfs


@op(tags={"memory-use": "high"})
def concat_pages(paged_dfs: list[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    """Concatenate similar pages of data from different years into single dataframes.

    Transform a list of dictionaries of dataframes into a single dictionary of
    dataframes, where each dataframe is the concatenation of dataframes with identical
    keys from the input list.

    For the relatively large EIA930 dataset this is a very memory-intensive operation,
    so the op is tagged with a high memory-use tag. For all the other datasets which use
    this op, the time spent concatenating pages is very brief, so this tag should not
    impact the overall concurrency of the DAG much.

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


def _is_dict_str_strint(_context: TypeCheckContext, x: Any) -> bool:
    if not isinstance(x, dict):
        return False
    for key, value in x.items():
        if not isinstance(key, str):
            return False
        if not isinstance(value, str | int):
            return False
    return True


# 2024-03-27: Dagster can't automatically convert union types within
# parametrized types; we have to write our own custom DagsterType for now.
dagster_dict_str_strint = DagsterType(
    name="dict[str, str | int]", type_check_fn=_is_dict_str_strint
)


def partition_extractor_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> OpDefinition:
    """Construct a Dagster op that extracts one partition of data, given an extractor.

    Args:
        extractor_cls: Class of type :class:`Extractor` used to extract the data.
        name: Name of an Excel based dataset (e.g. "eia860").
    """

    @op(
        required_resource_keys={"datastore"},
        name=f"extract_single_{name}_partition",
        ins={"part_dict": In(dagster_type=dagster_dict_str_strint)},
    )
    def extract_single_partition(
        context, part_dict: dict[str, str | int]
    ) -> dict[str, pd.DataFrame]:
        """A function that extracts a year of spreadsheet data from an Excel file.

        This function will be decorated with a Dagster op and returned.

        Args:
            context: Dagster keyword that provides access to resources and config.
            part_dict: Dictionary of partition name and partition to extract.

        Returns:
            A dictionary of DataFrames extracted from Excel/CSV, keyed by page name.
        """
        ds = context.resources.datastore
        return extractor_cls(ds).extract(**part_dict)

    return extract_single_partition


def partitions_from_settings_factory(name: str) -> OpDefinition:
    """Construct a Dagster op to get target partitions from settings in Dagster context.

    Args:
        name: Name of an Excel based dataset (e.g. "eia860").

    """

    @op(
        out=DynamicOut(),
        required_resource_keys={"dataset_settings"},
        name=f"{name}_partitions_from_settings",
    )
    def partitions_from_settings(context) -> DynamicOutput:
        """Produce target partitions for the given dataset from the dataset settings.

        These will be used to kick off worker processes to extract each year of data in
        parallel.

        Yields:
            A Dagster :class:`DynamicOutput` object representing the partition to be
            extracted. See the Dagster API documentation for more details:
            https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        """
        if "eia" in name:  # Account for nested settings if EIA
            partition_settings = context.resources.dataset_settings.eia
        else:
            partition_settings = context.resources.dataset_settings
        # Get year/year_quarter/half_year partition
        data_settings = getattr(partition_settings, name)  # Get dataset settings

        partition = [
            var
            for var in vars(data_settings)
            if any(
                date_partition in var
                for date_partition in ["years", "half_years", "year_quarters"]
            )
        ]
        assert len(partition) == 1, (
            f"Only one working partition is supported: {partition}."
        )
        partition = partition[0]
        parts = getattr(data_settings, partition)  # Get the actual values
        # In Zenodo we use "year", "half_year" as the partition, but in our settings
        # we use the plural "years". Drop the "s" at the end if present.
        partition = partition.removesuffix("s")
        for part in parts:
            yield DynamicOutput({partition: part}, mapping_key=str(part))

    return partitions_from_settings


def raw_df_factory(
    extractor_cls: type[GenericExtractor], name: str
) -> AssetsDefinition:
    """Return a dagster graph asset to extract raw DataFrames from CSV or Excel files.

    Args:
        extractor_cls: The dataset-specific CSV or Excel extractor used to extract the
            data. Must correspond to the dataset identified by ``name``.
        name: Name of a CSV or Excel based dataset (e.g. "eia860" or "eia930").
    """
    # Build a Dagster op that can extract a single year/half-year of data
    partition_extractor = partition_extractor_factory(extractor_cls, name)
    # Get the list of target partitions to extract from the PUDL ETL settings object
    # which is stored in the Dagster context that is available to all ops.
    partitions_from_settings = partitions_from_settings_factory(name)

    def raw_dfs() -> dict[str, pd.DataFrame]:
        """Produce a dictionary of extracted dataframes."""
        partitions = partitions_from_settings()
        logger.info(partitions)
        # Clone dagster op for each year using DynamicOut.map()
        # See https://docs.dagster.io/_apidocs/dynamic#dagster.DynamicOut
        dfs = partitions.map(lambda partition: partition_extractor(partition))
        # Collect the results from all of those cloned ops and concatenate the
        # individual years of data into a single multi-year dataframe for each different
        # page in the spreadsheet based dataset using DynamicOut.collect()
        return concat_pages(dfs.collect())

    return graph_asset(name=f"raw_{name}__all_dfs")(raw_dfs)
