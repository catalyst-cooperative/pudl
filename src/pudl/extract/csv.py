"""Extractor for CSV data."""
from csv import DictReader
from functools import lru_cache
from importlib import resources
from zipfile import ZipFile

import pandas as pd
from dagster import AssetsDefinition, OpDefinition, graph_asset, op

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class CsvArchive:
    """Represents API for accessing files within a single CSV archive."""

    def __init__(self, zipfile: ZipFile):
        """Constructs new instance of CsvArchive."""
        self.zipfile = zipfile

    def load_table(self, filename: str) -> pd.DataFrame:
        """Read the data from the CSV source and return as a dataframe."""
        logger.info(f"Extracting {filename} from CSV into pandas DataFrame.")
        with self.zipfile.open(filename) as f:
            df = pd.read_csv(f)
        return df


class CsvReader:
    """Wrapper to provide standardized access to CSV files."""

    def __init__(
        self,
        datastore: Datastore,
        dataset: str,
    ):
        """Create a new instance of CsvReader.

        This can be used for retrieving data from CSV files.

        Args:
            datastore: provides access to raw files on disk.
            dataset: name of the dataset (e.g. eia176), this is used to load metadata
                from package_data/{dataset} subdirectory.
        """
        self.datastore = datastore
        self.dataset = dataset
        self._table_file_map = {}
        for row in self._open_csv_resource("table_file_map.csv"):
            self._table_file_map[row["table"]] = row["filename"]

    def _open_csv_resource(self, base_filename: str) -> DictReader:
        """Open the given resource file as :class:`csv.DictReader`."""
        csv_path = resources.files(f"pudl.package_data.{self.dataset}") / base_filename
        return DictReader(csv_path.open())

    def get_table_names(self) -> list[str]:
        """Returns list of tables that this datastore provides access to."""
        return list(self._table_file_map)

    @lru_cache
    def get_archive(self) -> CsvArchive:
        """Returns a CsvArchive instance corresponding to the dataset."""
        return CsvArchive(self.datastore.get_zipfile_resource(self.dataset))


class CsvExtractor:
    """Generalized class for extracting and loading data from CSV files into SQL database.

    When subclassing from this generic extractor, one should implement dataset specific
    logic in the following manner:

    1. Set DATABASE_NAME class attribute. This controls what filename is used for the output
    sqlite database.
    2. Set DATASET class attribute. This is used to load metadata from package_data/{dataset} subdirectory.

    The extraction logic is invoked by calling extract() method of this class.
    """

    DATABASE_NAME = None
    DATASET = None

    def __init__(self, datastore: Datastore):
        """Constructs new instance of CsvExtractor.

        Args:
            datastore: top-level datastore instance for accessing raw data files.
        """
        self.csv_reader = self.get_csv_reader(datastore)

    def get_csv_reader(self, datastore: Datastore):
        """Returns instance of CsvReader to access the data."""
        return CsvReader(datastore, self.DATASET)

    def extract(self) -> dict[str, pd.DataFrame]:
        """Extracts a dictionary of table names and dataframes from CSV files."""
        data = {}
        for table in self.csv_reader.get_table_names():
            filename = self.csv_reader._table_file_map[table]
            df = self.csv_reader.get_archive().load_table(filename)
            data[table] = df
        return data


def extractor_factory(extractor_cls: type[CsvExtractor], name: str) -> OpDefinition:
    """Construct a Dagster op that extracts one year of data, given an extractor class.

    Args:
        extractor_cls: Class of type :class:`GenericExtractor` used to extract the data.
        name: Name of an Excel based dataset (e.g. "eia860").
    """

    def extract(context) -> dict[str, pd.DataFrame]:
        """A function that extracts data from a CSV file.

        This function will be decorated with a Dagster op and returned.

        Args:
            context: Dagster keyword that provides access to resources and config.

        Returns:
            A dictionary of DataFrames extracted from CSV, keyed by page name.
        """
        ds = context.resources.datastore
        return extractor_cls(ds).extract()

    return op(
        required_resource_keys={"datastore", "dataset_settings"},
        name=f"extract_single_{name}_year",
    )(extract)


def raw_df_factory(extractor_cls: type[CsvExtractor], name: str) -> AssetsDefinition:
    """Return a dagster graph asset to extract a set of raw DataFrames from CSV files.

    Args:
        extractor_cls: The dataset-specific CSV extractor used to extract the data.
            Needs to correspond to the dataset identified by ``name``.
        name: Name of an CSV based dataset (e.g. "eia176"). Currently this must be
            one of the attributes of :class:`pudl.settings.EiaSettings`
    """
    extractor = extractor_factory(extractor_cls, name)

    def raw_dfs() -> dict[str, pd.DataFrame]:
        """Produce a dictionary of extracted EIA dataframes."""
        return extractor()

    return graph_asset(name=f"{name}_raw_dfs")(raw_dfs)
