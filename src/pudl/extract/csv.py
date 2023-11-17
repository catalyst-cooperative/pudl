"""Extractor for CSV data."""
from csv import DictReader
from importlib import resources

import pandas as pd
from dagster import AssetsDefinition, OpDefinition, graph_asset, op

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class CsvExtractor:
    """Generalized class for extracting dataframes from CSV files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    def __init__(self, datastore: Datastore, dataset: str):
        """Create a new instance of CsvExtractor.

        This can be used for retrieving data from CSV files.

        Args:
            datastore: provides access to raw files on disk.
            dataset: used to load metadata from package_data/{dataset} subdirectory.
        """
        self.dataset = dataset
        self._zipfile = datastore.get_zipfile_resource(dataset)
        self._table_file_map = {
            row["table"]: row["filename"]
            for row in self._open_csv_resource("table_file_map.csv")
        }

    def _open_csv_resource(self, base_filename: str) -> DictReader:
        """Open the given resource file as :class:`csv.DictReader`."""
        csv_path = resources.files(f"pudl.package_data.{self.dataset}") / base_filename
        return DictReader(csv_path.open())

    def get_table_names(self) -> list[str]:
        """Returns list of tables that this extractor provides access to."""
        return list(self._table_file_map)

    def extract_one(self, table_name: str) -> pd.DataFrame:
        """Read the data from the CSV source file and return as a dataframe."""
        logger.info(f"Extracting {table_name} from CSV into pandas DataFrame.")
        filename = self._table_file_map[table_name]
        with self._zipfile.open(filename) as f:
            df = pd.read_csv(f)
        return df

    def extract_all(self) -> dict[str, pd.DataFrame]:
        """Extracts a dictionary of table names and dataframes from CSV source files."""
        data = {}
        for table_name in self.get_table_names():
            df = self.extract_one(table_name)
            data[table_name] = df
        return data


def extractor_factory(extractor_cls: type[CsvExtractor], name: str) -> OpDefinition:
    """Construct a Dagster op that extracts data given an extractor class.

    Args:
        extractor_cls: Class of type :class:`CsvExtractor` used to extract the data.
        name: Name of a CSV-based dataset (e.g. "eia176").
    """

    def extract(context) -> dict[str, pd.DataFrame]:
        """A function that extracts data from a CSV file.

        This function will be decorated with a Dagster op and returned.

        Args:
            context: Dagster keyword that provides access to resources and config.

        Returns:
            A dictionary of DataFrames extracted from CSV, keyed by table name.
        """
        ds = context.resources.datastore
        return extractor_cls(ds, name).extract()

    return op(
        required_resource_keys={"datastore", "dataset_settings"},
        name=f"extract_single_{name}_year",
    )(extract)


def raw_df_factory(extractor_cls: type[CsvExtractor], name: str) -> AssetsDefinition:
    """Return a dagster graph asset to extract a set of raw DataFrames from CSV files.

    Args:
        extractor_cls: The dataset-specific CSV extractor used to extract the data.
            Needs to correspond to the dataset identified by ``name``.
        name: Name of a CSV-based dataset (e.g. "eia176"). Currently this must be
            one of the attributes of :class:`pudl.settings.EiaSettings`
    """
    extractor = extractor_factory(extractor_cls, name)

    def raw_dfs() -> dict[str, pd.DataFrame]:
        """Produce a dictionary of extracted EIA dataframes."""
        return extractor()

    return graph_asset(name=f"{name}_raw_dfs")(raw_dfs)
