"""Extractor for CSV data."""
from csv import DictReader
from importlib import resources
from zipfile import ZipFile

import pandas as pd

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def open_csv_resource(dataset: str, base_filename: str) -> DictReader:
    """Open the given resource file as :class:`csv.DictReader`.

    Args:
        dataset: used to load metadata from package_data/{dataset} subdirectory.
        base_filename: the name of the file in the subdirectory to open.
    """
    csv_path = resources.files(f"pudl.package_data.{dataset}") / base_filename
    return DictReader(csv_path.open())


def get_table_file_map(dataset: str) -> dict[str, str]:
    """Return a dictionary of table names and filenames for the dataset.

    Args:
        dataset: used to load metadata from package_data/{dataset} subdirectory.
    """
    return {
        row["table"]: row["filename"]
        for row in open_csv_resource(dataset, "table_file_map.csv")
    }


class CsvExtractor:
    """Generalized class for extracting dataframes from CSV files.

    The extraction logic is invoked by calling extract() method of this class.
    """

    def __init__(self, zipfile: ZipFile, table_file_map: dict[str, str]):
        """Create a new instance of CsvExtractor.

        This can be used for retrieving data from CSV files.

        Args:
            zipfile: zipfile object containing source files
            table_file_map: map of table name to source file in zipfile archive
        """
        self._zipfile = zipfile
        self._table_file_map = table_file_map

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
