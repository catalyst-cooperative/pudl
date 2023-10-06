"""Extractor for CSV data."""
from csv import DictReader
from functools import lru_cache
from importlib import resources
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import sqlalchemy as sa

from pudl import logging_helpers
from pudl.workspace.datastore import Datastore

logger = logging_helpers.get_logger(__name__)


class CsvReader:
    """Wrapper to provide standardized access to CSV files."""

    def __init__(self, datastore: Datastore, dataset: str):
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
    def _cache_zipfile(self) -> ZipFile:
        """Returns a ZipFile instance corresponding to the dataset."""
        return self.datastore.get_zipfile_resource(self.dataset)

    def read(self, filename: str) -> pd.DataFrame:
        """Read the data from the CSV source and return as dataframes."""
        logger.info(f"Extracting {filename} from CSV into pandas DataFrame.")
        zipfile = self._cache_zipfile()
        with zipfile.open(filename) as f:
            # TODO: Define encoding
            df = pd.read_csv(f)
        return df


class CsvExtractor:
    """Generalized class for loading data from CSV files into tables into SQLAlchemy.

    When subclassing from this generic extractor, one should implement dataset specific
    logic in the following manner:

    1. Set DATABASE_NAME class attribute. This controls what filename is used for the output
    sqlite database.
    2. Set DATASET class attribute. This is used to load metadata from package_data/{dataset} subdirectory.

    Dataset specific logic and transformations can be injected by overriding:

    # TODO: Update the details here to align with functions in this class
    1. finalize_schema() in order to modify sqlite schema. This is called just before
    the schema is written into the sqlite database. This is good place for adding
    primary and/or foreign key constraints to tables.
    2. aggregate_table_frames() is responsible for concatenating individual data frames
    (one par input partition) into single one. This is where deduplication can take place.
    3. transform_table(table_name, df) will be invoked after dataframe is loaded from
    the foxpro database and before it's written to sqlite. This is good place for
    table-specific preprocessing and/or cleanup.
    4. postprocess() is called after data is written to sqlite. This can be used for
    database level final cleanup and transformations (e.g. injecting missing
    respondent_ids).

    The extraction logic is invoked by calling execute() method of this class.
    """

    # TODO: Reconcile this with the above
    """This represents an ETL pipeling (as opposed to an ELT pipeline), since we're more interested in transformed output and don't want to commit a lot of space to persisting raw data
    Transformation mainly occurs just before loading (aside from more circumstantial pre- or postprocessing that's introduced).
    """

    DATABASE_NAME = None
    DATASET = None

    def __init__(self, datastore: Datastore, output_path: Path):
        """Constructs new instance of CsvExtractor.

        Args:
            datastore: top-level datastore instance for accessing raw data files.
            output_path: directory where the output databases should be stored.
            # TODO: Consider including this for consistency
            clobber: if True, existing databases should be replaced.
        """
        self.sqlite_engine = sa.create_engine(self.get_db_path())
        self.sqlite_meta = sa.MetaData()
        self.output_path = output_path
        self.csv_reader = self.get_csv_reader(datastore)

    def get_db_path(self) -> str:
        """Returns the connection string for the sqlite database."""
        db_path = str(Path(self.output_path) / self.DATABASE_NAME)
        return f"sqlite:///{db_path}"

    def get_csv_reader(self, datastore: Datastore):
        """Returns instance of CsvReader to access the data."""
        return CsvReader(datastore, dataset=self.DATASET)

    def execute(self):
        """Runs the extraction of the data from csv to sqlite."""
        self.delete_schema()
        self.create_sqlite_tables()
        self.load_table_data()
        self.postprocess()

    def delete_schema(self):
        # TODO: Implement or extract from dbf.py to more general space. Take a pass at reconciling with excel.py
        """Drops all tables from the existing sqlite database."""
        pass

    def create_sqlite_tables(self):
        # TODO: Implement or extract from dbf.py to more general space. Take a pass at reconciling with excel.py
        """Creates database schema based on the input tables."""
        pass

    def load_table_data(self) -> None:
        """Extracts and loads csv data into sqlite."""
        for table in self.csv_reader.get_table_names():
            df = self.csv_reader.read(table)
            coltypes = {col.name: col.type for col in self.sqlite_meta.tables[table].c}
            logger.info(f"SQLite: loading {len(df)} rows into {table}.")
            df.to_sql(
                table,
                self.sqlite_engine,
                if_exists="append",
                chunksize=100000,
                dtype=coltypes,
                index=False,
            )

    def postprocess(self):
        """This method is called after all the data is loaded into sqlite to transform raw data to targets."""
        pass
