"""Extractor for CSV data."""
from csv import DictReader
from functools import lru_cache
from importlib import resources
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import sqlalchemy as sa

import pudl.logging_helpers
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class CsvTableSchema:
    """Simple data-wrapper for the fox-pro table schema."""

    def __init__(self, table_name: str):
        """Creates new instance of the table schema setting.

        The table name will be set as table_name and table will have no columns.
        """
        self.name = table_name
        self._columns = []
        self._column_types = {}
        self._short_name_map = {}  # short_name_map[short_name] -> long_name

    def add_column(
        self,
        col_name: str,
        col_type: sa.types.TypeEngine,
        short_name: str | None = None,
    ):
        """Adds a new column to this table schema."""
        assert col_name not in self._columns
        self._columns.append(col_name)
        self._column_types[col_name] = col_type
        if short_name is not None:
            self._short_name_map[short_name] = col_name

    def get_columns(self) -> list[tuple[str, sa.types.TypeEngine]]:
        """Itereates over the (column_name, column_type) pairs."""
        for col_name in self._columns:
            yield (col_name, self._column_types[col_name])

    def get_column_names(self) -> set[str]:
        """Returns set of long column names."""
        return set(self._columns)

    def get_column_rename_map(self) -> dict[str, str]:
        """Returns dictionary that maps from short to long column names."""
        return dict(self._short_name_map)

    def create_sa_table(self, sa_meta: sa.MetaData) -> sa.Table:
        """Creates SQLAlchemy table described by this instance.

        Args:
            sa_meta: new table will be written to this MetaData object.
        """
        table = sa.Table(self.name, sa_meta)
        for col_name, col_type in self.get_columns():
            table.append_column(sa.Column(col_name, col_type))
        return table


class CsvArchive:
    """Represents API for accessing files within a single CSV archive."""

    def __init__(
        self,
        zipfile: ZipFile,
        table_file_map: dict[str, str],
        column_types: dict[str, dict[str, sa.types.TypeEngine]],
    ):
        """Constructs new instance of CsvArchive."""
        self.zipfile = zipfile
        self._table_file_map = table_file_map
        self._column_types = column_types
        self._table_schemas: dict[str, list[str]] = {}

    @lru_cache
    def get_table_schema(self, table_name: str) -> CsvTableSchema:
        """Returns TableSchema for a given table and a given year."""
        with self.zipfile.open(self._table_file_map[table_name]) as f:
            text_f = TextIOWrapper(f)
            table_columns = DictReader(text_f).fieldnames

        # TODO: Introduce some validations here so we can know if source structure changed
        schema = CsvTableSchema(table_name)
        for column_name in table_columns:
            # TODO: length for string type, if default is inappropriate
            col_type = self._column_types[table_name][column_name]
            schema.add_column(column_name, col_type)
        return schema

    def load_table(self, table_name: str) -> pd.DataFrame:
        """Returns dataframe that holds data for a table contained within this archive.

        Args:
            table_name: name of the table.
        """
        sch = self.get_table_schema(table_name)
        df = pd.DataFrame(iter(self.get_table_dbf(table_name)))
        df = df.drop("_NullFlags", axis=1, errors="ignore").rename(
            sch.get_column_rename_map(), axis=1
        )
        return df


class CsvReader:
    """Wrapper to provide standardized access to CSV files."""

    def __init__(
        self,
        datastore: Datastore,
        dataset: str,
        column_types: dict[str, dict[str, sa.types.TypeEngine]],
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
        self._column_types = column_types
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
        """Returns a ZipFile instance corresponding to the dataset."""
        return CsvArchive(
            self.datastore.get_zipfile_resource(self.dataset),
            table_file_map=self._table_file_map,
            column_types=self._column_types,
        )

    @lru_cache
    # TODO: We shouldn't need to call get_zipfile_resource multiple times
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
    3. Set COLUMN_TYPES to a map of tables to column names and their sqlalchemy types. This is used to generate DDL.

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
    COLUMN_TYPES = {}

    def __init__(self, datastore: Datastore, output_path: Path):
        """Constructs new instance of CsvExtractor.

        Args:
            datastore: top-level datastore instance for accessing raw data files.
            output_path: directory where the output databases should be stored.
            # TODO: Consider including this for consistency
            clobber: if True, existing databases should be replaced.
        """
        self.output_path = output_path
        self.csv_reader = self.get_csv_reader(datastore)
        self.sqlite_engine = sa.create_engine(self.get_db_path())
        self.sqlite_meta = sa.MetaData()

    def get_db_path(self) -> str:
        """Returns the connection string for the sqlite database."""
        db_path = str(Path(self.output_path) / self.DATABASE_NAME)
        return f"sqlite:///{db_path}"

    def get_csv_reader(self, datastore: Datastore):
        """Returns instance of CsvReader to access the data."""
        return CsvReader(datastore, self.DATASET, self.COLUMN_TYPES)

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
        """Creates database schema based on the input tables."""
        csv_archive = self.csv_reader.get_archive()
        for tn in self.csv_reader.get_table_names():
            csv_archive.get_table_schema(tn).create_sa_table(self.sqlite_meta)
        self.finalize_schema(self.sqlite_meta)
        self.sqlite_meta.create_all(self.sqlite_engine)

    def load_table_data(self) -> None:
        """Extracts and loads csv data into sqlite."""
        for table in self.csv_reader.get_table_names():
            # TODO: Make a method instead of using this private attribute
            filename = self.csv_reader._table_file_map[table]
            df = self.csv_reader.read(filename)
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

    def finalize_schema(self, meta: sa.MetaData) -> sa.MetaData:
        """This method is called just before the schema is written to sqlite.

        You can use this method to apply dataset specific alterations to the schema,
        such as adding primary and foreign key constraints.
        """
        return meta

    def postprocess(self):
        """This method is called after all the data is loaded into sqlite to transform raw data to targets."""
        pass
