"""Generalized DBF extractor for FERC data."""
import csv
import importlib
from collections import defaultdict
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import sqlalchemy as sa
from dbfread import DBF, FieldParser

import pudl
import pudl.logging_helpers
from pudl.metadata.classes import DataSource
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class DbfTableSchema:
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

    def get_columns(self) -> Iterator[tuple[str, sa.types.TypeEngine]]:
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


class AbstractFercDbfReader(Protocol):
    """This is the interface definition for dealing with fox-pro datastores."""

    def get_dataset(self) -> str:
        """Returns name of the dataset that this datastore provides access to."""
        ...

    def get_table_names(self) -> list[str]:
        """Returns list of all available table names."""
        ...

    def get_table_schema(self, table_name: str, year: int) -> DbfTableSchema:
        """Returns schema for a given table and a given year."""
        ...

    def load_table_dfs(self, table_name: str, years: list[int]) -> pd.DataFrame | None:
        """Returns dataframe that contains data for a given table across given years."""
        ...


class FercFieldParser(FieldParser):
    """A custom DBF parser to deal with bad FERC data types."""

    def parseN(self, field, data: bytes) -> int | float | None:  # noqa: N802
        """Augments the Numeric DBF parser to account for bad FERC data.

        There are a small number of bad entries in the backlog of FERC Form 1
        data. They take the form of leading/trailing zeroes or null characters
        in supposedly numeric fields, and occasionally a naked '.'

        Accordingly, this custom parser strips leading and trailing zeros and
        null characters, and replaces a bare '.' character with zero, allowing
        all these fields to be cast to numeric values.

        Args:
            field: The DBF field being parsed.
            data: Binary data (bytes) read from the DBF file.
        """  # noqa: D417
        # Strip whitespace, null characters, and zeroes
        data = data.strip().strip(b"*\x00").lstrip(b"0")
        # Replace bare periods (which are non-numeric) with zero.
        if data == b".":
            data = b"0"
        return super().parseN(field, data)


DBF_TYPES = {
    "C": sa.String,
    "D": sa.Date,
    "F": sa.Float,
    "I": sa.Integer,
    "L": sa.Boolean,
    "M": sa.Text,  # 10 digit .DBT block number, stored as a string...
    "N": sa.Float,
    "T": sa.DateTime,
    "0": sa.Integer,  # based on dbf2sqlite mapping
    "B": "XXX",  # .DBT block number, binary string
    "@": "XXX",  # Timestamp... Date = Julian Day, Time is in milliseconds?
    "+": "XXX",  # Autoincrement (e.g. for IDs)
    "O": "XXX",  # Double, 8 bytes
    "G": "XXX",  # OLE 10 digit/byte number of a .DBT block, stored as string
}
"""dict: A mapping of DBF field types to SQLAlchemy Column types.

This dictionary maps the strings which are used to denote field types in the DBF objects
to the corresponding generic SQLAlchemy Column types: These definitions come from a
combination of the dbfread example program dbf2sqlite and this DBF file format
documentation page: http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm

Unmapped types left as 'XXX' which should result in an error if encountered.
"""


# TODO(rousik): instead of using class-level constants, we could pass the params in the constructor, which should
# allow us to instantiate these dataset-specific datastores in the extractor code.
# That may make the manipulations little easier.
class FercDbfReader:
    """Wrapper to provide standardized access to FERC DBF databases."""

    def __init__(
        self,
        datastore: Datastore,
        dataset: str,
        field_parser: FieldParser = FercFieldParser,
    ):
        """Creates new instance of FercDbfReader.

        This can be used for retrieving data from the legacy FoxPro
        databases that are used by FERC Form N datasets up to 2020.

        Args:
            datastore: provides access to raw files on disk.
            dataset: name of the dataset (e.g. ferc1), this is used to
            load metadata from package_data/{dataset} subdirectory.
            field_parser: FieldParser class to use when loading data
        """
        self._cache = {}
        self.datastore = datastore
        self.dataset = dataset
        self.field_parser = field_parser

        # dbc_file_map.csv contains path to the DBC file that contains the
        # overall database schemas. It is expected that DBF files live in
        # the same directory.
        self._dbc_path = {}
        for row in self._open_csv_resource("dbc_file_map.csv"):
            self._dbc_path[int(row["year"])] = Path(row["path"])

        # table_file_map holds mapping between tables and their corresponding
        # DBF files.
        self._table_file_map = {}
        for row in self._open_csv_resource("table_file_map.csv"):
            self._table_file_map[row["table"]] = row["filename"]

    def get_dataset(self) -> str:
        """Returns the name of the dataset this datastore works with."""
        return self.dataset

    def _open_csv_resource(self, base_filename: str) -> csv.DictReader:
        """Opens the given resource file as csv.DictReader."""
        pkg_path = f"pudl.package_data.{self.dataset}"
        return csv.DictReader(importlib.resources.open_text(pkg_path, base_filename))

    def _get_dir(self, year: int) -> Path:
        """Returns the directory where the files for given year are stored."""
        try:
            return self._dbc_path[year].parent
        except KeyError:
            raise ValueError(f"No {self.dataset} data for year {year}")

    def _get_file(self, year: int, filename: str) -> Any:
        """Returns the file descriptor for a given year and base filename."""
        return self._get_file_by_path(year, self._get_dir(year) / filename)

    def _get_file_by_path(self, year: int, path: Path) -> Any:
        """Returns the file descriptor for a file identified by its full path."""
        if year not in self._cache:
            self._cache[year] = self.datastore.get_zipfile_resource(
                self.dataset, year=year, data_format="dbf"
            )
        archive = self._cache[year]
        try:
            return archive.open(path.as_posix())
        except KeyError:
            raise KeyError(f"{path} not available for year {year} in {self.dataset}.")

    def get_table_dbf(self, table_name: str, year: int) -> DBF:
        """Opens the DBF for a given table and year."""
        fname = self._table_file_map[table_name]
        fd = self._get_file(year, fname)
        return DBF(
            fname,
            encoding="latin1",
            parserclass=self.field_parser,
            ignore_missing_memofile=True,
            filedata=fd,
        )

    def get_table_names(self) -> list[str]:
        """Returns list of tables that this datastore provides access to."""
        return list(self._table_file_map)

    @lru_cache
    def get_db_schema(self, year: int) -> dict[str, list[str]]:
        """Returns dict with table names as keys, and list of column names as values."""
        dbf = DBF(
            "",
            ignore_missing_memofile=True,
            filedata=self._get_file_by_path(year, self._dbc_path[year]),
        )
        table_names: dict[Any, str] = {}
        table_columns = defaultdict(list)
        for row in dbf:
            obj_id = row.get("OBJECTID")
            obj_name = row.get("OBJECTNAME")
            obj_type = row.get("OBJECTTYPE", None)
            if obj_type == "Table":
                table_names[obj_id] = obj_name
            elif obj_type == "Field":
                parent_id = row.get("PARENTID")
                table_columns[parent_id].append(obj_name)
        # Remap table ids to table names.
        return {table_names[tid]: cols for tid, cols in table_columns.items()}

    # TODO(rousik): table column map should be remapping short names (in dbf) to long names found in db schema (DBC).
    # This is kind of annoying transformation but we can't do without it.

    @lru_cache
    def get_table_schema(self, table_name: str, year: int) -> DbfTableSchema:
        """Returns TableSchema for a given table and a given year."""
        table_columns = self.get_db_schema(year)[table_name]
        dbf = self.get_table_dbf(table_name, year)
        dbf_fields = [field for field in dbf.fields if field.name != "_NullFlags"]
        if len(table_columns) != len(table_columns):
            return ValueError(
                f"Number of DBF fields in {table_name} does not match what was "
                f"found in the DBC index file for {year}."
            )
        schema = DbfTableSchema(table_name)
        for long_name, dbf_col in zip(table_columns, dbf_fields):
            if long_name[:8] != dbf_col.name.lower()[:8]:
                raise ValueError(
                    f"DBF field name mismatch: {dbf_col.name} != {long_name}"
                )
            col_type = DBF_TYPES[dbf_col.type]
            if col_type == sa.String:
                col_type = sa.String(length=dbf_col.length)
            schema.add_column(long_name, col_type, short_name=dbf_col.name)
        return schema

    def _load_single_year(self, table_name: str, year: int) -> pd.DataFrame:
        """Returns dataframe that holds data for a single year for a given table.

        Args:
            table_name: name of the table.
            year: year for which the data should be loaded.
        """
        sch = self.get_table_schema(table_name, year)
        df = pd.DataFrame(iter(self.get_table_dbf(table_name, year)))
        df = df.drop("_NullFlags", axis=1, errors="ignore").rename(
            sch.get_column_rename_map(), axis=1
        )
        return df

    def load_table_dfs(self, table_name: str, years: list[int]) -> pd.DataFrame | None:
        """Returns the concatenation of the data for a given table and years.

        Args:
            table_name: name of the table to load.
            years: list of years to load and concatenate.
        """
        yearly_dfs = []
        for yr in years:
            try:
                yearly_dfs.append(self._load_single_year(table_name, yr))
            except KeyError:
                continue
        if yearly_dfs:
            return pd.concat(yearly_dfs, sort=True)
        return None


class FercDbfExtractor:
    """Generalized class for loading data from foxpro databases into SQLAlchemy.

    When subclassing from this generic extractor, one should implement dataset specific
    logic in the following manner:
    1. set DATABASE_NAME. This is going to be used as the file for the resulting sqlite
    database.
    2. Overrride get_datastore() method to return the right kind of dataset specific datastore.

    Dataset specific logic and transformations can be injected by overriding:
    1. finalize_schema() in order to modify sqlite schema. This is called just before the
    schema is written into the sqlite database. This is good place for adding primary and/or
    foreign key constraints to tables.
    2. transform_table(table_name, df) will be invoked after dataframe is loaded from the foxpro
    database and before it's written to sqlite. This is good place for table-specific
    preprocessing and/or cleanup.
    3. postprocess() is called after data is written to sqlite. This can be used for database
    level final cleanup and transformations (e.g. injecting missing respondent_ids).

    The extraction logic is invoked by calling execute() method of this class.
    """

    DATABASE_NAME = None

    def __init__(
        self,
        datastore: Datastore,
        settings: Any,
        output_path: Path,
        clobber: bool = False,
    ):
        """Constructs new instance of FercDbfExtractor.

        Args:
            datastore: top-level datastore instance for accessing raw data files.
            settings: generic settings object for this extrctor.
            output_path: directory where the output databases should be stored.
            clobber: if True, existing databases should be replaced.
        """
        self.settings = settings
        self.clobber = clobber
        self.output_path = output_path
        self.datastore = self.get_datastore(datastore)
        self.sqlite_engine = sa.create_engine(self.get_db_path())
        self.sqlite_meta = sa.MetaData()
        self.sqlite_meta.reflect(self.sqlite_engine)

    def get_db_path(self) -> str:
        """Returns the connection string for the sqlite database."""
        db_path = str(Path(self.output_path) / self.DATABASE_NAME)
        return f"sqlite:///{db_path}"

    def execute(self):
        """Runs the extraction of the data from dbf to sqlite."""
        # TODO(rousik): perhaps we should check clobber here before starting anything.
        self.delete_schema()
        self.create_sqlite_tables()
        self.load_table_data()
        self.postprocess()

    def delete_schema(self):
        """Drops all tables from the existing sqlite database."""
        try:
            pudl.helpers.drop_tables(
                self.sqlite_engine,
                clobber=self.clobber,
            )
        except sa.exc.OperationalError:
            pass
        self.sqlite_engine = sa.create_engine(self.get_db_path())
        self.sqlite_meta = sa.MetaData()
        self.sqlite_meta.reflect(self.sqlite_engine)

    def create_sqlite_tables(self):
        """Creates database schema based on the input tables."""
        refyear = self.settings.refyear
        if refyear is None:
            refyear = max(
                DataSource.from_id(self.datastore.get_dataset()).working_partitions[
                    "years"
                ]
            )
        for tn in self.datastore.get_table_names():
            self.datastore.get_table_schema(tn, refyear).create_sa_table(
                self.sqlite_meta
            )
        self.finalize_schema(self.sqlite_meta)
        self.sqlite_meta.create_all(self.sqlite_engine)

    def transform_table(self, table_name: str, in_df: pd.DataFrame) -> pd.DataFrame:
        """Transforms the content of a single table.

        This method can be used to modify contents of the dataframe after it has
        been loaded from fox pro database and before it's written to sqlite database.

        Args:
            table_name: name of the table that the dataframe is associated with
            in_df: dataframe that holds all records.
        """
        return in_df

    def load_table_data(self):
        """Loads all tables from fox pro database and writes them to sqlite."""
        for table in self.datastore.get_table_names():
            logger.info(f"Pandas: reading {table} into a DataFrame.")
            new_df = self.datastore.load_table_dfs(table, self.settings.years)
            new_df = self.transform_table(table, new_df)

            logger.debug(f"    {table}: N = {len(new_df)}")
            if len(new_df) <= 0:
                continue

            coltypes = {col.name: col.type for col in self.sqlite_meta.tables[table].c}
            logger.info(f"SQLite: loading {len(new_df)} rows into {table}.")
            new_df.to_sql(
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
        """This metod is called after all the data is loaded into sqlite."""
        pass
