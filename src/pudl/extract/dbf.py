"""Generalized DBF extractor for FERC data."""
import csv
import importlib
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import IO, Any, Protocol, Self

import pandas as pd
import sqlalchemy as sa
from dbfread import DBF, FieldParser

import pudl
import pudl.logging_helpers
from pudl.metadata.classes import DataSource
from pudl.settings import FercToSqliteSettings, GenericDatasetSettings
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


class FercDbfArchive:
    """Represents API for accessing files within a single DBF archive.

    Typically, archive contains data for a single year and single FERC form dataset
    (e.g. FERC Form 1 or FERC Form 2).
    """

    def __init__(
        self,
        zipfile: zipfile.ZipFile,
        dbc_path: Path,
        table_file_map: dict[str, str],
        partition: dict[str, Any],
        field_parser: FieldParser,
    ):
        """Constructs new instance of FercDbfArchive."""
        self.zipfile = zipfile
        self.partition = dict(partition)
        self.root_path: Path = dbc_path.parent
        self.dbc_path: Path = dbc_path
        self._table_file_map = table_file_map
        self.field_parser = field_parser
        self._table_schemas: dict[str, list[str]] = {}

    def get_file(self, filename: str) -> IO[bytes]:
        """Opens the file within this archive."""
        path = self.root_path / filename
        try:
            return self.zipfile.open(path.as_posix())
        except KeyError:
            raise KeyError(f"{path} not available for {self.partition}.")

    def get_db_schema(self) -> dict[str, list[str]]:
        """Returns dict with table names as keys, and list of column names as values."""
        if not self._table_schemas:
            # TODO(janrous): this should be locked to ensure multi-thread safety
            dbf = DBF(
                "",
                ignore_missing_memofile=True,
                filedata=self.zipfile.open(self.dbc_path.as_posix()),
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
            self._table_schemas = {
                table_names[tid]: cols for tid, cols in table_columns.items()
            }
        return self._table_schemas

    def get_table_dbf(self, table_name: str) -> DBF:
        """Opens the DBF for a given table."""
        fname = self._table_file_map[table_name]
        return DBF(
            fname,
            encoding="latin1",
            parserclass=self.field_parser,
            ignore_missing_memofile=True,
            filedata=self.get_file(fname),
        )

    @lru_cache
    def get_table_schema(self, table_name: str) -> DbfTableSchema:
        """Returns TableSchema for a given table and a given year."""
        table_columns = self.get_db_schema()[table_name]
        dbf = self.get_table_dbf(table_name)
        dbf_fields = [field for field in dbf.fields if field.name != "_NullFlags"]
        if len(table_columns) != len(table_columns):
            return ValueError(
                f"Number of DBF fields in {table_name} does not match what was "
                f"found in the DBC index file for {self.partition}."
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


class AbstractFercDbfReader(Protocol):
    """This is the interface definition for dealing with fox-pro datastores."""

    def get_dataset(self: Self) -> str:
        """Returns name of the dataset that this datastore provides access to."""
        ...

    def get_table_names(self: Self) -> list[str]:
        """Returns list of all available table names."""
        ...

    def get_archive(self: Self, **filters) -> FercDbfArchive:
        """Returns single archive matching specific filters."""
        ...

    def get_table_schema(self: Self, table_name: str, year: int) -> DbfTableSchema:
        """Returns schema for a given table and a given year."""
        ...

    def load_table_dfs(
        self: Self, table_name: str, partitions: list[dict[str, Any]]
    ) -> pd.DataFrame | None:
        """Returns dataframe that contains data for a given table across given years."""
        ...

    def transform_table_part(
        self: Self, table_part: pd.DataFrame, table_name: str, partition: dict[str, Any]
    ) -> pd.DataFrame:
        """Apply table-specific per-partition transforms if necessary."""
        ...


# TODO(rousik): we should have a class for accessing single archive.


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


class PartitionedDataFrame:
    """This class bundles pandas.DataFrame with partition information."""

    def __init__(self, df: pd.DataFrame, partition: dict[str, Any]):
        """Constructs new instance of PartitionedDataFrame."""
        self.df = df
        self.partition = partition


# TODO(rousik): instead of using class-level constants, we could pass the params in the constructor, which should
# allow us to instantiate these dataset-specific datastores in the extractor code.
# That may make the manipulations little easier.
class FercDbfReader:
    """Wrapper to provide standardized access to FERC DBF databases."""

    def __init__(
        self: Self,
        datastore: Datastore,
        dataset: str,
        field_parser: FieldParser = FercFieldParser,
    ):
        """Create a new instance of FercDbfReader.

        This can be used for retrieving data from the legacy FoxPro databases that are
        used by FERC Form N datasets up to 2020.

        Args:
            datastore: provides access to raw files on disk.
            dataset: name of the dataset (e.g. ferc1), this is used to load metadata
                from package_data/{dataset} subdirectory.
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

    def get_dataset(self: Self) -> str:
        """Return the name of the dataset this datastore works with."""
        return self.dataset

    def _open_csv_resource(self: Self, base_filename: str) -> csv.DictReader:
        """Open the given resource file as :class:`csv.DictReader`."""
        pkg_path = f"pudl.package_data.{self.dataset}"
        return csv.DictReader(importlib.resources.open_text(pkg_path, base_filename))

    @lru_cache
    def get_archive(self: Self, year: int, **filters) -> FercDbfArchive:
        """Returns single dbf archive matching given filters."""
        nfilters = self._normalize(filters)
        return FercDbfArchive(
            self.datastore.get_zipfile_resource(self.dataset, year=year, **nfilters),
            dbc_path=self._dbc_path[year],
            partition=filters,
            table_file_map=self._table_file_map,
            field_parser=self.field_parser,
        )

    def get_table_names(self: Self) -> list[str]:
        """Returns list of tables that this datastore provides access to."""
        return list(self._table_file_map)

    @staticmethod
    def _normalize(filters: dict[str, Any]) -> dict[str, str]:
        """Casts partition values to lowercase strings."""
        return {k: str(v).lower() for k, v in filters.items()}

    def valid_partition_filter(self: Self, fl: dict[str, Any]) -> bool:
        """Returns True if a given filter fl is considered to be valid.

        This can be used to eliminate partitions that are not suitable for processing,
        e.g. for early years of FERC Form 2, databases marked with part=1 or part=2 are
        not suitable.
        """
        logger.info(f"running base class valid_partition_filter on {fl}")
        return True

    def load_table_dfs(
        self: Self, table_name: str, partitions: list[dict[str, Any]]
    ) -> list[PartitionedDataFrame]:
        """Returns all data for a given table.

        Merges data for a given table across all partitions.

        Args:
            table_name: name of the table to load.
            partitions: list of partition filters to use
        """
        # Retrieve all archives that match given years
        # Then try to simply merge
        # There may be need for some first-pass aggregation of tables
        # from within the same year.
        dfs: list[PartitionedDataFrame] = []
        for p in partitions:
            archive = self.get_archive(**p)
            try:
                dfs.append(PartitionedDataFrame(archive.load_table(table_name), p))
            except KeyError:
                logger.debug(f"Table {table_name} missing for partition {p}")
                continue
        return dfs


class FercDbfExtractor:
    """Generalized class for loading data from foxpro databases into SQLAlchemy.

    When subclassing from this generic extractor, one should implement dataset specific
    logic in the following manner:

    1. set DATABASE_NAME class attribute. This controls what filename is used for the output
    sqlite database.
    2. Implement get_dbf_reader() method to return the right kind of dataset specific
    AbstractDbfReader instance.

    Dataset specific logic and transformations can be injected by overriding:

    1. finalize_schema() in order to modify sqlite schema. This is called just before
    the schema is written into the sqlite database. This is good place for adding
    primary and/or foreign key constraints to tables.
    2. transform_table(table_name, df) will be invoked after dataframe is loaded from
    the foxpro database and before it's written to sqlite. This is good place for
    table-specific preprocessing and/or cleanup.
    3. postprocess() is called after data is written to sqlite. This can be used for
    database level final cleanup and transformations (e.g. injecting missing
    respondent_ids).

    The extraction logic is invoked by calling execute() method of this class.
    """

    DATABASE_NAME = None
    DATASET = None

    def __init__(
        self,
        datastore: Datastore,
        settings: FercToSqliteSettings,
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
        self.settings: GenericDatasetSettings = self.get_settings(settings)
        self.clobber = clobber
        self.output_path = output_path
        self.datastore = datastore
        self.dbf_reader = self.get_dbf_reader(datastore)
        self.sqlite_engine = sa.create_engine(self.get_db_path())
        self.sqlite_meta = sa.MetaData()
        self.sqlite_meta.reflect(self.sqlite_engine)

    def get_settings(
        self, global_settings: FercToSqliteSettings
    ) -> GenericDatasetSettings:
        """Returns dataset relevant settings from the global_settings."""
        return NotImplemented(
            "get_settings() needs to extract dataset specific settings."
        )

    def get_dbf_reader(self, datastore: Datastore) -> AbstractFercDbfReader:
        """Returns appropriate instance of AbstractFercDbfReader to access the data."""
        return FercDbfReader(datastore, dataset=self.DATASET)

    def get_db_path(self) -> str:
        """Returns the connection string for the sqlite database."""
        db_path = str(Path(self.output_path) / self.DATABASE_NAME)
        return f"sqlite:///{db_path}"

    def execute(self):
        """Runs the extraction of the data from dbf to sqlite."""
        logger.info(
            f"Running dbf extraction for {self.DATASET} with settings: {self.settings}"
        )
        if self.settings.disabled:
            logger.warning(f"Dataset {self.DATASET} extraction is disabled, skipping")
            return

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
                DataSource.from_id(self.dbf_reader.get_dataset()).working_partitions[
                    "years"
                ]
            )
        ref_dbf = self.dbf_reader.get_archive(year=refyear, data_format="dbf")
        for tn in self.dbf_reader.get_table_names():
            ref_dbf.get_table_schema(tn).create_sa_table(self.sqlite_meta)
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

    @staticmethod
    def is_valid_partition(fl: dict[str, Any]) -> bool:
        """Returns True if the partition filter should be considered for processing."""
        return True

    def aggregate_table_frames(
        self, table_name: str, dfs: list[PartitionedDataFrame]
    ) -> pd.DataFrame | None:
        """Function to aggregate partitioned data frames into a single one.

        By default, this simply concatenates the frames, but custom dataset specific
        behaviors can be implemented.
        """
        if not dfs:
            return None
        return pd.concat([df.df for df in dfs])

    def load_table_data(self):
        """Loads all tables from fox pro database and writes them to sqlite."""
        partitions = [
            p
            for p in self.datastore.get_datapackage_descriptor(
                self.DATASET
            ).get_partition_filters(data_format="dbf")
            if self.is_valid_partition(p) and p.get("year", None) in self.settings.years
        ]
        logger.info(
            f"Loading {self.DATASET} table data from {len(partitions)} partitions."
        )
        for table in self.dbf_reader.get_table_names():
            logger.info(f"Pandas: reading {table} into a DataFrame.")
            new_df = self.aggregate_table_frames(
                table, self.dbf_reader.load_table_dfs(table, partitions)
            )
            if new_df is None or len(new_df) <= 0:
                logger.warning(f"Table {table} contains no data, skipping.")
                continue
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


def deduplicate_by_year(
    dfs: list[PartitionedDataFrame], pk_column: str
) -> pd.DataFrame | None:
    """Deduplicate records by year, keeping the most recent version of each record.

    It will use pk_column as the primary key column. report_yr column is expected to
    either be present, or it will be derived from partition["year"].
    """
    yr_dfs: list[pd.DataFrame] = []
    for p_df in dfs:
        df = p_df.df
        if "report_yr" not in df.columns:
            df = df.assign(report_yr=p_df.partition["year"])
        yr_dfs.append(df)
    agg_df = pd.concat(yr_dfs)
    return (
        agg_df.sort_values(by=["report_yr", pk_column])
        .drop_duplicates(subset=pk_column, keep="last")
        .drop(columns="report_yr")
    )
