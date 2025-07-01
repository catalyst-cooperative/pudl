"""Dagster IO Managers."""

import re
from pathlib import Path
from sqlite3 import sqlite_version
from typing import Any

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from alembic.autogenerate.api import compare_metadata
from alembic.migration import MigrationContext
from dagster import (
    Field,
    InitResourceContext,
    InputContext,
    IOManager,
    OutputContext,
    UPathIOManager,
    io_manager,
)
from packaging import version
from upath import UPath

import pudl
from pudl.helpers import get_parquet_table
from pudl.metadata.classes import PUDL_PACKAGE, Package, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    # TODO(rousik): Figure out which kind of identifier is used when.
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


class PudlMixedFormatIOManager(IOManager):
    """Format switching IOManager that supports sqlite and parquet.

    This IOManager provides for the use of parquet files along with the standard SQLite
    database produced by PUDL.
    """

    # Defaults should be provided here and should be potentially
    # overridden by os env variables. This now resides in the
    # @io_manager constructor of this, see pudl_mixed_format_io_manager".
    write_to_parquet: bool
    """If true, data will be written to parquet files."""

    read_from_parquet: bool
    """If true, data will be read from parquet files instead of sqlite."""

    def __init__(self, write_to_parquet: bool = False, read_from_parquet: bool = False):
        """Creates new instance of mixed format pudl IO manager.

        By default, data is written and read from sqlite, but experimental
        support for writing and/or reading from parquet files can be enabled
        by setting the corresponding flags to True.

        Args:
            write_to_parquet: if True, all data will be written to parquet
                files in addition to sqlite.
            read_from_parquet: if True, all data reads will be using
                parquet files as source of truth. Otherwise, data will be
                read from the sqlite database. Reading from parquet provides
                performance increases as well as better datatype handling, so
                this option is encouraged.
        """
        if read_from_parquet and not write_to_parquet:
            raise RuntimeError(
                "read_from_parquet cannot be set when write_to_parquet is False."
            )
        self.write_to_parquet = write_to_parquet
        self.read_from_parquet = read_from_parquet
        self._sqlite_io_manager = PudlSQLiteIOManager(
            base_dir=PudlPaths().output_dir,
            db_name="pudl",
        )
        self._parquet_io_manager = PudlParquetIOManager()

    def handle_output(
        self, context: OutputContext, obj: pd.DataFrame | str
    ) -> pd.DataFrame:
        """Passes the output to the appropriate IO manager instance."""
        self._sqlite_io_manager.handle_output(context, obj)
        if self.write_to_parquet:
            self._parquet_io_manager.handle_output(context, obj)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Reads input from the appropriate IO manager instance."""
        if self.read_from_parquet:
            return self._parquet_io_manager.load_input(context)
        return self._sqlite_io_manager.load_input(context)


class SQLiteIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(
        self,
        base_dir: str,
        db_name: str,
        md: sa.MetaData | None = None,
        timeout: float = 1_000.0,
    ):
        """Init a SQLiteIOmanager.

        Args:
            base_dir: base directory where all the step outputs which use this object
                manager will be stored in.
            db_name: the name of sqlite database.
            md: database metadata described as a SQLAlchemy MetaData object. If not
                specified, default to metadata stored in the pudl.metadata subpackage.
            timeout: How many seconds the connection should wait before raising
                an exception, if the database is locked by another connection.
                If another connection opens a transaction to modify the database,
                it will be locked until that transaction is committed.
        """
        self.base_dir = Path(base_dir)
        self.db_name = db_name

        bad_sqlite_version = version.parse(sqlite_version) < version.parse(
            MINIMUM_SQLITE_VERSION
        )
        if bad_sqlite_version:
            logger.warning(
                f"Found SQLite {sqlite_version} which is less than "
                f"the minimum required version {MINIMUM_SQLITE_VERSION} "
                "As a result, data type constraint checking has been disabled."
            )

        # If no metadata is specified, create an empty sqlalchemy metadata object.
        if md is None:
            md = sa.MetaData()
        self.md = md

        self.engine = self._setup_database(timeout=timeout)

    def _setup_database(self, timeout: float = 1_000.0) -> sa.Engine:
        """Create database and metadata if they don't exist.

        Args:
            timeout: How many seconds the connection should wait before raising an
                exception, if the database is locked by another connection.  If another
                connection opens a transaction to modify the database, it will be locked
                until that transaction is committed.

        Returns:
            engine: SQL Alchemy engine that connects to a database in the base_dir.
        """
        # If the sqlite directory doesn't exist, create it.
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True)
        db_path = self.base_dir / f"{self.db_name}.sqlite"

        engine = sa.create_engine(
            f"sqlite:///{db_path}", connect_args={"timeout": timeout}
        )

        # Create the database and schemas
        if not db_path.exists():
            db_path.touch()
            self.md.create_all(engine)

        return engine

    def _get_sqlalchemy_table(self, table_name: str) -> sa.Table:
        """Get SQL Alchemy Table object from metadata given a table_name.

        Args:
            table_name: The name of the table to look up.

        Returns:
            table: Corresponding SQL Alchemy Table in SQLiteIOManager metadata.

        Raises:
            ValueError: if table_name does not exist in the SQLiteIOManager metadata.
        """
        sa_table = self.md.tables.get(table_name, None)
        if sa_table is None:
            raise ValueError(
                f"{table_name} not found in database metadata. Either add the table to "
                "the metadata or use a different IO Manager."
            )
        return sa_table

    def _handle_pandas_output(self, context: OutputContext, df: pd.DataFrame):
        """Write dataframe to the database.

        SQLite does not support concurrent writes to the database. Instead, SQLite
        queues write transactions and executes them one at a time.  This allows the
        assets to be processed in parallel. See the `SQLAlchemy docs
        <https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#database-
        locking-behavior-concurrency>`__ to learn more about SQLite concurrency.

        Args:
            context: dagster keyword that provides access to output information like
                asset name.
            df: dataframe to write to the database.
        """
        table_name = get_table_name_from_context(context)
        sa_table = self._get_sqlalchemy_table(table_name)
        column_difference = set(sa_table.columns.keys()) - set(df.columns)
        if column_difference:
            raise ValueError(
                f"{table_name} dataframe is missing columns: {column_difference}"
            )

        engine = self.engine
        with engine.begin() as con:
            # Remove old table records before loading to db
            con.execute(sa_table.delete())

        with engine.begin() as con:
            df.to_sql(
                table_name,
                con,
                if_exists="append",
                index=False,
                chunksize=100_000,
                dtype={c.name: c.type for c in sa_table.columns},
            )

    # TODO (bendnorman): Create a SQLQuery type so it's clearer what this method expects
    def _handle_str_output(self, context: OutputContext, query: str):
        """Execute a sql query on the database.

        This is used for creating output views in the database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            query: sql query to execute in the database.
        """
        engine = self.engine
        table_name = get_table_name_from_context(context)

        # Make sure the metadata has been created for the view
        _ = self._get_sqlalchemy_table(table_name)

        with engine.begin() as con:
            # Drop the existing view if it exists and create the new view.
            # TODO (bendnorman): parameterize this safely.
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
            con.execute(query)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output.

        If the output is a dataframe, write it to the database. If it is a string
        execute it as a SQL query.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            obj: a sql query or dataframe to add to the database.

        Raises:
            Exception: if an asset or op returns an unsupported datatype.
        """
        if isinstance(obj, pd.DataFrame):
            self._handle_pandas_output(context, obj)
        elif isinstance(obj, str):
            self._handle_str_output(context, obj)
        else:
            raise Exception(
                "SQLiteIOManager only supports pandas DataFrames and strings of SQL "
                "queries."
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        table_name = get_table_name_from_context(context)
        # Check if the table_name exists in the self.md object
        _ = self._get_sqlalchemy_table(table_name)

        engine = self.engine

        with engine.begin() as con:
            try:
                df = pd.read_sql_table(table_name, con)
            except ValueError as err:
                raise ValueError(
                    f"{table_name} not found. Either the table was dropped "
                    "or it doesn't exist in the pudl.metadata.resources."
                    "Add the table to the metadata and recreate the database."
                ) from err
            if df.empty:
                raise AssertionError(
                    f"The {table_name} table is empty. Materialize "
                    f"the {table_name} asset so it is available in the database."
                )
            return df


class PudlParquetIOManager(IOManager):
    """IOManager that writes pudl tables to pyarrow parquet files."""

    def handle_output(self, context: OutputContext, df: Any) -> None:
        """Writes pudl dataframe to parquet file."""
        assert isinstance(df, pd.DataFrame), "Only panda dataframes are supported."
        table_name = get_table_name_from_context(context)
        parquet_path = PudlPaths().parquet_path(table_name)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        res = Resource.from_id(table_name)

        df = res.enforce_schema(df)
        schema = res.to_pyarrow()
        with pq.ParquetWriter(
            where=parquet_path,
            schema=schema,
            compression="snappy",
            version="2.6",
        ) as writer:
            writer.write_table(
                pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Loads pudl table from parquet file."""
        table_name = get_table_name_from_context(context)
        return get_parquet_table(table_name)


class PudlSQLiteIOManager(SQLiteIOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database.

    This class extends the SQLiteIOManager class to manage database metadata and dtypes
    using the :class:`pudl.metadata.classes.Package` class.
    """

    def __init__(
        self,
        base_dir: str,
        db_name: str,
        package: Package | None = None,
        timeout: float = 1_000.0,
    ):
        """Initialize PudlSQLiteIOManager.

        Args:
            base_dir: base directory where all the step outputs which use this object
                manager will be stored in.
            db_name: the name of sqlite database.
            package: Package object that contains collections of
                :class:`pudl.metadata.classes.Resources` objects and methods
                for validating and creating table metadata. It is used in this class
                to create sqlalchemy metadata and check datatypes of dataframes. If not
                specified, defaults to a Package with all metadata stored in the
                :mod:`pudl.metadata.resources` subpackage.

                Every table that appears in `self.md` is specified in `self.package`
                as a :class:`pudl.metadata.classes.Resources`. However, not every
                :class:`pudl.metadata.classes.Resources` in `self.package` is included
                in `self.md` as a table. This is because `self.package` is used to ensure
                datatypes of dataframes loaded from database views are correct. However,
                the metadata for views in `self.package` should not be used to create
                table schemas in the database because views are just stored sql statements
                and do not require a schema.
            timeout: How many seconds the connection should wait before raising an
                exception, if the database is locked by another connection.  If another
                connection opens a transaction to modify the database, it will be locked
                until that transaction is committed.
        """
        if package is None:
            package = PUDL_PACKAGE
        self.package = package
        md = self.package.to_sql()
        sqlite_path = Path(base_dir) / f"{db_name}.sqlite"
        if not sqlite_path.exists():
            raise RuntimeError(
                f"{sqlite_path} not initialized! Run `alembic upgrade head`."
            )

        super().__init__(base_dir, db_name, md, timeout)

        existing_schema_context = MigrationContext.configure(self.engine.connect())
        metadata_diff = compare_metadata(existing_schema_context, self.md)
        if metadata_diff:
            logger.info(f"Metadata diff:\n\n{metadata_diff}")
            raise RuntimeError(
                "Database schema has changed, run `alembic revision "
                "--autogenerate -m 'relevant message' && alembic upgrade head`."
            )

    def _handle_str_output(self, context: OutputContext, query: str):
        """Execute a sql query on the database.

        This is used for creating output views in the database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            query: sql query to execute in the database.
        """
        engine = self.engine
        table_name = get_table_name_from_context(context)

        # Check if there is a Resource in self.package for table_name.
        # We don't want folks creating views without adding package metadata.
        try:
            _ = self.package.get_resource(table_name)
        except ValueError as err:
            raise ValueError(
                f"{table_name} does not appear in pudl.metadata.resources. "
                "Check for typos, or add the table to the metadata and recreate the "
                f"PUDL SQlite database. It's also possible that {table_name} is one of "
                "the tables that does not get loaded into the PUDL SQLite DB because "
                "it's a work in progress or is distributed in Apache Parquet format."
            ) from err

        with engine.begin() as con:
            # Drop the existing view if it exists and create the new view.
            # TODO (bendnorman): parameterize this safely.
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
            con.execute(query)

    def _handle_pandas_output(self, context: OutputContext, df: pd.DataFrame):
        """Enforce PUDL DB schema and write dataframe to SQLite."""
        table_name = get_table_name_from_context(context)
        # If table_name doesn't show up in the self.md object, this will raise an error
        sa_table = self._get_sqlalchemy_table(table_name)
        res = self.package.get_resource(table_name)

        df = res.enforce_schema(df)
        with self.engine.begin() as con:
            # Remove old table records before loading to db
            con.execute(sa_table.delete())

            df.to_sql(
                table_name,
                con,
                if_exists="append",
                index=False,
                chunksize=100_000,
                dtype={c.name: c.type for c in sa_table.columns},
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        table_name = get_table_name_from_context(context)

        # Check if there is a Resource in self.package for table_name
        try:
            res = self.package.get_resource(table_name)
        except ValueError as err:
            raise ValueError(
                f"{table_name} does not appear in pudl.metadata.resources. "
                "Check for typos, or add the table to the metadata and recreate the "
                f"PUDL SQlite database. It's also possible that {table_name} is one of "
                "the tables that does not get loaded into the PUDL SQLite DB because "
                "it's a work in progress or is distributed in Apache Parquet format."
            ) from err

        with self.engine.begin() as con:
            try:
                df = pd.concat(
                    [
                        res.enforce_schema(chunk_df)
                        for chunk_df in pd.read_sql_table(
                            table_name, con, chunksize=100_000
                        )
                    ]
                )
            except ValueError as err:
                raise ValueError(
                    f"{table_name} not found. Either the table was dropped "
                    "or it doesn't exist in the pudl.metadata.resources."
                    "Add the table to the metadata and recreate the database."
                ) from err
            if df.empty:
                raise AssertionError(
                    f"The {table_name} table is empty. Materialize the {table_name} "
                    "asset so it is available in the database."
                )
        return df


@io_manager(
    config_schema={
        "write_to_parquet": Field(
            bool,
            description="""If true, data will be written to parquet files,
                in addition to the SQLite database.""",
            default_value=True,
        ),
        "read_from_parquet": Field(
            bool,
            description="""If True, the canonical source of data for reads
                will be parquet files. Otherwise, data will be read from the
                SQLite database.""",
            default_value=True,
        ),
    }
)
def pudl_mixed_format_io_manager(init_context: InitResourceContext) -> IOManager:
    """Create a SQLiteManager dagster resource for the pudl database."""
    return PudlMixedFormatIOManager(
        write_to_parquet=init_context.resource_config["write_to_parquet"],
        read_from_parquet=init_context.resource_config["read_from_parquet"],
    )


@io_manager
def parquet_io_manager(init_context: InitResourceContext) -> IOManager:
    """Create a Parquet only IO manager."""
    return PudlParquetIOManager()


class FercSQLiteIOManager(SQLiteIOManager):
    """IO Manager for reading tables from FERC databases.

    This class should be subclassed and the load_input and handle_output methods should
    be implemented.

    This IOManager expects the database to already exist.
    """

    def __init__(
        self,
        base_dir: str = None,
        db_name: str = None,
        md: sa.MetaData = None,
        timeout: float = 1_000.0,
    ):
        """Initialize FercSQLiteIOManager.

        Args:
            base_dir: base directory where all the step outputs which use this object
                manager will be stored in.
            db_name: the name of sqlite database.
            md: database metadata described as a SQLAlchemy MetaData object. If not
                specified, default to metadata stored in the pudl.metadata subpackage.
            timeout: How many seconds the connection should wait before raising an
                exception, if the database is locked by another connection.  If another
                connection opens a transaction to modify the database, it will be locked
                until that transaction is committed.
        """
        # TODO(rousik): Note that this is a bit of a partially implemented IO manager that
        # is not actually used for writing anything. Given that this is derived from base
        # SqliteIOManager, we do not support handling of parquet formats. This is probably
        # okay for now.
        super().__init__(base_dir, db_name, md, timeout)

    def _setup_database(self, timeout: float = 1_000.0) -> sa.Engine:
        """Create database engine and read the metadata.

        Args:
            timeout: How many seconds the connection should wait before raising an
                exception, if the database is locked by another connection.  If another
                connection opens a transaction to modify the database, it will be locked
                until that transaction is committed.

        Returns:
            engine: SQL Alchemy engine that connects to a database in the base_dir.
        """
        # If the sqlite directory doesn't exist, create it.
        db_path = self.base_dir / f"{self.db_name}.sqlite"
        if not db_path.exists():
            raise ValueError(
                f"No DB found at {db_path}. Run the job that creates the "
                f"{self.db_name} database."
            )

        engine = sa.create_engine(
            f"sqlite:///{db_path}", connect_args={"timeout": timeout}
        )

        # Connect to the local SQLite DB and read its structure.
        ferc1_meta = sa.MetaData()
        ferc1_meta.reflect(engine)
        self.md = ferc1_meta

        return engine

    def handle_output(self, context: OutputContext, obj):
        """Handle an op or asset output."""
        raise NotImplementedError(
            "FercSQLiteIOManager can't write outputs. Subclass FercSQLiteIOManager and "
            "implement the handle_output method."
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        raise NotImplementedError(
            "FercSQLiteIOManager can't load inputs. Subclass FercSQLiteIOManager and "
            "implement the load_input method."
        )


class FercDBFSQLiteIOManager(FercSQLiteIOManager):
    """IO Manager for only reading tables from the FERC 1 database.

    This IO Manager is for reading data only. It does not handle outputs because the raw
    FERC tables are not known prior to running the ETL and are not recorded in our
    metadata.
    """

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output."""
        raise NotImplementedError("FercDBFSQLiteIOManager can't write outputs yet.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        # TODO (daz): this is hard-coded to FERC1, though this is nominally for all FERC datasets.
        ferc1_settings = context.resources.dataset_settings.ferc1

        table_name = get_table_name_from_context(context)
        # Remove preceding asset name metadata
        table_name = table_name.replace("raw_ferc1_dbf__", "")

        # Check if the table_name exists in the self.md object
        _ = self._get_sqlalchemy_table(table_name)

        engine = self.engine

        with engine.begin() as con:
            return pd.read_sql_query(
                f"SELECT * FROM {table_name} "  # noqa: S608
                "WHERE report_year BETWEEN :min_year AND :max_year;",
                con=con,
                params={
                    "min_year": min(ferc1_settings.dbf_years),
                    "max_year": max(ferc1_settings.dbf_years),
                },
            ).assign(sched_table_name=table_name)


@io_manager(required_resource_keys={"dataset_settings"})
def ferc1_dbf_sqlite_io_manager(init_context) -> FercDBFSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc1 dbf database."""
    return FercDBFSQLiteIOManager(
        base_dir=PudlPaths().output_dir,
        db_name="ferc1_dbf",
    )


class FercXBRLSQLiteIOManager(FercSQLiteIOManager):
    """IO Manager for only reading tables from the XBRL database.

    This IO Manager is for reading data only. It does not handle outputs because the raw
    FERC tables are not known prior to running the ETL and are not recorded in our
    metadata.
    """

    @staticmethod
    def refine_report_year(df: pd.DataFrame, xbrl_years: list[int]) -> pd.DataFrame:
        """Set a fact's report year by its actual dates.

        Sometimes a fact belongs to a context which has no ReportYear associated with
        it; other times there are multiple ReportYears associated with a single filing.
        In these cases the report year of a specific fact may be associated with the
        other years in the filing.

        In many cases we can infer the actual report year from the fact's associated
        time period - either duration or instant.
        """
        is_duration = len({"start_date", "end_date"} - set(df.columns)) == 0
        is_instant = "date" in df.columns

        def get_year(df: pd.DataFrame, col: str) -> pd.Series:
            datetimes = pd.to_datetime(df.loc[:, col], format="%Y-%m-%d", exact=False)
            if datetimes.isna().any():
                raise ValueError(f"{col} has null values!")
            return datetimes.apply(lambda x: x.year)

        if is_duration:
            start_years = get_year(df, "start_date")
            end_years = get_year(df, "end_date")
            if not (start_years == end_years).all():
                raise ValueError("start_date and end_date are in different years!")
            new_report_years = start_years
        elif is_instant:
            new_report_years = get_year(df, "date")
        else:
            raise ValueError("Attempted to read a non-instant, non-duration table.")

        # we include XBRL data from before our "officially supported" XBRL
        # range because we want to use it to set start-of-year values for the
        # first XBRL year.
        xbrl_years_plus_one_previous = [min(xbrl_years) - 1] + xbrl_years
        return (
            df.assign(report_year=new_report_years)
            .loc[lambda df: df.report_year.isin(xbrl_years_plus_one_previous)]
            .reset_index(drop=True)
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output."""
        raise NotImplementedError("FercXBRLSQLiteIOManager can't write outputs yet.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        ferc_settings = getattr(
            context.resources.dataset_settings,
            re.search(r"ferc\d+", self.db_name).group(),
        )

        table_name = get_table_name_from_context(context)
        # Remove preceding asset name metadata
        table_name = table_name.replace(f"raw_{self.db_name}__", "")

        # TODO (bendnorman): Figure out a better to handle tables that
        # don't have duration and instant
        # Not every table contains both instant and duration
        # Return empty dataframe if table doesn't exist
        if table_name not in self.md.tables:
            return pd.DataFrame()

        engine = self.engine

        sched_table_name = re.sub("_instant|_duration", "", table_name)
        with engine.begin() as con:
            df = pd.read_sql(
                f"SELECT {table_name}.* FROM {table_name}",  # noqa: S608 - table names not supplied by user
                con=con,
            ).assign(sched_table_name=sched_table_name)

        return df.pipe(
            FercXBRLSQLiteIOManager.refine_report_year,
            xbrl_years=ferc_settings.xbrl_years,
        )


@io_manager(required_resource_keys={"dataset_settings"})
def ferc1_xbrl_sqlite_io_manager(init_context) -> FercXBRLSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc1 xbrl database."""
    return FercXBRLSQLiteIOManager(
        base_dir=PudlPaths().output_dir,
        db_name="ferc1_xbrl",
    )


@io_manager(required_resource_keys={"dataset_settings"})
def ferc714_xbrl_sqlite_io_manager(init_context) -> FercXBRLSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc714 xbrl database."""
    return FercXBRLSQLiteIOManager(
        base_dir=PudlPaths().output_dir,
        db_name="ferc714_xbrl",
    )


class EpaCemsIOManager(UPathIOManager):
    """An IO Manager that dumps outputs to a parquet file."""

    extension: str = ".parquet"

    def __init__(self, base_path: UPath, schema: pa.Schema) -> None:
        """Initialize a EpaCemsIOManager."""
        super().__init__(base_path=base_path)
        self.schema = schema

    def dump_to_path(self, context: OutputContext, obj: dd.DataFrame, path: UPath):
        """Write dataframe to parquet file."""
        raise NotImplementedError("This IO Manager doesn't support writing data.")

    def load_from_path(self, context: InputContext, path: UPath) -> dd.DataFrame:
        """Load a directory of parquet files to a dask dataframe."""
        logger.info(f"Reading parquet file from {path}")
        return dd.read_parquet(
            path,
            engine="pyarrow",
            index=False,
            split_row_groups=True,
        )


@io_manager
def epacems_io_manager(
    init_context: InitResourceContext,
) -> EpaCemsIOManager:
    """IO Manager that writes EPA CEMS partitions to individual parquet files."""
    schema = Resource.from_id("core_epacems__hourly_emissions").to_pyarrow()
    return EpaCemsIOManager(base_path=UPath(PudlPaths().parquet_path()), schema=schema)
