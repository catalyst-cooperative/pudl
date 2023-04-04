"""Dagster IO Managers."""
import re
from pathlib import Path
from sqlite3 import sqlite_version

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
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
from sqlalchemy.exc import SQLAlchemyError
from upath import UPath

import pudl
from pudl.helpers import EnvVar
from pudl.metadata.classes import Package, Resource

logger = pudl.logging_helpers.get_logger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


class ForeignKeyError(SQLAlchemyError):
    """Raised when data in a database violates a foreign key constraint."""

    def __init__(
        self, child_table: str, parent_table: str, foreign_key: str, rowids: list[int]
    ):
        """Initialize a new ForeignKeyError object."""
        self.child_table = child_table
        self.parent_table = parent_table
        self.foreign_key = foreign_key
        self.rowids = rowids

    def __str__(self):
        """Create string representation of ForeignKeyError object."""
        return (
            f"Foreign key error for table: {self.child_table} -- {self.parent_table} "
            f"{self.foreign_key} -- on rows {self.rowids}\n"
        )

    def __eq__(self, other):
        """Compare a ForeignKeyError with another object."""
        if isinstance(other, ForeignKeyError):
            return (
                (self.child_table == other.child_table)
                and (self.parent_table == other.parent_table)
                and (self.foreign_key == other.foreign_key)
                and (self.rowids == other.rowids)
            )
        return False


class ForeignKeyErrors(SQLAlchemyError):
    """Raised when data in a database violate multiple foreign key constraints."""

    def __init__(self, fk_errors: list[ForeignKeyError]):
        """Initialize a new ForeignKeyErrors object."""
        self.fk_errors = fk_errors

    def __str__(self):
        """Create string representation of ForeignKeyErrors object."""
        fk_errors = list(map(lambda x: str(x), self.fk_errors))
        return "\n".join(fk_errors)

    def __iter__(self):
        """Iterate over the fk errors."""
        return self.fk_errors

    def __getitem__(self, idx):
        """Index the fk errors."""
        return self.fk_errors[idx]


class SQLiteIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(
        self,
        base_dir: str,
        db_name: str,
        md: sa.MetaData = None,
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
        self.md = md
        if not self.md:
            self.md = sa.MetaData()

        self.engine = self._setup_database(timeout=timeout)

    def _get_table_name(self, context) -> str:
        """Get asset name from dagster context object."""
        if context.has_asset_key:
            table_name = context.asset_key.to_python_identifier()
        else:
            table_name = context.get_identifier()
        return table_name

    def _setup_database(self, timeout: float = 1_000.0) -> sa.engine.Engine:
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
                f"{sa_table} not found in database metadata. Either add the table to "
                "the metadata or use a different IO Manager."
            )
        return sa_table

    def _get_fk_list(self, table: str) -> pd.DataFrame:
        """Retrieve a dataframe of foreign keys for a table.

        Description from the SQLite Docs: 'This pragma returns one row for each foreign
        key constraint created by a REFERENCES clause in the CREATE TABLE statement of
        table "table-name".'

        The PRAGMA returns one row for each field in a foreign key constraint. This
        method collapses foreign keys with multiple fields into one record for
        readability.
        """
        with self.engine.connect() as con:
            table_fks = pd.read_sql_query(f"PRAGMA foreign_key_list({table});", con)

        # Foreign keys with multiple fields are reported in separate records.
        # Combine the multiple fields into one string for readability.
        # Drop duplicates so we have one FK for each table and foreign key id
        table_fks["fk"] = table_fks.groupby("table")["to"].transform(
            lambda field: "(" + ", ".join(field) + ")"
        )
        table_fks = table_fks[["id", "table", "fk"]].drop_duplicates()

        # Rename the fields so we can easily merge with the foreign key errors.
        table_fks = table_fks.rename(columns={"id": "fkid", "table": "parent"})
        table_fks["table"] = table
        return table_fks

    def check_foreign_keys(self) -> None:
        """Check foreign key relationships in the database.

        The order assets are loaded into the database will not satisfy foreign key
        constraints so we can't enable foreign key constraints. However, we can
        check for foreign key failures once all of the data has been loaded into
        the database using the `foreign_key_check` and `foreign_key_list` PRAGMAs.

        You can learn more about the PRAGMAs in the `SQLite docs
        <https://www.sqlite.org/pragma.html#pragma_foreign_key_check>`__.

        Raises:
            ForeignKeyErrors: if data in the database violate foreign key constraints.
        """
        logger.info(f"Running foreign key check on {self.db_name} database.")
        with self.engine.connect() as con:
            fk_errors = pd.read_sql_query("PRAGMA foreign_key_check;", con)

        if not fk_errors.empty:
            # Merge in the actual FK descriptions
            tables_with_fk_errors = fk_errors.table.unique().tolist()
            table_foreign_keys = pd.concat(
                [self._get_fk_list(table) for table in tables_with_fk_errors]
            )

            fk_errors_with_keys = fk_errors.merge(
                table_foreign_keys,
                how="left",
                on=["parent", "fkid", "table"],
                validate="m:1",
            )

            errors = []
            # For each foreign key error, raise a ForeignKeyError
            for (
                table_name,
                parent_name,
                parent_fk,
            ), parent_fk_df in fk_errors_with_keys.groupby(["table", "parent", "fk"]):
                errors.append(
                    ForeignKeyError(
                        child_table=table_name,
                        parent_table=parent_name,
                        foreign_key=parent_fk,
                        rowids=parent_fk_df["rowid"].values,
                    )
                )
            raise ForeignKeyErrors(errors)

        logger.info("Success! No foreign key constraint errors found.")

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
        table_name = self._get_table_name(context)
        sa_table = self._get_sqlalchemy_table(table_name)

        column_difference = set(sa_table.columns.keys()) - set(df.columns)
        if column_difference:
            raise ValueError(
                f"{table_name} dataframe is missing columns: {column_difference}"
            )

        engine = self.engine
        with engine.connect() as con:
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
        table_name = self._get_table_name(context)

        with engine.connect() as con:
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
        table_name = self._get_table_name(context)
        # Check if the table_name exists in the self.md object
        _ = self._get_sqlalchemy_table(table_name)

        engine = self.engine

        with engine.connect() as con:
            try:
                df = pd.read_sql_table(table_name, con)
            except ValueError:
                raise ValueError(
                    f"{table_name} not found. Either the table was dropped "
                    "or it doesn't exist in the pudl.metadata.resources."
                    "Add the table to the metadata and recreate the database."
                )
            if df.empty:
                raise AssertionError(
                    f"The {table_name} table is empty. Materialize "
                    "the {table_name} asset so it is available in the database."
                )
            return df


class PudlSQLiteIOManager(SQLiteIOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database.

    This class extends the SQLiteIOManager class to manage database metadata and dtypes
    using the :class:`pudl.metadata.classes.Package` class.
    """

    def __init__(
        self,
        base_dir: str = None,
        db_name: str = None,
        package: Package = None,
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

                Every table that appears in `self.md` is sepcified in `self.package`
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
        if not package:
            package = Package.from_resource_ids()
        self.package = package
        md = self.package.to_sql()
        super().__init__(base_dir, db_name, md, timeout)

    def _handle_str_output(self, context: OutputContext, query: str):
        """Execute a sql query on the database.

        This is used for creating output views in the database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            query: sql query to execute in the database.
        """
        engine = self.engine
        table_name = self._get_table_name(context)

        # Check if there is a Resource in self.package for table_name.
        # We don't want folks creating views without adding package metadata.
        try:
            _ = self.package.get_resource(table_name)
        except ValueError:
            raise ValueError(
                f"{table_name} does not appear in pudl.metadata.resources. "
                "Check for typos, or add the table to the metadata and recreate the "
                f"PUDL SQlite database. It's also possible that {table_name} is one of "
                "the tables that does not get loaded into the PUDL SQLite DB because "
                "it's a work in progress or is distributed in Apache Parquet format."
            )

        with engine.connect() as con:
            # Drop the existing view if it exists and create the new view.
            # TODO (bendnorman): parameterize this safely.
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
            con.execute(query)

    def _handle_pandas_output(self, context: OutputContext, df: pd.DataFrame):
        """Enforce PUDL DB schema and write dataframe to SQLite."""
        table_name = self._get_table_name(context)
        # If table_name doesn't show up in the self.md object, this will raise an error
        sa_table = self._get_sqlalchemy_table(table_name)
        res = self.package.get_resource(table_name)

        df = res.enforce_schema(df)

        with self.engine.connect() as con:
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
        table_name = self._get_table_name(context)

        # Check if there is a Resource in self.package for table_name
        try:
            res = self.package.get_resource(table_name)
        except ValueError:
            raise ValueError(
                f"{table_name} does not appear in pudl.metadata.resources. "
                "Check for typos, or add the table to the metadata and recreate the "
                f"PUDL SQlite database. It's also possible that {table_name} is one of "
                "the tables that does not get loaded into the PUDL SQLite DB because "
                "it's a work in progress or is distributed in Apache Parquet format."
            )

        with self.engine.connect() as con:
            try:
                df = pd.concat(
                    [
                        res.enforce_schema(chunk_df)
                        for chunk_df in pd.read_sql_table(
                            table_name, con, chunksize=100_000
                        )
                    ]
                )
            except ValueError:
                raise ValueError(
                    f"{table_name} not found. Either the table was dropped "
                    "or it doesn't exist in the pudl.metadata.resources."
                    "Add the table to the metadata and recreate the database."
                )
            if df.empty:
                raise AssertionError(
                    f"The {table_name} table is empty. Materialize the {table_name} "
                    "asset so it is available in the database."
                )
        return df


@io_manager(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    }
)
def pudl_sqlite_io_manager(init_context) -> PudlSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the pudl database."""
    base_dir = init_context.resource_config["pudl_output_path"]
    return PudlSQLiteIOManager(base_dir=base_dir, db_name="pudl")


class FercSQLiteIOManager(SQLiteIOManager):
    """IO Manager for reading tables from FERC databases.

    This class should be subclassed and the load_input and handle_output methods should
    be implemented.

    This IOManager exepcts the database to already exist.
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
        super().__init__(base_dir, db_name, md, timeout)

    def _setup_database(self, timeout: float = 1_000.0) -> sa.engine.Engine:
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
        ferc1_settings = context.resources.dataset_settings.ferc1

        table_name = self._get_table_name(context)

        # Check if the table_name exists in the self.md object
        _ = self._get_sqlalchemy_table(table_name)

        engine = self.engine

        with engine.connect() as con:
            return pd.read_sql_query(
                f"SELECT * FROM {table_name} "  # nosec: B608
                "WHERE report_year BETWEEN :min_year AND :max_year;",
                con=con,
                params={
                    "min_year": min(ferc1_settings.dbf_years),
                    "max_year": max(ferc1_settings.dbf_years),
                },
            ).assign(sched_table_name=table_name)


@io_manager(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
    required_resource_keys={"dataset_settings"},
)
def ferc1_dbf_sqlite_io_manager(init_context) -> FercDBFSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc1 dbf database."""
    base_dir = init_context.resource_config["pudl_output_path"]
    return FercDBFSQLiteIOManager(
        base_dir=base_dir,
        db_name="ferc1",
    )


class FercXBRLSQLiteIOManager(FercSQLiteIOManager):
    """IO Manager for only reading tables from the XBRL database.

    This IO Manager is for reading data only. It does not handle outputs because the raw
    FERC tables are not known prior to running the ETL and are not recorded in our
    metadata.
    """

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output."""
        raise NotImplementedError("FercXBRLSQLiteIOManager can't write outputs yet.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        ferc1_settings = context.resources.dataset_settings.ferc1

        table_name = self._get_table_name(context)
        # TODO (bendnorman): Figure out a better to handle tables that
        # don't have duration and instant
        # Not every table contains both instant and duration
        # Return empty dataframe if table doesn't exist
        if table_name not in self.md.tables:
            return pd.DataFrame()

        engine = self.engine

        id_table = "identification_001_duration"

        sched_table_name = re.sub("_instant|_duration", "", table_name)
        with engine.connect() as con:
            return pd.read_sql(
                f"""
                SELECT {table_name}.*, {id_table}.report_year FROM {table_name}
                JOIN {id_table} ON {id_table}.filing_name = {table_name}.filing_name
                WHERE {id_table}.report_year BETWEEN :min_year AND :max_year;
                """,  # nosec: B608 - table names not supplied by user
                con=con,
                params={
                    "min_year": min(ferc1_settings.xbrl_years),
                    "max_year": max(ferc1_settings.xbrl_years),
                },
            ).assign(sched_table_name=sched_table_name)


@io_manager(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    },
    required_resource_keys={"dataset_settings"},
)
def ferc1_xbrl_sqlite_io_manager(init_context) -> FercXBRLSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc1 dbf database."""
    base_dir = init_context.resource_config["pudl_output_path"]
    return FercXBRLSQLiteIOManager(
        base_dir=base_dir,
        db_name="ferc1_xbrl",
    )


class PandasParquetIOManager(UPathIOManager):
    """An IO Manager that dumps outputs to a parquet file."""

    extension: str = ".parquet"

    def __init__(self, base_path: UPath, schema: pa.Schema) -> None:
        """Initialize a PandasParquetIOManager."""
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
            use_nullable_dtypes=True,
            engine="pyarrow",
            index=False,
            split_row_groups=True,
        )


@io_manager(
    config_schema={
        "base_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            is_required=False,
            default_value=None,
        )
    }
)
def epacems_io_manager(
    init_context: InitResourceContext,
) -> PandasParquetIOManager:
    """IO Manager that writes EPA CEMS partitions to individual parquet files."""
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    base_path = UPath(init_context.resource_config["base_path"])
    return PandasParquetIOManager(base_path=base_path, schema=schema)
