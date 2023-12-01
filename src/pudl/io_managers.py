"""Dagster IO Managers."""
import json
import re
from pathlib import Path
from sqlite3 import sqlite_version

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from alembic.autogenerate.api import compare_metadata
from alembic.migration import MigrationContext
from dagster import (
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
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths

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


class ForeignKeyErrors(SQLAlchemyError):  # noqa: N818
    """Raised when data in a database violate multiple foreign key constraints."""

    def __init__(self, fk_errors: list[ForeignKeyError]):
        """Initialize a new ForeignKeyErrors object."""
        self.fk_errors = fk_errors

    def __str__(self):
        """Create string representation of ForeignKeyErrors object."""
        fk_errors = [str(x) for x in self.fk_errors]
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

    def _get_table_name(self, context) -> str:
        """Get asset name from dagster context object."""
        if context.has_asset_key:
            table_name = context.asset_key.to_python_identifier()
        else:
            table_name = context.get_identifier()
        return table_name

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

    def _get_fk_list(self, table: str) -> pd.DataFrame:
        """Retrieve a dataframe of foreign keys for a table.

        Description from the SQLite Docs: 'This pragma returns one row for each foreign
        key constraint created by a REFERENCES clause in the CREATE TABLE statement of
        table "table-name".'

        The PRAGMA returns one row for each field in a foreign key constraint. This
        method collapses foreign keys with multiple fields into one record for
        readability.
        """
        with self.engine.begin() as con:
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
        with self.engine.begin() as con:
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
        table_name = self._get_table_name(context)

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
        table_name = self._get_table_name(context)
        # Check if the table_name exists in the self.md object
        _ = self._get_sqlalchemy_table(table_name)

        engine = self.engine

        with engine.begin() as con:
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
                    f"the {table_name} asset so it is available in the database."
                )
            return df


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
        if package is None:
            package = Package.from_resource_ids()
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

        with engine.begin() as con:
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


@io_manager
def pudl_sqlite_io_manager(init_context) -> PudlSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the pudl database."""
    return PudlSQLiteIOManager(base_dir=PudlPaths().output_dir, db_name="pudl")


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

        table_name = self._get_table_name(context)
        # Remove preceeding asset name metadata
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
    def filter_for_freshest_data(
        table: pd.DataFrame, primary_key: list[str]
    ) -> pd.DataFrame:
        """Get most updated values for each XBRL context.

        An XBRL context includes an entity ID, the time period the data applies to, and
        other dimensions such as utility type. Each context has its own ID, but they are
        frequently redefined with the same contents but different IDs - so we identify
        them by their actual content.

        Each row in our SQLite database includes all the facts for one context/filing
        pair.

        If one context is represented in multiple filings, we take the facts from the
        most recently-published filing.

        This means that if a recently-published filing does not include a value for a
        fact that was previously reported, then that value will remain null. We do not
        forward-fill facts on a fact-by-fact basis.
        """
        filing_metadata_cols = {"publication_time", "filing_name"}
        xbrl_context_cols = [c for c in primary_key if c not in filing_metadata_cols]
        # we do this in multiple stages so we can log the drop-off at each stage.
        stages = [
            {
                "message": "completely duplicated rows",
                "subset": table.columns,
            },
            {
                "message": "rows that are exactly the same in multiple filings",
                "subset": [c for c in table.columns if c not in filing_metadata_cols],
            },
            {
                "message": "rows that were updated by later filings",
                "subset": xbrl_context_cols,
            },
        ]
        original = table.sort_values("publication_time")
        for stage in stages:
            deduped = original.drop_duplicates(subset=stage["subset"], keep="last")
            logger.debug(f"Dropped {len(original) - len(deduped)} {stage['message']}")
            original = deduped

        return deduped

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
            datetimes = pd.to_datetime(df.loc[:, col])
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

    def _get_primary_key(self, sched_table_name: str) -> list[str]:
        # TODO (daz): as of 2023-10-13, our datapackage.json is merely
        # "frictionless-like" so we manually parse it as JSON. once we make our
        # datapackage.json conformant, we will need to at least update the
        # "primary_key" to "primaryKey", but maybe there will be other changes
        # as well.
        with (self.base_dir / f"{self.db_name}_datapackage.json").open() as f:
            datapackage = json.loads(f.read())
        [table_resource] = [
            tr for tr in datapackage["resources"] if tr["name"] == sched_table_name
        ]
        return table_resource["schema"]["primary_key"]

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output."""
        raise NotImplementedError("FercXBRLSQLiteIOManager can't write outputs yet.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        # TODO (daz): this is hard-coded to FERC1, though this is nominally for all FERC datasets.
        ferc1_settings = context.resources.dataset_settings.ferc1

        table_name = self._get_table_name(context)
        # Remove preceeding asset name metadata
        table_name = table_name.replace("raw_ferc1_xbrl__", "")

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

        primary_key = self._get_primary_key(table_name)

        return (
            df.pipe(
                FercXBRLSQLiteIOManager.filter_for_freshest_data,
                primary_key=primary_key,
            )
            .pipe(
                FercXBRLSQLiteIOManager.refine_report_year,
                xbrl_years=ferc1_settings.xbrl_years,
            )
            .drop(columns=["publication_time"])
        )


@io_manager(required_resource_keys={"dataset_settings"})
def ferc1_xbrl_sqlite_io_manager(init_context) -> FercXBRLSQLiteIOManager:
    """Create a SQLiteManager dagster resource for the ferc1 dbf database."""
    return FercXBRLSQLiteIOManager(
        base_dir=PudlPaths().output_dir,
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
            engine="pyarrow",
            index=False,
            split_row_groups=True,
        )


@io_manager
def epacems_io_manager(
    init_context: InitResourceContext,
) -> PandasParquetIOManager:
    """IO Manager that writes EPA CEMS partitions to individual parquet files."""
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()
    return PandasParquetIOManager(
        base_path=UPath(PudlPaths().output_dir), schema=schema
    )
