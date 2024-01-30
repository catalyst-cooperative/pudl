"""Dagster IO Managers."""
import json
import os
import re
from pathlib import Path
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
from upath import UPath

import pudl
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

# TODO(rousik): This should be part of our project dependency and
# we guarantee modern versions so maybe we can drop this test now.
# This version is from 2020-05-22 which is really really old.
MINIMUM_SQLITE_VERSION = "3.32.0"


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    # TODO(rousik): Figure out which kind of identifier is used when.
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


class SQLiteHelper:
    """A simple helper class to interact with sqlite database.

    This provides light wrapper for reading from and writing to sqlite
    databases and for dealing with the schema.
    """
    def __init__(self, db_path: Path, md: sa.MetaData | None = None,
                 reflect_metadata: bool = False,
                 create_if_missing: bool = False,
                 timeout: float = 1_000.0):
        """Initializes connection to sqlite database.

        Args:
            db_path: file where the sqlite database is stored
            md: database metadata. This could be pre-existing schema
                that should be written into the database, or can
                be absent.
            reflect_metadata: if True, use reflection to infer the
                database metadata.
            create_if_missing: if True, new database will be created,
                including parent paths. Otherwise, this will throw
                an exception.
            timeout: How many seconds the connection should wait before raising
                an exception, if the database is locked by another connection.
                If another connection opens a transaction to modify the database,
                it will be locked until that transaction is committed.
        """
        self.engine = sa.create_engine(
            sa.URL("sqlite", database=db_path.as_posix()),
            connect_args={"timeout": timeout},
        )
        self.md = md
        if not db_path.exists():
            if not create_if_missing:
                # TODO(rousik): perhaps use better exception for this.
                raise ValueError(
                    f"No DB found at {db_path}. Run the job that creates the "
                    f"{self.db_name} database."
                )
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.touch()
            # TODO(rousik): Do we need metadata? We could easily
            # start with empty metadata.
            if self.md is not None:
                self.md.create_all(self.engine)

        if reflect_metadata:
            self.md = sa.MetaData().reflect(self.engine)

    def get_table_meta(self, table_name: str) -> sa.Table:
        """Returns SQLAlchemy metadata for given table."""
        try:
            self.md.tables.get(table_name)
        except ValueError:
            raise ValueError(
                f"{table_name} not found in database metadata. Either add table to "
                f"the metadata or use different IO Manager."
            )

    def write_table(self, table_name: str, df: pd.DataFrame) -> None:
        """Writes contents of a dataframe into a table.

        Deletes records from the table prior to writing new data.
        """
        table = self.get_table_meta(table_name)
        with self.engine.begin() as con:
            # Remove old table records before loading to db
            con.execute(table.delete())
            df.to_sql(
                table_name,
                con,
                if_exists="append",
                index=False,
                chunksize=100_000,
                dtype={c.name: c.type for c in table.columns},
            )

    def execute_sql(self, table_name: str, sql_query: str) -> None:
        """Runs ad-hoc sql query.

        This can be used to create view. When this is executed,
        existing view named {table_name} will be dropped.
        """
        # Ensure that table/view is known.
        _ = self.get_table_meta(table_name)
        with self.engine.begin() as con:
            con.execute(f"DROP VIEW IF EXISTS {table_name}")
            con.execute(sql_query)

    def read_from_table(self, table_name: str) -> pd.DataFrame:
        """Reads data from table into pandas dataframe."""
        try:
            _ = self.get_table_meta(table_name)
        except ValueError:
            # TODO(rousik): Improve this error message, perhaps retain
            # the original one with lots of what-to-dos.
            raise ValueError(
                f"Table {table_name} doesn't exist in the schema."
            )
        with self.engine.begin() as con:
            try:
                # TODO(rousik): One version of the sqlite manager uses
                # chunked read, the other one doesn't. Is either
                # approach better or worse?
                df = pd.concat(pd.read_sql_table(
                    table_name, con, chunk_size=100_000
                ))
            except ValueError:
                # TODO(rousik): Is this possible? Shouldn't get_table_meta()
                # enforce that table exists at this point?
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

    def get_metadata_diff(self) -> Any:
        """Compares actual database schema with the one provided."""
        return compare_metadata(
            MigrationContext.configure(self.engine.connect()),
            self.md,
        )

class PudlMixedFormatIOManager(IOManager):
    """Format switching IOManager that supports sqlite and parquet.

    We currently support both sqlite and parquet formats. This io manager
    allows us to choose which formats should be used for reading and which
    should be used for reading.

    This should allow us to develop support for parquet format without
    affecting the existing sqlite-based functionality.

    Switching between reads from sqlite and parquet should allow us
    to test the two formats for eqivalence during development.
    """

    def __init__(
        self,
        write_to_parquet: bool = False,
        read_from_parquet: bool = False,
        sqlite_io_manager: IOManager | None = None,
        parquet_io_manager: IOManager | None = None,
    ):
        """Creates new instance of mixed format pudl IO manager.

        By default, data is written and read from sqlite, but experimental
        support for writing and/or reading from parquet files can be enabled
        by setting the corresponding flags to True.

        Args:
            write_to_parquet: if True, all data will be written to parquet
                files in addition to sqlite.
            read_from_parquet: if True, all data reads will be using
                parquet files as source of truth. Otherwise, data will be
                read from the sqlite database.
            sqlite_io_manager: allows substituting default sqlite io manager
                 for testing purposes.
            parquet_io_manager: allows substituting default parquet io manager
                for testing purposes.
        """
        self.write_to_parquet = write_to_parquet
        self.read_from_parquet = read_from_parquet
        if sqlite_io_manager:
            self._sqlite_io_manager = sqlite_io_manager
        else:
            self._sqlite_io_manager = PudlSQLiteIOManager(
                base_dir=PudlPaths().output_dir,
                db_name="pudl",
            )
        if parquet_io_manager:
            self._parquet_io_manager = parquet_io_manager
        else:
            self._parquet_io_manager = PudlParquetIOManager()

        if self.write_to_parquet or self.read_from_parquet:
            logger.warning(
                f"pudl_io_manager: experimental support for parquet enabled. "
                f"(read={self.read_from_parquet}, write={self.write_to_parquet})"
            )

    def get_sqlite_io_manager(self) -> IOManager:
        """Returns the embedded sqlite io manager for schema validation."""
        # TODO(rousik): instead of returning io manager, we should return interface
        # that guarantees sqlite specific functionality.
        return self._sqlite_io_manager

    def handle_output(self, context: OutputContext, obj: Any) -> Any:
        """Passes the output to the appropriate IO manager instance."""
        self._sqlite_io_manager.handle_output(context, obj)
        if self.write_to_parquet:
            self._parquet_io_manager.handle_output(context, obj)

    def load_input(self, context: InputContext) -> Any:
        """Reads input from the appropriate IO manager instance."""
        if self.read_from_parquet:
            return self._parquet_io_manager.load_input(context)
        return self._sqlite_io_manager.load_input(context)


# TODO(rousik): This should be renamed to GenericSQLiteIOManager
# for better disambiguation.
class SQLiteIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(
        self,
        base_dir: str,
        db_name: str,
        md: sa.MetaData | None = None,
    ):
        """Init a SQLiteIOmanager.

        Args:
            base_dir: base directory where all the step outputs which use this object
                manager will be stored in.
            db_name: the name of sqlite database.
            md: database metadata described as a SQLAlchemy MetaData object. If not
                specified, default to metadata stored in the pudl.metadata subpackage.

        """
        if md is None:
            # Use empty metadata if not specified. Perhaps this is not necessary thanks
            # to inner method also accepting Nones.
            md = sa.MetaData()

        self.sqlite_helper = SQLiteHelper(
            db_path=Path(base_dir) / f"{db_name}.sqlite",
            md=md,
            create_if_missing=True,
            reflect_metadata=True,
        )

    # TODO(bendnorman): str represents sql query. We should use
    # dedicated type for this.
    def handle_output(self, context: OutputContext, data: pd.DataFrame | str):
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
        table = self.sqlite_helper.get_table_meta(table_name)
        if isinstance(data, str):
            self.sqlite_helper.execute_sql(table_name, data)
        elif isinstance(data, pd.DataFrame):
            missing_columns = set(table.column.keys()) - set(data.columns)
            if missing_columns:
                raise ValueError(
                    f"DataFrame is missing columns when writing to table "
                    f"{table_name}: {', '.join(missing_columns)}"
                )
            # TODO(rousik): Check against the schema in the metadata and enforce it
            self.sqlite_helper.write_table(table_name, data)
        else:
            raise TypeError(
                "SQLiteIOManager only supports pandas DataFrames and strings of SQL "
                "queries."
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        return self.sqlite_helper.read_from_table(
            get_table_name_from_context(context),
        )


class PudlParquetIOManager(IOManager):
    """IOManager that writes pudl tables to pyarrow parquet files."""

    def handle_output(self, context: OutputContext, df: Any) -> None:
        """Writes pudl dataframe to parquet file."""
        assert isinstance(df, pd.DataFrame), "Only panda dataframes are supported."
        table_name = get_table_name_from_context(context)
        parquet_path = PudlPaths().parquet_path("pudl", table_name)
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

    def load_input(self, context: InputContext) -> Any:
        """Loads pudl table from parquet file."""
        table_name = get_table_name_from_context(context)
        parquet_path = PudlPaths().parquet_path("pudl", table_name)
        res = Resource.from_id(table_name)
        df = pq.read_table(source=parquet_path, schema=res.to_pyarrow()).to_pandas()
        return res.enforce_schema(df)


class PudlSQLiteIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database.

    This class extends the SQLiteIOManager class to manage database metadata and dtypes
    using the :class:`pudl.metadata.classes.Package` class.
    """

    def __init__(
        self,
        base_dir: str,
        db_name: str,
        package: Package | None = None,
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
        """
        self.package = package or Package.from_resource_ids()
        # TODO(rousik): We should accept full Path to the database file
        # instead. That will be simpler.
        sqlite_path = Path(base_dir) / f"{db_name}.sqlite"

        if not sqlite_path.exists():
            raise RuntimeError(
                f"{sqlite_path} not initialized! Run `alembic upgrade head`."
            )

        md = self.package.to_sql()
        self.sqlite_helper = SQLiteHelper(
            db_path= sqlite_path,
            md=md,
            reflect_metadata=False,
            create_if_missing=False,
        )
        metadata_diff = self.sqlite_helper.get_metadata_diff()
        if metadata_diff:
            logger.info(f"Metadata diff:\n\n{metadata_diff}")
            raise RuntimeError(
                "Database schema has changed, run `alembic revision "
                "--autogenerate -m 'relevant message' && alembic upgrade head`."
            )

    def handle_output(self, context: OutputContext, data: pd.DataFrame | str):
        """Writes frame or sql query to a table."""
        table_name = get_table_name_from_context(context)

        res = self.package.get_resource(table_name)
        if isinstance(data, str):
            self.sqlite_helper.execute_sql(table_name, data)
        elif isinstance(data, pd.DataFrame):
            self.sqlite_helper.write_table(
                table_name,
                res.enforce_schema(data),
            )
        else:
            raise TypeError(
                "PandasSQLiteIOManager only supports pandas DataFrames and strings of SQL "
                "queries."
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
        except ValueError:
            raise ValueError(
                f"{table_name} does not appear in pudl.metadata.resources. "
                "Check for typos, or add the table to the metadata and recreate the "
                f"PUDL SQlite database. It's also possible that {table_name} is one of "
                "the tables that does not get loaded into the PUDL SQLite DB because "
                "it's a work in progress or is distributed in Apache Parquet format."
            )
        return res.enforce_schema(
            self.sqlite_helper.read_from_table(table_name)
        )

@io_manager(
    config_schema={
        "write_to_parquet": Field(
            bool,
            description="""If true, data will be written to parquet files,
                in addition to the SQLite database.""",
            default_value=bool(os.getenv("PUDL_WRITE_TO_PARQUET")),
        ),
        "read_from_parquet": Field(
            bool,
            description="""If True, the canonical source of data for reads
                will be parquet files. Otherwise, data will be read from the
                SQLite database.""",
            default_value=bool(os.getenv("PUDL_READ_FROM_PARQUET")),
        ),
    }
)
def pudl_io_manager(init_context) -> IOManager:
    """Create a SQLiteManager dagster resource for the pudl database."""
    return PudlMixedFormatIOManager(
        write_to_parquet=init_context.resource_config["write_to_parquet"],
        read_from_parquet=init_context.resource_config["read_from_parquet"],
    )


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

        table_name = get_table_name_from_context(context)
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
    schema = Resource.from_id("core_epacems__hourly_emissions").to_pyarrow()
    return PandasParquetIOManager(
        base_path=UPath(PudlPaths().output_dir), schema=schema
    )
