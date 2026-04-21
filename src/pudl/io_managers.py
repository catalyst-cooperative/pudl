"""Dagster IO Managers."""

import json
import re
from functools import cached_property
from pathlib import Path
from sqlite3 import sqlite_version
from typing import Literal

import dagster as dg
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from alembic.autogenerate.api import compare_metadata
from alembic.migration import MigrationContext
from dagster import (
    ConfigurableIOManager,
    DagsterInvariantViolationError,
    InputContext,
    IOManager,
    OutputContext,
)
from packaging import version
from pydantic import model_validator

import pudl
from pudl.ferc_sqlite_provenance import (
    FercSQLiteProvenance,
    assert_ferc_sqlite_compatible,
)
from pudl.helpers import get_parquet_table, get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Package, Resource
from pudl.resources import (
    PudlEtlSettingsResource,
    ZenodoDoiSettingsResource,
    etl_settings,
    zenodo_dois,
)
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


def _get_dagster_instance_if_available(
    context: InputContext,
) -> dg.DagsterInstance | None:
    """Return the Dagster instance from an input context if one was provided.

    Returns ``None`` in two cases where provenance checks should be skipped:

    * The context has no attached instance (e.g. ad hoc ``InputContext`` objects built
      by notebook or integration-test helpers).
    * The instance is ephemeral (created by ``execute_in_process()`` without an explicit
      ``instance=`` argument). An ephemeral instance has an empty event log, so
      provenance checks against it would always raise rather than meaningfully validate.
    """
    try:
        instance = context.instance
        return None if instance.is_ephemeral else instance
    except DagsterInvariantViolationError:
        return None


def get_table_name_from_context(context: InputContext | OutputContext) -> str:
    """Retrieves the table name from the context object."""
    # TODO(rousik): Figure out which kind of identifier is used when.
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


def get_ferc_form_name(db_name: str) -> str:
    """Extract the FERC form name from a SQLite database name."""
    match: re.Match[str] | None = re.search(r"ferc\d+", db_name)
    if match is None:
        raise ValueError(f"Could not determine FERC form from db_name={db_name!r}")
    return match.group()


class PudlMixedFormatIOManager(ConfigurableIOManager):
    """Format switching IOManager that supports sqlite and parquet.

    This IOManager provides for the use of parquet files along with the standard SQLite
    database produced by PUDL.
    """

    write_to_parquet: bool = True
    """If true, data will be written to parquet files."""

    read_from_parquet: bool = True
    """If true, data will be read from parquet files instead of sqlite."""

    @model_validator(mode="after")
    def validate_parquet_settings(self) -> "PudlMixedFormatIOManager":
        """Ensure the configured read/write mode is internally consistent."""
        if self.read_from_parquet and not self.write_to_parquet:
            raise RuntimeError(
                "read_from_parquet cannot be set when write_to_parquet is False."
            )
        return self

    @cached_property
    def _sqlite_io_manager(self) -> "PudlSQLiteIOManager":
        """Build the SQLite-backed runtime IO manager lazily."""
        return PudlSQLiteIOManager(
            base_dir=PudlPaths().output_dir,
            db_name="pudl",
        )

    @cached_property
    def _parquet_io_manager(self) -> "PudlParquetIOManager":
        """Build the Parquet-backed runtime IO manager lazily."""
        return PudlParquetIOManager()

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """Passes the output to the appropriate IO manager instance."""
        self._sqlite_io_manager.handle_output(context, obj)
        if self.write_to_parquet:
            self._parquet_io_manager.handle_output(context, obj)

    def load_input(
        self, context: InputContext
    ) -> pd.DataFrame | gpd.GeoDataFrame | pl.LazyFrame:
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

    def _handle_pandas_output(self, context: OutputContext, df: pd.DataFrame) -> None:
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

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """Handle an op or asset output.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            obj: a dataframe to add to the database.

        Raises:
            TypeError: if an asset or op returns an unsupported datatype.
        """
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"SQLiteIOManager only supports pandas DataFrames, got {type(obj)}."
            )
        self._handle_pandas_output(context, obj)

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

    def handle_output(
        self, context: OutputContext, obj: pd.DataFrame | pl.LazyFrame
    ) -> None:
        """Writes pudl dataframe to parquet file."""
        table_name = get_table_name_from_context(context)
        res = Resource.from_id(table_name)
        parquet_path = PudlPaths().parquet_path(table_name)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(obj, pd.DataFrame):
            df = res.enforce_schema(obj)
            pa_schema = res.to_pyarrow()
            df.to_parquet(
                path=parquet_path,
                index=False,
                schema=pa_schema,
            )
        elif isinstance(obj, pl.LazyFrame):
            obj.cast(res.to_polars_dtypes()).sink_parquet(
                parquet_path,
                engine="streaming",
                row_group_size=100_000,
            )
        else:
            raise TypeError(
                "PudlParquetIOManager only supports pandas DataFrames and Polars LazyFrames"
                f", got {type(obj)}."
            )

    def load_input(
        self, context: InputContext
    ) -> pd.DataFrame | gpd.GeoDataFrame | pl.LazyFrame:
        """Loads pudl table from parquet file."""
        table_name = get_table_name_from_context(context)
        if context.dagster_type.typing_type == pl.LazyFrame:
            df = get_parquet_table_polars(table_name)
        else:
            df = get_parquet_table(table_name)
        return df


class PudlGeoParquetIOManager(PudlParquetIOManager):
    """Do some extra work to output valid GeoParquet files when appropriate."""

    def _create_geoparquet_metadata(self, gdf: gpd.GeoDataFrame, res: Resource) -> str:
        """Create GeoParquet metadata JSON string."""
        # Find geometry columns from the resource schema
        geometry_columns = {}
        for field in res.schema.fields:
            if field.type == "geometry" and field.name in gdf.columns:
                geometry_columns[field.name] = {
                    "encoding": "WKB",
                    "geometry_types": [],  # Could be enhanced with actual geometry type detection
                    "crs": gdf.crs.to_wkt() if gdf.crs else None,
                    # Calculate bbox from geometry
                    "bbox": gdf.total_bounds.tolist()
                    if not gdf.empty and gdf.crs
                    else None,
                }

        # Determine primary geometry column
        primary_column = None
        if hasattr(gdf, "geometry") and gdf.geometry.name in geometry_columns:
            primary_column = gdf.geometry.name
        elif geometry_columns:
            primary_column = list(geometry_columns.keys())[0]

        geo_metadata = {
            "version": "1.0.0",
            "primary_column": primary_column,
            "columns": geometry_columns,
        }
        return json.dumps(geo_metadata)

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame) -> None:
        """Write a PUDL dataframe to GeoParquet."""
        if not isinstance(obj, gpd.GeoDataFrame):
            raise TypeError(
                f"Only geopandas dataframes are supported, got {type(obj)}."
            )
        table_name = get_table_name_from_context(context)
        parquet_path = PudlPaths().parquet_path(table_name)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        res = Resource.from_id(table_name)
        gdf = res.enforce_schema(obj)

        # Extract metadata before modifying geometry columns
        geo_metadata = self._create_geoparquet_metadata(gdf, res)
        # Convert geometry columns to WKB
        geometry_fields = [
            field.name
            for field in res.schema.fields
            if field.type == "geometry" and field.name in gdf.columns
        ]
        for field_name in geometry_fields:
            # This conversion is required to get the right data into the Parquet output
            # but it isn't technically compatible with the GeoDataFrame, so we get a
            # warning from Geopandas about the geometry column not being a geometry.
            logger.info(f"Convert geometry column {table_name}.{field_name} to WKB.")
            gdf[field_name] = gdf[field_name].to_wkb()

        # Convert to PyArrow table with explicit schema
        pa_table = pa.Table.from_pandas(
            gdf, schema=res.to_pyarrow(), preserve_index=False
        )
        # Add GeoParquet metadata
        metadata = pa_table.schema.metadata or {}
        metadata["geo"] = geo_metadata
        pa_table = pa_table.replace_schema_metadata(metadata)
        pq.write_table(pa_table, parquet_path)


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

    def _handle_pandas_output(self, context: OutputContext, df: pd.DataFrame) -> None:
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


pudl_mixed_format_io_manager = PudlMixedFormatIOManager()
parquet_io_manager = PudlParquetIOManager()
geoparquet_io_manager = PudlGeoParquetIOManager()


class FercSQLiteIOManager(SQLiteIOManager):
    """IO Manager for reading tables from FERC databases.

    This class should be subclassed and the load_input and handle_output methods should
    be implemented.

    This IOManager expects the database to already exist.
    """

    _db_path: Path  # set during _setup_database(); typed here for IDE and type-checker visibility

    def __init__(
        self,
        base_dir: str | None = None,
        db_name: str | None = None,
        md: sa.MetaData | None = None,
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
        # SQLiteIOManager, we do not support handling of parquet formats. This is probably
        # okay for now.
        super().__init__(base_dir, db_name, md, timeout)

    def _setup_database(self, timeout: float = 1_000.0) -> sa.Engine:
        """Create database engine and read metadata if the DB already exists.

        Args:
            timeout: How many seconds the connection should wait before raising an
                exception, if the database is locked by another connection.  If another
                connection opens a transaction to modify the database, it will be locked
                until that transaction is committed.

        Returns:
            engine: SQL Alchemy engine that connects to a database in the base_dir.
        """
        db_path = self.base_dir / f"{self.db_name}.sqlite"
        self._db_path = db_path

        engine = sa.create_engine(
            f"sqlite:///{db_path}", connect_args={"timeout": timeout}
        )

        # For single-pass Dagster runs, this resource may initialize before upstream
        # sqlite-producing assets have materialized the DB file.
        if db_path.exists():
            self._reflect_metadata(engine)
        else:
            logger.info(
                f"{db_path} not found during resource initialization; metadata reflection "
                "will happen on first load."
            )

        return engine

    def _reflect_metadata(self, engine: sa.Engine | None = None) -> None:
        """Reflect table metadata from the sqlite database into ``self.md``."""
        reflected = sa.MetaData()
        reflected.reflect(engine if engine is not None else self.engine)
        self.md: sa.MetaData = reflected

    def _ensure_database_ready(self) -> None:
        """Ensure the sqlite DB exists and metadata has been reflected."""
        if not self._db_path.exists():
            raise ValueError(
                f"No DB found at {self._db_path}. Run the job that creates the "
                f"{self.db_name} database."
            )
        if not self.md.tables:
            self._reflect_metadata()

    def _query(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Execute a year-filtered read against the FERC SQLite database."""
        raise NotImplementedError(
            "Subclasses of FercSQLiteIOManager must implement _query."
        )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str) -> None:
        """Handle an op or asset output."""
        raise NotImplementedError(
            "FercSQLiteIOManager can't write outputs. Subclass FercSQLiteIOManager and "
            "implement the handle_output method."
        )


class FercDbfSQLiteIOManager(FercSQLiteIOManager):
    """IO Manager for reading tables from FERC DBF SQLite databases.

    This IO Manager is for reading data only. It does not handle outputs because the raw
    FERC tables are not known prior to running the ETL and are not recorded in our
    metadata.

    The form name is inferred from ``self.db_name`` via :func:`get_ferc_form_name`, so
    a single class serves all FERC DBF datasets (ferc1_dbf, ferc2_dbf, etc.) as long as
    the corresponding settings object exposes a ``dbf_years`` attribute.
    """

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str) -> None:
        """Handle an op or asset output."""
        raise NotImplementedError("FercDbfSQLiteIOManager can't write outputs yet.")

    def _query(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Execute the year-filtered read against the FERC DBF SQLite database.

        Args:
            table_name: Name of the table to query (without the ``raw_<db_name>__``
                prefix).
            years: Years to include in the result set.
        """
        _ = self._get_sqlalchemy_table(table_name)
        with self.engine.begin() as con:
            return pd.read_sql_query(
                f"SELECT * FROM {table_name} "  # noqa: S608
                "WHERE report_year BETWEEN :min_year AND :max_year;",
                con=con,
                params={
                    "min_year": min(years),
                    "max_year": max(years),
                },
            ).assign(sched_table_name=table_name)


class _FercSQLiteConfigurableIOManagerBase(ConfigurableIOManager):
    """Base class for Dagster-native FERC SQLite IO manager wrappers.

    Holds the shared resource dependencies (``etl_settings``, ``zenodo_dois``,
    ``db_name``) and provides default delegation for ``engine``, ``handle_output``,
    and ``load_input``. Subclasses must define ``_manager`` (a ``cached_property``
        returning the appropriate underlying IO manager) and ``data_format``
    (``dbf`` or ``xbrl``).

    Note:
        This wrapper pattern is a temporary workaround for nested ``etl_settings``
        resource dependencies inside the FERC IO managers. Because Dagster wires
        resource dependencies at instantiation time, overriding the top-level
        ``etl_settings`` resource alone (e.g. in tests) is not enough — the IO
        managers must be rebuilt against the new resource instance. ``build_defs``
        in ``pudl.etl`` handles that rebuilding explicitly. A follow-up PR will
        remove the nested dependency, at which point this base class can be
        simplified or eliminated. See issue #5118
    """

    etl_settings: dg.ResourceDependency[PudlEtlSettingsResource]
    zenodo_dois: dg.ResourceDependency[ZenodoDoiSettingsResource]
    dataset: str
    data_format: Literal["dbf", "xbrl"]

    @property
    def _years_key(self) -> str:
        return f"{self.data_format}_years"

    @property
    def db_name(self) -> str:
        return f"{self.dataset}_{self.data_format}"

    @property
    def engine(self) -> sa.Engine:
        """Expose the underlying SQLAlchemy engine for tests and helpers."""
        return self._manager.engine

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str) -> None:
        """Delegate writes to the underlying runtime IO manager."""
        return self._manager.handle_output(context, obj)

    def _prepare(self, context: InputContext) -> None:
        """Ensure the database is ready and provenance is compatible with this run."""
        self._manager._ensure_database_ready()
        zenodo_doi = getattr(self.zenodo_dois, self.dataset)

        provenance = FercSQLiteProvenance(
            dataset=self.dataset,
            data_format=self.data_format,
            zenodo_doi=zenodo_doi,
            years=self.etl_settings.ferc_to_sqlite_settings.get_dataset_years(
                self.dataset, self.data_format
            ),
        )

        assert_ferc_sqlite_compatible(
            instance=_get_dagster_instance_if_available(context), provenance=provenance
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from the configured FERC SQLite database."""
        self._prepare(context)
        ferc_settings = getattr(
            self.etl_settings.dataset_settings,
            self.dataset,
        )
        table_name = get_table_name_from_context(context).replace(
            f"raw_{self.db_name}__", ""
        )
        return self._manager._query(table_name, getattr(ferc_settings, self._years_key))


class FercDbfSQLiteConfigurableIOManager(_FercSQLiteConfigurableIOManagerBase):
    """Configurable IO manager for reading tables from FERC DBF SQLite databases.

    Instantiate with ``dataset`` (``ferc1``, ``ferc714``, etc.)
    """

    data_format: Literal["dbf"] = "dbf"

    @cached_property
    def _manager(self) -> FercDbfSQLiteIOManager:
        """Build the underlying SQLite reader lazily."""
        return FercDbfSQLiteIOManager(
            base_dir=PudlPaths().output_dir,
            db_name=self.db_name,
        )


class FercXbrlSQLiteIOManager(FercSQLiteIOManager):
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

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str) -> None:
        """Handle an op or asset output."""
        raise NotImplementedError("FercXbrlSQLiteIOManager can't write outputs yet.")

    def _query(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Execute the full-table read against the FERC XBRL SQLite database.

        Args:
            table_name: Name of the table to query (without the ``raw_<db_name>__``
                prefix).
            years: Years to include in the result set (passed to
                :meth:`refine_report_year`).
        """
        # TODO (bendnorman): Figure out a better way to handle tables that
        # don't have duration and instant variants.
        # Not every table contains both instant and duration;
        # return an empty dataframe if the table doesn't exist.
        if table_name not in self.md.tables:
            return pd.DataFrame()
        sched_table_name = re.sub("_instant|_duration", "", table_name)
        with self.engine.begin() as con:
            df = pd.read_sql(
                f"SELECT {table_name}.* FROM {table_name}",  # noqa: S608 - table names not supplied by user
                con=con,
            ).assign(sched_table_name=sched_table_name)
        return df.pipe(FercXbrlSQLiteIOManager.refine_report_year, xbrl_years=years)


class FercXbrlSQLiteConfigurableIOManager(_FercSQLiteConfigurableIOManagerBase):
    """Configurable IO manager for reading tables from a FERC XBRL SQLite database.

    Instantiate with ``dataset`` (``ferc1``, ``ferc714``, etc.).
    """

    data_format: Literal["xbrl"] = "xbrl"

    @cached_property
    def _manager(self) -> FercXbrlSQLiteIOManager:
        """Build the underlying SQLite reader lazily."""
        return FercXbrlSQLiteIOManager(
            base_dir=PudlPaths().output_dir,
            db_name=self.db_name,
        )


ferc1_dbf_sqlite_io_manager = FercDbfSQLiteConfigurableIOManager(
    etl_settings=etl_settings,
    zenodo_dois=zenodo_dois,
    dataset="ferc1",
)
ferc1_xbrl_sqlite_io_manager = FercXbrlSQLiteConfigurableIOManager(
    etl_settings=etl_settings,
    zenodo_dois=zenodo_dois,
    dataset="ferc1",
)
ferc714_xbrl_sqlite_io_manager = FercXbrlSQLiteConfigurableIOManager(
    etl_settings=etl_settings,
    zenodo_dois=zenodo_dois,
    dataset="ferc714",
)
