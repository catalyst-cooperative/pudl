"""Dagster IO managers used by PUDL assets.

This module defines the IO-manager implementations that translate between Dagster asset
execution and PUDL's storage formats, including Parquet (with native GeoParquet support
for assets that return a :class:`geopandas.GeoDataFrame`) and the FERC prerequisite
SQLite databases. Put :class:`dagster.IOManager` and :class:`dagster.ConfigurableIOManager`
classes here, along with configured singleton instances that the default code location
reuses. Keep data-processing logic out of this module; it should focus on persistence,
loading, and storage-compatibility concerns.

For the underlying Dagster concept, see https://docs.dagster.io/guides/build/io-managers
"""

import hashlib
import re
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
import geopandas
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import sqlalchemy as sa
from dagster import DagsterInvariantViolationError, InputContext, OutputContext
from pydantic import PrivateAttr

import pudl.logging_helpers
from pudl.dagster.provenance import (
    FercSqliteProvenance,
    FercSqliteProvenanceRecord,
    ferc_sqlite_provenance_is_compatible,
    get_xbrl_extractor_version,
)
from pudl.dagster.resources import (
    GlobalDataConfigResource,
    PudlPathsResource,
    ZenodoDoiSettingsResource,
    global_data_config_resource,
    pudl_paths_resource,
    zenodo_doi_settings_resource,
)
from pudl.helpers import get_parquet_table, get_parquet_table_polars
from pudl.metadata.classes import Resource

logger = pudl.logging_helpers.get_logger(__name__)


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


class PudlParquetIOManager(dg.ConfigurableIOManager):
    """IOManager that writes pudl tables to pyarrow parquet files."""

    pudl_paths: dg.ResourceDependency[PudlPathsResource]

    @staticmethod
    def _record_parquet_file_metadata(
        context: dg.OutputContext, parquet_path: Path
    ) -> None:
        """Attach file size and SHA-256 hash to the Dagster output metadata.

        This metadata is later retrieved by the ``pudl_datapackage`` asset to
        populate the frictionless datapackage descriptor without re-reading the
        parquet files.
        """
        parquet_meta = pq.read_metadata(parquet_path)
        file_bytes = parquet_path.stat().st_size
        sha256 = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
        context.add_output_metadata(
            {
                "dagster/row_count": dg.MetadataValue.int(parquet_meta.num_rows),
                "dagster/column_count": dg.MetadataValue.int(parquet_meta.num_columns),
                "dagster/uri": dg.MetadataValue.path(str(parquet_path)),
                "bytes": dg.MetadataValue.int(file_bytes),
                "sha256": dg.MetadataValue.text(sha256),
            }
        )

    def handle_output(
        self,
        context: dg.OutputContext,
        obj: pd.DataFrame | geopandas.GeoDataFrame | pl.LazyFrame,
    ) -> None:
        """Writes a pudl dataframe to a Parquet file.

        GeoDataFrames are written as GeoParquet using native geopandas output,
        which produces spec-compliant CRS metadata readable by DuckDB >= 1.5.
        Regular DataFrames and Polars LazyFrames use the PUDL PyArrow schema to
        enforce exact column types on disk.
        """
        table_name = get_table_name_from_context(context)
        res = Resource.from_id(table_name)
        parquet_path = self.pudl_paths.parquet_path(table_name)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(obj, geopandas.GeoDataFrame):
            gdf = res.enforce_schema(obj)
            gdf.to_parquet(parquet_path, index=False)
        elif isinstance(obj, pd.DataFrame):
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
                "PudlParquetIOManager only supports pandas DataFrames, "
                f"geopandas GeoDataFrames, and Polars LazyFrames, got {type(obj)}."
            )
        self._record_parquet_file_metadata(context, parquet_path)

    def load_input(
        self, context: dg.InputContext
    ) -> pd.DataFrame | geopandas.GeoDataFrame | pl.LazyFrame:
        """Loads pudl table from parquet file."""
        table_name = get_table_name_from_context(context)
        if context.dagster_type.typing_type == pl.LazyFrame:
            df = get_parquet_table_polars(table_name, paths=self.pudl_paths)
        else:
            df = get_parquet_table(table_name, paths=self.pudl_paths)
        return df


parquet_io_manager = PudlParquetIOManager(pudl_paths=pudl_paths_resource)


class FercSqliteIOManagerBase(dg.ConfigurableIOManager):
    """Shared lazy-loading behavior for FERC SQLite Dagster IO managers.

    Subclasses provide the query details for a particular FERC SQLite backend, while
    this base class owns three shared responsibilities:

    1. lazily creating and caching a SQLAlchemy engine for the configured database
    2. lazily reflecting and caching SQLAlchemy metadata once the database exists
    3. checking Dagster provenance metadata before each read
    """

    global_data_config: dg.ResourceDependency[GlobalDataConfigResource]
    pudl_paths: dg.ResourceDependency[PudlPathsResource]
    zenodo_dois: dg.ResourceDependency[ZenodoDoiSettingsResource]
    dataset: str
    data_format: ClassVar[str]

    _engine: sa.Engine | None = PrivateAttr(default=None)
    _metadata: sa.MetaData | None = PrivateAttr(default=None)

    @property
    def _years_key(self) -> str:
        return f"{self.data_format}_years"

    @property
    def db_name(self) -> str:
        """Return the SQLite database name for this dataset and data format."""
        return f"{self.dataset}_{self.data_format}"

    @property
    def db_path(self) -> Path:
        """Return the canonical SQLite path for this dataset and data format."""
        return self.pudl_paths.sqlite_db_path(self.db_name)

    @property
    def engine(self) -> sa.Engine:
        """Return a cached SQLAlchemy engine for this FERC SQLite database."""
        if self._engine is None:
            self._engine = sa.create_engine(f"sqlite:///{self.db_path}")
        return self._engine

    @property
    def metadata(self) -> sa.MetaData:
        """Return cached reflected metadata for this database.

        The metadata is reflected on first access and reused for subsequent reads.
        Accessing this property requires the SQLite database to already exist.
        """
        if not self.db_path.exists():
            raise ValueError(
                f"No DB found at {self.db_path}. Run the job that creates the "
                f"{self.db_name} database."
            )

        if self._metadata is None:
            metadata = sa.MetaData()
            metadata.reflect(self.engine)
            self._metadata = metadata

        return self._metadata

    def _get_sqlalchemy_table(self, table_name: str) -> sa.Table:
        """Return reflected SQLAlchemy table metadata for a FERC SQLite table."""
        sa_table = self.metadata.tables.get(table_name, None)
        if sa_table is None:
            raise ValueError(
                f"{table_name} not found in {self.db_name} metadata. Either add the "
                "table to the metadata or use a different IO Manager. Database is "
                f"located at {self.db_path}."
            )
        return sa_table

    def _check_provenance(self, context: InputContext) -> None:
        """Check that the existing FERC SQLite database is compatible with this run.

        This is intentionally separate from engine and metadata caching because the
        compatibility check depends on the Dagster run context rather than on local
        process state.
        """
        zenodo_doi = self.zenodo_dois.get_doi(self.dataset)

        provenance = FercSqliteProvenance(
            dataset=self.dataset,
            data_format=self.data_format,
            zenodo_doi=zenodo_doi,
            years=self.global_data_config.ferc_to_sqlite.get_dataset_years(
                self.dataset, self.data_format
            ),
            ferc_xbrl_extractor_version=get_xbrl_extractor_version(),
        )

        if ((instance := _get_dagster_instance_if_available(context)) is not None) and (
            not ferc_sqlite_provenance_is_compatible(
                observed_provenance=FercSqliteProvenanceRecord.from_dagster_instance(
                    instance=instance,
                    dataset=self.dataset,
                    data_format=self.data_format,
                ),
                required_provenance=provenance,
            )
        ):
            raise RuntimeError(
                f"{self.dataset}_{self.data_format} provenace metadata is not compatible"
                " with requirements of current run. Refresh the FERC SQLite assets."
            )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from the configured FERC SQLite database.

        Ensure that the database exists and its schema has been reflected, then verify
        the upstream FERC-to-SQLite provenance recorded in Dagster before delegating to
        the subclass-specific query implementation.
        """
        _ = self.metadata
        self._check_provenance(context)
        ferc_data_config = getattr(
            self.global_data_config.pudl,
            self.dataset,
        )
        table_name = get_table_name_from_context(context).replace(
            f"raw_{self.db_name}__", ""
        )
        return self._query(table_name, getattr(ferc_data_config, self._years_key))

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame | str) -> None:
        """Reject writes because these IO managers currently support reads only."""
        raise NotImplementedError("Ferc SQLite IO managers can't write outputs yet.")

    def _query(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Execute a filtered read against the FERC SQLite database."""
        raise NotImplementedError("Subclasses must implement _query.")


class FercDbfSqliteIOManager(FercSqliteIOManagerBase):
    """IO manager for reading tables from FERC DBF SQLite databases.

    Instantiate with ``dataset`` (``ferc1``, ``ferc714``, etc.)
    """

    data_format: ClassVar[str] = "dbf"

    def _query(self, table_name: str, years: list[int]) -> pd.DataFrame:
        """Execute the year-filtered read against the FERC DBF SQLite database."""
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


class FercXbrlSqliteIOManager(FercSqliteIOManagerBase):
    """IO manager for reading tables from a FERC XBRL SQLite database.

    Instantiate with ``dataset`` (``ferc1``, ``ferc714``, etc.).
    """

    data_format: ClassVar[str] = "xbrl"

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
        df = df.assign(report_year=new_report_years)
        df = df.loc[df.report_year.isin(xbrl_years_plus_one_previous)]
        return df.reset_index(drop=True)

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
        if table_name not in self.metadata.tables:
            return pd.DataFrame()
        sched_table_name = re.sub("_instant|_duration", "", table_name)
        with self.engine.begin() as con:
            df = pd.read_sql(
                f"SELECT {table_name}.* FROM {table_name}",  # noqa: S608 - table names not supplied by user
                con=con,
            ).assign(sched_table_name=sched_table_name)
        return df.pipe(self.refine_report_year, xbrl_years=years)


ferc1_dbf_sqlite_io_manager = FercDbfSqliteIOManager(
    global_data_config=global_data_config_resource,
    pudl_paths=pudl_paths_resource,
    zenodo_dois=zenodo_doi_settings_resource,
    dataset="ferc1",
)
ferc1_xbrl_sqlite_io_manager = FercXbrlSqliteIOManager(
    global_data_config=global_data_config_resource,
    pudl_paths=pudl_paths_resource,
    zenodo_dois=zenodo_doi_settings_resource,
    dataset="ferc1",
)
ferc714_xbrl_sqlite_io_manager = FercXbrlSqliteIOManager(
    global_data_config=global_data_config_resource,
    pudl_paths=pudl_paths_resource,
    zenodo_dois=zenodo_doi_settings_resource,
    dataset="ferc714",
)

default_io_managers: dict[str, Any] = {
    "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
    "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
    "ferc714_xbrl_sqlite_io_manager": ferc714_xbrl_sqlite_io_manager,
    "parquet_io_manager": parquet_io_manager,
    # PUDL assets historically wrote to both SQLite and Parquet via the
    # "pudl_io_manager" key. The ETL now writes only Parquet, so this key is
    # kept as an alias for the plain Parquet IO manager to avoid churning the
    # io_manager_key on every asset.
    "pudl_io_manager": parquet_io_manager,
}
