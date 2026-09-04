"""Dagster assets that assembles pudl.sqlite and pudl.duckdb from PUDL's Parquet outputs.

Both databases are assembled by one function, :func:`_write_pudl_db` which creates the
empty schema through a throwaway SQLAlchemy engine. Each table's Parquet file is read
with DuckDB and a DuckDB connection is used to insert it into the datase. Everything
that differs between the two databases is cued by :class:`_DatabaseTarget`'s ``db_type``
field (``"sqlite"`` or ``"duckdb"``), which every other type-dependent value -- the
``PUDL_PACKAGE.to_sql()`` call that shapes the schema, the SQLAlchemy engine URL,
whether rows land in the DuckDB file or an ``ATTACH``ed SQLite file, whether the
pre-write primary-key check runs -- is derived from, so the two can never end up
specified inconsistently.

:data:`SQLITE_TARGET` and :data:`DUCKDB_TARGET` are the two instances;
:func:`build_pudl_db_asset` wraps either one in a Dagster asset.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import dagster as dg
import duckdb
import sqlalchemy as sa

import pudl.logging_helpers
from pudl.helpers import get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

_SQLITE_ATTACH_ALIAS = "pudl_sqlite_out"
"""Alias that the destination pudl.sqlite database is attached to through DuckDB."""

_WRITE_EXCEPTIONS: tuple[type[Exception], ...] = (ValueError, duckdb.Error)
"""Exceptions treated as data-quality problems with an individual table.

These exceptions are caught per table in _write_pudl_db() so one bad table doesn't abort
hundreds of others. ValueError comes from _validate_primary_key(); duckdb.Error
covers both problems reading a table's Parquet file (e.g. a column that doesn't match
the declared schema) and the NOT NULL / UNIQUE / PRIMARY KEY / CHECK violations the
destination raises back through DuckDB as the rows are streamed in.
"""

_DUCKDB_MAX_IDENTIFIER_LENGTH = 255
"""Explicit maximum length for DuckDB identifiers.

Required because duckdb-engine's SQLAlchemy dialect subclasses postgresql's, inheriting a
63-character identifier length limit that DuckDB itself doesn't actually have.
"""


@dataclass
class TableWriteError:
    """A single table's failed write: which table, and the exception that stopped it."""

    table_name: str
    """Name of the table whose write failed."""
    exception: Exception
    """The exception raised while validating or writing the table."""

    def __str__(self) -> str:
        """Render as ``table_name: ExceptionType: message`` for logs and reports."""
        return f"{self.table_name}: {type(self.exception).__name__}: {self.exception}"


@dataclass
class TableWriteReport:
    """Outcome of building a database: what wrote successfully, and what didn't."""

    row_counts: dict[str, int] = field(default_factory=dict)
    """Mapping of table name to number of rows written, per successfully written table."""
    errors: list[TableWriteError] = field(default_factory=list)
    """One :class:`TableWriteError` per failed table, in the order they failed."""

    @property
    def failed_tables(self) -> list[str]:
        """Names of the tables that failed to write.

        Returns:
            The ``table_name`` of every recorded error, in failure order.
        """
        return [error.table_name for error in self.errors]

    def summary(self) -> str:
        """Human-readable report of every table that failed to write, and why.

        Returns:
            A multi-line string: a single ``"Wrote all N table(s)."`` line when
            nothing failed, otherwise a header line plus one indented line per
            failed table.
        """
        total = len(self.row_counts) + len(self.errors)
        if not self.errors:
            return f"Wrote all {total} table(s)."
        lines = [f"Wrote {len(self.row_counts)}/{total} table(s)."]
        lines.append(f"{len(self.errors)} table(s) failed:")
        lines.extend(f"  - {error}" for error in self.errors)
        return "\n".join(lines)


def _has_integer_rowid_alias_pk(resource: Resource) -> bool:
    """Return whether a resource's primary key is susceptible to SQLite's ROWID alias.

    When a table's primary key is a *single* column declared with type ``INTEGER``,
    SQLite treats that column as an alias for its internal ``rowid`` rather than as an
    ordinary constrained column. This has a surprising consequence: inserting ``NULL``
    into that column does not raise an error, even though the column also has an
    explicit ``NOT NULL`` constraint (every PUDL primary key column does -- see
    ``Resource._check_primary_key_in_fields``). Instead, SQLite silently substitutes the
    next available rowid and the insert succeeds -- a bad row (``NULL`` where a primary
    key value was expected) is quietly laundered into a plausible-looking row instead of
    failing loudly.

    This only applies to a single-column ``integer``/``year`` primary key (``year`` also
    maps to ``sa.BigInteger`` -- see ``FIELD_DTYPES_SQLITE``). Composite (multi-column)
    primary keys and non-integer single-column primary keys are not ROWID aliases, so
    SQLite's own ``NOT NULL``/``UNIQUE`` enforcement on those columns works exactly as
    expected.

    Args:
        resource: Metadata Resource for the table, whose ``schema.primary_key`` and
            field types are inspected.

    Returns:
        True if the resource has a single-column ``integer``/``year`` primary key.
        False for composite or non-integer primary keys.
    """
    pk = resource.schema.primary_key
    if len(pk) != 1:
        return False
    field = next(f for f in resource.schema.fields if f.name == pk[0])
    return field.type in ("integer", "year")


def _validate_primary_key(resource: Resource, paths: PudlPaths) -> None:
    """Check a table's Parquet data against its own primary key before writing.

    Only does anything when :func:`_has_integer_rowid_alias_pk` is True, since that is
    the only kind of primary key SQLite doesn't already enforce ``NOT NULL`` /
    ``UNIQUE`` on correctly. For all other cases SQLite's own constraint enforcement at
    insert time is sufficient, and this returns immediately without reading the Parquet
    file.

    Args:
        resource: Metadata Resource for the table to check.
        paths: PUDL workspace paths, used to locate the table's Parquet file.

    Raises:
        ValueError: If the Parquet data contains a null or duplicate primary key
            value. The message names the resource and lists each violation.
    """
    if not _has_integer_rowid_alias_pk(resource):
        return
    lazy_df = get_parquet_table_polars(resource.name, paths=paths)
    if errors := resource.check_primary_key(lazy_df):
        raise ValueError(
            f"{resource.name}: " + "\n".join(str(error) for error in errors)
        )


def _copy_table(
    conn: duckdb.DuckDBPyConnection,
    resource: Resource,
    parquet_path: Path,
    *,
    table_ref: str,
) -> int:
    """Stream one table's Parquet data into ``table_ref`` via DuckDB.

    DuckDB reads the Parquet file with its columnar engine and writes the rows straight
    into ``table_ref`` -- either a table in the DuckDB file itself, or a table in an
    ``ATTACH``ed SQLite database (see :func:`_write_pudl_db`). Column order comes from
    the resource metadata so the ``SELECT`` lines up with the destination schema
    regardless of the Parquet file's column order.

    ``PRIMARY KEY`` / ``NOT NULL`` / ``UNIQUE`` (and, for DuckDB, ``CHECK``) are
    enforced by the destination as the rows land. SQLite foreign keys are declared
    but, per SQLite's default (``PRAGMA foreign_keys = OFF``), not checked on write;
    the DuckDB schema has no foreign keys at all.

    Args:
        conn: An open DuckDB connection. For the SQLite target the destination
            database must already be ``ATTACH``ed under :data:`_SQLITE_ATTACH_ALIAS`.
        resource: Metadata Resource for the table being written; supplies the
            ordered column list.
        parquet_path: Path to the source Parquet file for this table.
        table_ref: Quoted SQL reference to the destination table.

    Returns:
        The number of rows in the destination table after the insert (which, for a
        freshly created table, is the number of rows written).

    Raises:
        duckdb.Error: If the Parquet file doesn't match the declared schema, or the
            destination rejects a row for a ``NOT NULL`` / ``UNIQUE`` / ``PRIMARY
            KEY`` / ``CHECK`` violation as it lands.
    """
    columns_sql = ", ".join(f'"{c}"' for c in resource.get_field_names())
    conn.execute(
        f"INSERT INTO {table_ref} "  # noqa: S608
        f"SELECT {columns_sql} FROM read_parquet(?)",
        [str(parquet_path)],
    )
    row = conn.execute(f"SELECT count(*) FROM {table_ref}").fetchone()  # noqa: S608
    assert row is not None  # SELECT count(*) always returns exactly one row
    return row[0]


@dataclass(frozen=True)
class _DatabaseTarget:
    """Everything needed to build one database, cued by which one it is.

    ``db_type`` is the only field that distinguishes SQLite from DuckDB; every other
    type-dependent value below is derived from it (as a property or a method).
    """

    db_type: Literal["sqlite", "duckdb"]
    """Which database this target builds."""

    description: str
    """Dagster asset description."""

    @property
    def asset_name(self) -> str:
        """Name of the Dagster asset that materializes this database."""
        return f"pudl_{self.db_type}"

    @property
    def attach_as_sqlite(self) -> bool:
        """Whether rows are written to ``ATTACH``ed SQLite DB rather than DuckDB."""
        return self.db_type == "sqlite"

    @property
    def check_primary_keys(self) -> bool:
        """Whether to run :func:`_validate_primary_key` before writing each table.

        Only SQLite needs this -- see that function for the ROWID-alias quirk it
        guards against. DuckDB enforces primary keys as expected.
        """
        return self.db_type == "sqlite"

    @property
    def max_identifier_length(self) -> int | None:
        """Override for the SQLAlchemy engine's identifier-length limit.

        Needed for DuckDB, whose SQLAlchemy dialect inherits Postgres' 63-character
        limit that DuckDB itself doesn't have. ``None`` leaves the dialect default
        alone (SQLite).
        """
        if self.db_type == "duckdb":
            return _DUCKDB_MAX_IDENTIFIER_LENGTH
        return None

    def engine_url(self, db_path: Path) -> str:
        """SQLAlchemy URL for the throwaway engine that creates the schema at ``db_path``."""
        return f"{self.db_type}:///{db_path}"

    def db_path(self, paths: PudlPaths) -> Path:
        """Where the database file lives, given the workspace paths."""
        if self.db_type == "sqlite":
            return paths.sqlite_db_path("pudl")
        return paths.duckdb_db_path("pudl")

    def build_metadata(self) -> sa.MetaData:
        """Build the empty-schema ``MetaData`` for this database.

        Reads ``PUDL_PACKAGE`` at call time, rather than module import time, which
        keeps it patchable in tests.
        """
        if self.db_type == "sqlite":
            # Filtered to create_database_schema=True resources; CHECK constraints
            # are dropped for insert speed (data is already validated upstream).
            return PUDL_PACKAGE.to_sql(
                dialect="sqlite", check_types=False, check_values=False
            )
        return PUDL_PACKAGE.to_sql(dialect="duckdb", include_foreign_keys=False)

    def table_ref(self, table_name: str) -> str:
        """Quoted SQL reference to ``table_name`` in the destination database."""
        if self.attach_as_sqlite:
            return f'{_SQLITE_ATTACH_ALIAS}."{table_name}"'
        return f'"{table_name}"'


SQLITE_TARGET = _DatabaseTarget(
    db_type="sqlite",
    description=(
        "SQLite database assembled from PUDL's Parquet outputs after the ETL "
        "completes. Written to $PUDL_OUTPUT/pudl.sqlite. Includes only tables "
        "whose Resource has create_database_schema=True. CHECK constraints "
        "are omitted for performance (data is already validated against the "
        "full schema upstream); foreign keys are declared but, per SQLite's "
        "default, not enforced on write."
    ),
)

DUCKDB_TARGET = _DatabaseTarget(
    db_type="duckdb",
    description=(
        "DuckDB database rebuilt from PUDL's Parquet outputs after the ETL "
        "completes. Written to $PUDL_OUTPUT/pudl.duckdb. Includes only tables "
        "whose Resource has create_database_schema=True. Foreign key "
        "constraints are excluded for performance."
    ),
)


def _write_pudl_db(
    target: _DatabaseTarget,
    db_path: Path,
    table_names: Sequence[str],
    paths: PudlPaths | None = None,
) -> TableWriteReport:
    """Build a fresh database at ``db_path`` and stream the given tables into it.

    Creates the empty schema through a throwaway SQLAlchemy engine (so a named
    ``ENUM`` shared by several tables is created once, and comments/constraints are
    emitted consistently), then has a DuckDB connection read each table's Parquet
    file and insert it -- into the DuckDB file directly, or into an ``ATTACH``ed
    SQLite file, per ``target``.

    Tables are written independently: a failure in one (see :data:`_WRITE_EXCEPTIONS`)
    is caught and recorded rather than aborting the run, so a single call surfaces
    every problem across all tables at once. Separated from the Dagster asset wrapper
    so it's directly unit-testable without a Dagster execution context.

    Args:
        target: Which database to build (:data:`SQLITE_TARGET` or
            :data:`DUCKDB_TARGET`).
        db_path: Where to write the file. Any existing file there is deleted first;
            parent directories are created as needed.
        table_names: Names of the tables to load. Every table must have a Resource
            with ``create_database_schema=True`` and a corresponding Parquet file.
        paths: PUDL workspace paths used to locate each table's Parquet file.
            Defaults to a fresh :class:`PudlPaths` built from the environment.

    Returns:
        A :class:`TableWriteReport` recording the row count for every table that
        wrote successfully and a :class:`TableWriteError` for every one that didn't.
    """
    paths = paths or PudlPaths()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.unlink(missing_ok=True)

    engine = sa.create_engine(target.engine_url(db_path))
    if target.max_identifier_length is not None:
        engine.dialect.max_identifier_length = target.max_identifier_length
    target.build_metadata().create_all(engine)
    engine.dispose()

    # Every insert runs through DuckDB. For DuckDB the connection is the database
    # file itself; for SQLite we open an in-memory DuckDB and ATTACH the file (whose
    # path comes from PudlPaths, not user input -- DuckDB has no parameter binding
    # for ATTACH targets).
    conn = duckdb.connect() if target.attach_as_sqlite else duckdb.connect(str(db_path))
    conn.execute("PRAGMA disable_progress_bar")
    if target.attach_as_sqlite:
        conn.execute("LOAD sqlite")
        conn.execute(
            f"ATTACH '{db_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)"  # noqa: S608
        )

    report = TableWriteReport()
    n_tables = len(table_names)
    try:
        for n, table_name in enumerate(table_names, start=1):
            logger.info(f"Writing {target.db_type} {n}/{n_tables} {table_name}")
            resource = PUDL_PACKAGE.get_resource(table_name)
            try:
                if target.check_primary_keys:
                    _validate_primary_key(resource, paths)
                report.row_counts[table_name] = _copy_table(
                    conn,
                    resource,
                    paths.parquet_path(table_name),
                    table_ref=target.table_ref(table_name),
                )
            except _WRITE_EXCEPTIONS as exc:
                logger.error(f"Failed to write {table_name} to {target.db_type}: {exc}")
                report.errors.append(TableWriteError(table_name, exc))
    finally:
        if target.attach_as_sqlite:
            conn.execute(f"DETACH {_SQLITE_ATTACH_ALIAS}")
        conn.close()
    return report


################################################################################
# Dagster asset factory
################################################################################


def build_pudl_db_asset(
    target: _DatabaseTarget,
    asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Build the Dagster asset that assembles one database from Parquet outputs.

    Args:
        target: Which database to build (:data:`SQLITE_TARGET` or
            :data:`DUCKDB_TARGET`).
        asset_keys: Keys of the Parquet-writing assets whose tables should be
            included (i.e. ``Resource.create_database_schema`` is True). Used both as
            the asset's dependencies and as the table list.

    Returns:
        A Dagster :class:`~dagster.AssetsDefinition` for the ``target``'s asset.
    """

    @dg.asset(
        name=target.asset_name,
        group_name="out_pudl",
        deps=list(asset_keys),
        required_resource_keys={"pudl_paths"},
        description=target.description,
    )
    def _pudl_db(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Materialize the database and record its size and table count."""
        pudl_paths: PudlPaths = context.resources.pudl_paths
        db_path = target.db_path(pudl_paths)
        table_names = [key.path[-1] for key in asset_keys]
        report = _write_pudl_db(target, db_path, table_names, paths=pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(db_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(db_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return _pudl_db


__all__ = [
    "DUCKDB_TARGET",
    "SQLITE_TARGET",
    "TableWriteError",
    "TableWriteReport",
    "build_pudl_db_asset",
]
