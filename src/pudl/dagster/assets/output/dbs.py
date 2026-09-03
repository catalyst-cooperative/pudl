"""Dagster assets that rebuild pudl.sqlite and pudl.duckdb from PUDL's Parquet outputs."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import dagster as dg
import duckdb
import sqlalchemy as sa

import pudl.logging_helpers
from pudl.helpers import get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

# Alias that the destination pudl.sqlite database is attached under from DuckDB
# while its tables are being loaded.
_SQLITE_ATTACH_ALIAS = "pudl_sqlite_out"

# Exceptions treated as data-quality problems with one table, rather than a bug in
# this module: caught per table in write_pudl_sqlite()/write_pudl_duckdb() so one bad
# table doesn't abort the hundreds of others. ValueError comes from
# _validate_primary_key(); duckdb.Error covers both problems reading a table's
# Parquet file (e.g. a column that doesn't match the declared schema) and the
# NOT NULL/UNIQUE/PRIMARY KEY violations SQLite raises back through DuckDB as the
# rows are streamed in.
_SQLITE_WRITE_EXCEPTIONS = (ValueError, duckdb.Error)

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
        """Render as ``table_name: ExceptionType: message`` for logs and reports.

        Returns:
            A single-line string combining the table name, the exception class
            name, and the exception message.
        """
        return f"{self.table_name}: {type(self.exception).__name__}: {self.exception}"


@dataclass
class TableWriteReport:
    """Outcome of rebuilding a database: what wrote successfully, and what didn't.

    Tables are written independently of each other in :func:`write_pudl_sqlite`/
    :func:`write_pudl_duckdb`, so a data-quality problem in one doesn't stop the rest
    from being attempted. This report is how the caller finds out, after the fact,
    exactly which tables failed and why -- across potentially hundreds of tables in
    one run -- rather than only ever learning about the first failure encountered.

    """

    row_counts: dict[str, int] = field(default_factory=dict)
    """Mapping of table name to number of rows written, sucessly written table."""
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


################################################################################
# SQLite
################################################################################


def _has_integer_rowid_alias_pk(resource: Resource) -> bool:
    """Return whether a resource's primary key is susceptible to SQLite's ROWID alias.

    When a table's primary key is a *single* column declared with type ``INTEGER``,
    SQLite treats that column as an alias for its internal ``rowid`` rather than as
    an ordinary constrained column. This has a surprising consequence: inserting
    ``NULL`` into that column does not raise an error, even though the column also
    has an explicit ``NOT NULL`` constraint (every PUDL primary key column does --
    see ``Resource._check_primary_key_in_fields``). Instead, SQLite silently
    substitutes the next available rowid and the insert succeeds -- a bad row
    (``NULL`` where a primary key value was expected) is quietly laundered into a
    plausible-looking row instead of failing loudly.

    This only applies to a single-column ``integer``/``year`` primary key (``year``
    also maps to ``sa.BigInteger`` -- see ``FIELD_DTYPES_SQLITE``). Composite
    (multi-column) primary keys and non-integer single-column primary keys are not
    ROWID aliases, so SQLite's own ``NOT NULL``/``UNIQUE`` enforcement on those
    columns works exactly as expected and needs no help from PUDL.

    Args:
        resource: Metadata Resource for the table, whose ``schema.primary_key`` and
            field types are inspected.

    Returns:
        True if the resource has a single-column ``integer``/``year`` primary key
        (which SQLite treats as a ROWID alias); False for composite or non-integer
        primary keys.
    """
    pk = resource.schema.primary_key
    if len(pk) != 1:
        return False
    field = next(f for f in resource.schema.fields if f.name == pk[0])
    return field.type in ("integer", "year")


def _validate_primary_key(resource: Resource, paths: PudlPaths) -> None:
    """Check a table's Parquet data against its own primary key before writing.

    Only does anything when :func:`_has_integer_rowid_alias_pk` is True, since that
    is the only kind of primary key SQLite doesn't already enforce ``NOT NULL`` /
    ``UNIQUE`` on correctly. For all other cases SQLite's own constraint enforcement
    at insert time is sufficient, and this returns immediately without reading the
    Parquet file.

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


def _copy_table_to_sqlite(
    duckdb_con: duckdb.DuckDBPyConnection,
    resource: Resource,
    parquet_path: Path,
) -> int:
    """Stream one table's Parquet data into the attached SQLite database.

    DuckDB reads the Parquet file with its columnar engine and writes straight into
    the ``ATTACH``ed SQLite file via the ``sqlite`` extension. The destination SQLite
    schema is deliberately built *without* ``CHECK`` constraints (see
    :func:`write_pudl_sqlite`), which keeps the insertion fast; all data is assumed
    to have already been validated upstream against the full PUDL schema.
    ``PRIMARY KEY``, ``NOT NULL`` and ``UNIQUE`` are still declared *and* enforced by
    SQLite as the rows land; foreign keys are declared but, per SQLite's default
    (``PRAGMA foreign_keys = OFF``), not checked on write.

    Args:
        duckdb_con: An open DuckDB connection with the destination SQLite database
            already ``ATTACH``ed under :data:`_SQLITE_ATTACH_ALIAS`.
        resource: Metadata Resource for the table being written; supplies the table
            name and the ordered column list.
        parquet_path: Path to the source Parquet file for this table.

    Returns:
        The number of rows written to the table.

    Raises:
        duckdb.Error: If the Parquet file doesn't match the declared schema, or
            SQLite rejects a row for a ``NOT NULL`` / ``UNIQUE`` / ``PRIMARY KEY``
            violation as it lands.
    """
    # Table/column names come from PUDL_PACKAGE resource metadata, not external
    # input, so this isn't user-controlled SQL injection.
    table_name = resource.name
    columns_sql = ", ".join(f'"{c}"' for c in resource.get_field_names())
    duckdb_con.execute(
        f'INSERT INTO {_SQLITE_ATTACH_ALIAS}."{table_name}" '  # noqa: S608
        f"SELECT {columns_sql} FROM read_parquet(?)",
        [str(parquet_path)],
    )
    row = duckdb_con.execute(
        f'SELECT count(*) FROM {_SQLITE_ATTACH_ALIAS}."{table_name}"'  # noqa: S608
    ).fetchone()
    assert row is not None  # SELECT count(*) always returns exactly one row
    return row[0]


def write_pudl_sqlite(
    sqlite_path: Path,
    table_names: Sequence[str],
    paths: PudlPaths | None = None,
) -> TableWriteReport:
    """Build a fresh ``pudl.sqlite`` at ``sqlite_path`` from the given Parquet tables.

    The database is created with a "lean" schema -- column types, primary keys,
    ``NOT NULL``, ``UNIQUE``, foreign keys and column comments, but no ``CHECK``
    constraints (see the inline comment for why) -- and each table is then streamed
    in from Parquet by DuckDB via :func:`_copy_table_to_sqlite`.

    Tables are written independently: a failure in one table (see
    :data:`_SQLITE_WRITE_EXCEPTIONS`) is caught and recorded rather than aborting the
    run, so a single call surfaces every problem across all tables at once instead of
    stopping at the first one. Separated from the Dagster asset wrapper so it's
    directly unit-testable without constructing Dagster execution contexts.

    Args:
        sqlite_path: Where to write the SQLite file. Any existing file at this path
            is deleted first; parent directories are created as needed.
        table_names: Names of the tables to load. Every table must have a Resource
            with ``create_database_schema=True`` and a corresponding Parquet file.
        paths: PUDL workspace paths used to locate each table's Parquet file.
            Defaults to a fresh :class:`PudlPaths` built from the environment.

    Returns:
        A :class:`TableWriteReport` recording the row count for every table that
        wrote successfully and a :class:`TableWriteError` for every one that didn't.
    """
    paths = paths or PudlPaths()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite_path.unlink(missing_ok=True)

    metadata = PUDL_PACKAGE.to_sql(  # already filtered to create_database_schema
        dialect="sqlite",
        check_types=False,
        check_values=False,
    )
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    metadata.create_all(engine)
    engine.dispose()

    report = TableWriteReport()
    duckdb_con = duckdb.connect()
    duckdb_con.execute("LOAD sqlite")
    duckdb_con.execute("PRAGMA disable_progress_bar")
    # The path comes from PudlPaths, not external input; DuckDB has no parameter
    # binding for ATTACH targets.
    duckdb_con.execute(
        f"ATTACH '{sqlite_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)"  # noqa: S608
    )
    n_tables = len(table_names)
    try:
        for n, table_name in enumerate(table_names, start=1):
            logger.info(f"Writing SQLite {n}/{n_tables} {table_name}")
            resource = PUDL_PACKAGE.get_resource(table_name)
            try:
                _validate_primary_key(resource, paths)
                report.row_counts[table_name] = _copy_table_to_sqlite(
                    duckdb_con, resource, paths.parquet_path(table_name)
                )
            except _SQLITE_WRITE_EXCEPTIONS as exc:
                logger.error(f"Failed to write {table_name} to SQLite: {exc}")
                report.errors.append(TableWriteError(table_name, exc))
    finally:
        duckdb_con.execute(f"DETACH {_SQLITE_ATTACH_ALIAS}")
        duckdb_con.close()

    return report


def build_pudl_sqlite_asset(
    sqlite_asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Build the Dagster asset that assembles ``pudl.sqlite`` from Parquet outputs.

    Args:
        sqlite_asset_keys: Keys of the Parquet-writing assets whose tables should be
            included in ``pudl.sqlite`` (i.e. ``Resource.create_database_schema`` is
            True). Used both as the asset's dependencies and as the table list.

    Returns:
        A Dagster :class:`~dagster.AssetsDefinition` for the ``pudl_sqlite`` asset.
    """

    @dg.asset(
        name="pudl_sqlite",
        group_name="out_pudl",
        deps=list(sqlite_asset_keys),
        required_resource_keys={"pudl_paths"},
        description=(
            "SQLite database rebuilt from PUDL's Parquet outputs after the ETL "
            "completes. Written to $PUDL_OUTPUT/pudl.sqlite. Includes only tables "
            "whose Resource has create_database_schema=True. CHECK constraints "
            "are omitted for performance (data is already validated against the "
            "full schema upstream); foreign keys are declared but, per SQLite's "
            "default, not enforced on write."
        ),
    )
    def pudl_sqlite(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Materialize ``pudl.sqlite`` and record its size and table count."""
        pudl_paths: PudlPaths = context.resources.pudl_paths
        sqlite_path = pudl_paths.sqlite_db_path("pudl")
        table_names = [key.path[-1] for key in sqlite_asset_keys]
        report = write_pudl_sqlite(sqlite_path, table_names, paths=pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(sqlite_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(sqlite_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return pudl_sqlite


################################################################################
# DuckDB
################################################################################


def _copy_table_to_duckdb(
    conn: sa.Connection,
    resource: Resource,
    parquet_path: Path,
) -> int:
    """Load one table's Parquet data into an already-created, empty DuckDB table.

    Uses ``conn.exec_driver_sql()`` to
    send SQL straight to the duckdb-engine DBAPI cursor -- duckdb-engine's SQLAlchemy
    ``Connection`` wraps the native ``duckdb.DuckDBPyConnection`` in a way that
    doesn't cleanly expose it directly, so this is the most native-speed path
    available through the engine.

    Args:
        conn: An open SQLAlchemy connection to the destination DuckDB database.
        resource: Metadata Resource for the table being written; supplies the table
            name and the ordered column list.
        parquet_path: Path to the source Parquet file for this table.

    Returns:
        The number of rows written, as reported directly by DuckDB's ``INSERT``.

    Raises:
        sqlalchemy.exc.DBAPIError: If the Parquet file doesn't match the declared
            schema, or a row violates a constraint (``NOT NULL``, ``CHECK``, ...).
    """
    table_name = resource.name
    columns_sql = ", ".join(f'"{c}"' for c in resource.get_field_names())
    result = conn.exec_driver_sql(
        f'INSERT INTO "{table_name}" SELECT {columns_sql} '  # noqa: S608
        "FROM read_parquet(?)",
        (str(parquet_path),),
    )
    row = result.fetchone()
    assert row is not None  # INSERT always returns exactly one "Count" row
    return row[0]


def write_pudl_duckdb(
    duckdb_path: Path,
    table_names: Sequence[str],
    paths: PudlPaths | None = None,
) -> TableWriteReport:
    """Build a fresh ``pudl.duckdb`` from the given Parquet tables, foreign-key-free.

    Schema creation goes through a real SQLAlchemy engine (``metadata.create_all()``).
    This allows ``ENUM`` constraints to be shared across many tables even if the
    constrained fields have the same name. Foreign key constraints are intentionally
    excluded; primary keys and ``NOT NULL`` are kept, and natively enforced by DuckDB.
    CHECK constraints are retained as annotations for downstream users; enforcement is
    cheap in DuckDB. Per-table failures are caught and collected rather than aborting
    the run, same as :func:`write_pudl_sqlite`.

    Args:
        duckdb_path: Where to write the DuckDB file. Any existing file at this path
            is deleted first; parent directories are created as needed.
        table_names: Names of the tables to load. Every table must have a Resource
            with ``create_database_schema=True`` and a corresponding Parquet file.
        paths: PUDL workspace paths used to locate each table's Parquet file.
            Defaults to a fresh :class:`PudlPaths` built from the environment.

    Returns:
        A :class:`TableWriteReport` recording the row count for every table that
        wrote successfully and a :class:`TableWriteError` for every one that didn't.
    """
    paths = paths or PudlPaths()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.unlink(missing_ok=True)

    metadata = PUDL_PACKAGE.to_sql(dialect="duckdb", include_foreign_keys=False)
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    metadata.create_all(engine)

    report = TableWriteReport()
    n_tables = len(table_names)
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA disable_progress_bar")
            for n, table_name in enumerate(table_names, start=1):
                logger.info(f"Writing DuckDB {n}/{n_tables} {table_name}")
                resource = PUDL_PACKAGE.get_resource(table_name)
                try:
                    report.row_counts[table_name] = _copy_table_to_duckdb(
                        conn, resource, paths.parquet_path(table_name)
                    )
                    conn.commit()
                except sa.exc.DBAPIError as exc:
                    conn.rollback()
                    logger.error(f"Failed to write {table_name} to DuckDB: {exc}")
                    report.errors.append(TableWriteError(table_name, exc))
    finally:
        engine.dispose()

    return report


def build_pudl_duckdb_asset(
    duckdb_asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Build the Dagster asset that rebuilds ``pudl.duckdb`` from Parquet outputs.

    Args:
        duckdb_asset_keys: Keys of the Parquet-writing assets whose tables should be
            included in ``pudl.duckdb`` (i.e. ``Resource.create_database_schema`` is
            True). Used both as the asset's dependencies and as the table list.

    Returns:
        A Dagster :class:`~dagster.AssetsDefinition` for the ``pudl_duckdb`` asset.
    """

    @dg.asset(
        name="pudl_duckdb",
        group_name="out_pudl",
        deps=list(duckdb_asset_keys),
        required_resource_keys={"pudl_paths"},
        description=(
            "DuckDB database rebuilt from PUDL's Parquet outputs after the ETL "
            "completes. Written to $PUDL_OUTPUT/pudl.duckdb. Includes only tables "
            "whose Resource has create_database_schema=True. Foreign key "
            "constraints are excluded for performance."
        ),
    )
    def pudl_duckdb(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Materialize ``pudl.duckdb`` and record its size and table count."""
        pudl_paths: PudlPaths = context.resources.pudl_paths
        duckdb_path = pudl_paths.duckdb_db_path("pudl")
        table_names = [key.path[-1] for key in duckdb_asset_keys]
        report = write_pudl_duckdb(duckdb_path, table_names, paths=pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(duckdb_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(duckdb_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return pudl_duckdb


__all__ = [
    "TableWriteError",
    "TableWriteReport",
    "build_pudl_duckdb_asset",
    "build_pudl_sqlite_asset",
    "write_pudl_duckdb",
    "write_pudl_sqlite",
]
