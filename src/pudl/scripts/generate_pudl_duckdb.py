"""Generate a DuckDB database of views onto Parquet files for each datapackage.

Each registered datapackage gets its own standalone database, named
``<package>_views.duckdb`` (e.g. ``pudl_views.duckdb``). The ``_views`` suffix
distinguishes these from databases that hold the data itself, like ``pudl.duckdb``. Every
table is exposed as a view reading the Parquet files directly, in a schema named for
where the files live (``local``, ``nightly``, ``stable``, ``published``). Nothing is
copied.
"""

import sys
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    import duckdb

    from pudl.metadata.classes import Package, Resource

REMOTE_BASE_PATHS: dict[str, dict[str, str]] = {
    "pudl": {
        "nightly": "s3://pudl.catalyst.coop/nightly",
        "stable": "s3://pudl.catalyst.coop/stable",
    },
    # There is only one published copy of FERC EQR; we don't keep multiple versions.
    "ferceqr": {"published": "s3://pudl.catalyst.coop/ferceqr"},
}
"""Remote locations of each package's Parquet files, keyed by package then schema.

A ``local`` schema pointing at ``$PUDL_OUTPUT/parquet`` is always added. A package with
no entry here only gets the ``local`` schema.
"""


def _view_location(resource: Resource, base_path: str) -> tuple[str, str]:
    """Return what a resource's view should read, and the path to check for existence.

    The layout comes from the resource's own declared ``path``, not from the package it
    belongs to. A path with a directory component is a directory of partition files
    and is read with a glob; a bare file name is a single Parquet file.

    Returns:
        The file or glob to select from, and the file or directory that must exist.
    """
    path = PurePosixPath(resource.path)
    if path.parent == PurePosixPath():
        location = f"{base_path}/{path}"
        return location, location
    directory = f"{base_path}/{path.parent}"
    return f"{directory}/*.parquet", f"{directory}/"


def create_views(
    conn: duckdb.DuckDBPyConnection,
    package: Package,
    base_paths: Mapping[str, str],
) -> list[str]:
    """Create a view for each of a package's resources in each of its schemas.

    Args:
        conn: Connection to the database to create the views in.
        package: Datapackage whose resources should be exposed as views.
        base_paths: Where the package's Parquet files live, keyed by schema name.

    Returns:
        The ``schema.table`` name of every view created. A resource whose files don't
        exist at a location is skipped there, since local outputs are often incomplete.
    """
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    from upath import UPath  # noqa: PLC0415

    created: list[str] = []
    for schema, base_path in base_paths.items():
        conn.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for resource in package.resources:
            location, must_exist = _view_location(resource, base_path)
            if not UPath(must_exist, anon=True).exists():
                continue
            conn.sql(
                f"CREATE OR REPLACE VIEW {schema}.{resource.name} AS "  # noqa: S608
                f"(SELECT * FROM '{location}')"
            )
            created.append(f"{schema}.{resource.name}")
    return created


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main() -> int:
    """Create a DuckDB database of views onto Parquet files for each datapackage."""
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    import duckdb  # noqa: PLC0415

    from pudl.metadata.classes import DATAPACKAGES  # noqa: PLC0415
    from pudl.workspace.setup import PudlPaths  # noqa: PLC0415

    paths = PudlPaths()
    for name, package in DATAPACKAGES.items():
        base_paths = {
            "local": str(paths.parquet_path()),
            **REMOTE_BASE_PATHS.get(name, {}),
        }
        with duckdb.connect(str(paths.duckdb_path(f"{name}_views"))) as conn:
            created = create_views(conn, package, base_paths)
        click.echo(f"{name}_views.duckdb: created {len(created)} views.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
