"""Basic script to generate a duckdb file with views to local/nightly parquet files."""

import sys

import click


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main():
    """Create duckdb file."""
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    import duckdb  # noqa: PLC0415
    from upath import UPath  # noqa: PLC0415

    from pudl.metadata.classes import PUDL_PACKAGE  # noqa: PLC0415
    from pudl.workspace.setup import PudlPaths  # noqa: PLC0415

    normal_tables = [r.name for r in PUDL_PACKAGE.resources if "ferceqr" not in r.name]
    partitioned_tables = [r.name for r in PUDL_PACKAGE.resources if "ferceqr" in r.name]
    schema_path_map = {
        "local": str(PudlPaths().parquet_path()),
        "nightly": "s3://pudl.catalyst.coop/nightly",
        "stable": "s3://pudl.catalyst.coop/stable",
    }
    with duckdb.connect(str(PudlPaths().duckdb_db_path("pudl"))) as conn:
        # Create local / nightly schema's
        [
            conn.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for schema in schema_path_map
        ]

    with duckdb.connect(str(PudlPaths().duckdb_db_path("pudl"))) as conn:
        # Create views to non-eqr tables
        [
            conn.sql(
                f"CREATE OR REPLACE VIEW {schema}.{table} AS "  # noqa: S608
                f"(SELECT * FROM '{base_path}/{table}.parquet')"  # noqa: S608
            )
            for table in normal_tables
            for schema, base_path in schema_path_map.items()
            # This is mostly for local files since all tables should exist in s3
            if UPath(f"{base_path}/{table}.parquet", anon=True).exists()
        ]

        # Create views to eqr tables. There's only one copy of EQR on s3 so these will
        # only appear in the nightly and local schemas
        [
            conn.sql(
                f"CREATE OR REPLACE VIEW {schema}.{table} AS "  # noqa: S608
                + f"(SELECT * FROM '{base_path}/{table}/*.parquet')".replace(  # noqa: S608
                    "nightly", "ferceqr"
                )
            )
            for table in partitioned_tables
            for schema, base_path in schema_path_map.items()
            if schema != "stable" and UPath(f"{base_path}/{table}/", anon=True).exists()
        ]


if __name__ == "__main__":
    sys.exit(main())
