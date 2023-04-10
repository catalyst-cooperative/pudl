"""Reset the PUDL output DB to match the schema in pudl.metadata."""
import argparse
import logging
import pathlib

import sqlalchemy as sa

from pudl.metadata.classes import Package
from pudl.workspace.setup import get_defaults

logger = logging.getLogger(f"catalystcoop.{__name__}")


def _parse_args():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument(
        "--sqlite-path", help="The path to the PUDL DB that you'd like to reset."
    )
    return parser.parse_args()


def reset_db(
    sqlite_path: str | None = None, metadata: sa.MetaData | None = None
) -> None:
    """Ensure empty SQLite DB at `sqlite_path` with schema `metadata`.

    Args:
        sqlite_path: the path to DB. Defaults to $PUDL_OUTPUT/pudl.sqlite
        metadata: SQLAlchemy MetaData describing desired DB state. Defaults to
            "all the tables defined in pudl.metadata".
    """
    if sqlite_path is None:
        sqlite_path = pathlib.Path(get_defaults()["pudl_out"]) / "pudl.sqlite"

    if metadata is None:
        metadata = Package.from_resource_ids().to_sql()

    if sqlite_path.exists():
        logger.info(f"Removing existing DB at {sqlite_path}...")
        sqlite_path.unlink()

    logger.info(f"Creating DB at {sqlite_path}...")
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")

    # Use the metadata to initialize the table schema in the new database
    with engine.connect() as conn:
        logger.info(f"Writing schema to DB at {sqlite_path}...")
        metadata.create_all(conn)


def main():
    """Main entry-point: parse args and run DB reset logic."""
    reset_db(**vars(_parse_args()))


if __name__ == "__main__":
    main()
