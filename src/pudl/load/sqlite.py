"""Load PUDL data into an SQLite database."""

import logging

import sqlalchemy as sa

from pudl.metadata.classes import Package, Resource
from pudl.metadata.resources import RESOURCE_METADATA

logger = logging.getLogger(__name__)


def dfs_to_sqlite(dfs, engine, foreign_keys="OFF"):
    """Load a dictionary of dataframes into the PUDL DB."""
    # This magic makes SQLAlchemy tell SQLite to check foreign key constraints
    # whenever we insert data into thd database, which it doesn't do by default
    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        from sqlite3 import Connection as SQLite3Connection
        if isinstance(dbapi_connection, SQLite3Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute(f"PRAGMA foreign_keys={foreign_keys};")
            cursor.close()

    # Build a list describing all possible PUDL DB tables:
    resources = [Resource.from_id(x) for x in RESOURCE_METADATA]
    # Use that list of tables to generate a SQLAlchemy MetaData object:
    md = Package(name="pudl", resources=resources).to_sql()
    # Delete any existing tables, and create them anew:
    md.drop_all(engine)
    md.create_all(engine)

    # Load any tables that exist in our dictionary of dataframes into the
    # corresponding table in the newly create database:
    for table in md.sorted_tables:
        if table.name in dfs:
            logger.info(f"Loading {table.name} into PUDL SQLite DB.")
            dfs[table.name].to_sql(
                table.name,
                engine,
                if_exists="append",
                index=False,
            )
