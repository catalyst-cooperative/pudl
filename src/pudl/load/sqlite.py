"""Load PUDL data into an SQLite database."""

import logging
from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version

import sqlalchemy as sa
from packaging import version
from prefect import task

from pudl.dfc import DataFrameCollection
from pudl.metadata.classes import Package

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


@task(checkpoint=False)
def dfs_to_sqlite(
    dfs: DataFrameCollection,
    engine: sa.engine.Engine,
    check_foreign_keys: bool = True,
    check_types: bool = True,
    check_values: bool = True,
) -> None:
    """Load a dictionary of dataframes into the PUDL DB."""
    # This magic makes SQLAlchemy tell SQLite to check foreign key constraints
    # whenever we insert data into thd database, which it doesn't do by default
    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        if isinstance(dbapi_connection, SQLite3Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute(
                f"PRAGMA foreign_keys={'ON' if check_foreign_keys else 'OFF'};")
            cursor.close()

    if (version.parse(sqlite_version) < version.parse(MINIMUM_SQLITE_VERSION) and check_types):
        check_types = False
        logger.warning(
            f"Found SQLite {sqlite_version} which is less than "
            f"the minimum required version {MINIMUM_SQLITE_VERSION} "
            "As a result, data type constraint checking has been disabled."
        )

    # Get dataframes from DataFrameCollection.
    dfs = dfs.to_dict()

    # Generate a SQLAlchemy MetaData object from dataframe names:
    md = Package.from_resource_ids(dfs).to_sql(
        check_types=check_types,
        check_values=check_values,
    )
    # Delete any existing tables, and create them anew:
    md.drop_all(engine)
    md.create_all(engine)

    # Load any tables that exist in our dictionary of dataframes into the
    # corresponding table in the newly create database:
    for table in md.sorted_tables:
        logger.info(f"Loading {table.name} into PUDL SQLite DB.")
        dfs[table.name].to_sql(
            table.name,
            engine,
            if_exists="append",
            index=False,
            dtype={c.name: c.type for c in table.columns},
        )
