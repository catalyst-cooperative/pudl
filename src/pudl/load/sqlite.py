"""Load PUDL data into an SQLite database."""

import logging
from sqlite3 import Connection as SQLite3Connection
from typing import Dict

import pandas as pd
import sqlalchemy as sa

from pudl.metadata.classes import Package, Resource

logger = logging.getLogger(__name__)


def dfs_to_sqlite(
    dfs: Dict[str, pd.DataFrame],
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

    # Build a list of resources from the keysin our dataframe dictionary:
    resources = [Resource.from_id(x) for x in dfs]
    # Use that list of resources to generate a SQLAlchemy MetaData object:
    md = Package(
        name="pudl",
        resources=resources,
    ).to_sql(
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
        )
