"""Routines for loading PUDL data into various storage formats."""

import logging
import sys
from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version

import pandas as pd
import sqlalchemy as sa
from dagster import Field, Nothing, op
from packaging import version
from sqlalchemy.exc import IntegrityError

from pudl.helpers import find_foreign_key_errors
from pudl.metadata.classes import Package

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


@op(
    config_schema={
        "check_foreign_keys": Field(bool, default_value=True),
        "check_types": Field(bool, default_value=True),
        "check_values": Field(bool, default_value=True),
    },
    required_resource_keys={"pudl_engine"},
)
def dfs_to_sqlite(
    context,
    dfs: dict[str, pd.DataFrame],
) -> Nothing:
    """Load a dictionary of dataframes into the PUDL SQLite DB.

    Args:
        context: dagster context keyword.
        dfs: All transformed dataframes.
    """
    engine = context.resources.pudl_engine
    check_foreign_keys = context.op_config["check_foreign_keys"]
    # This magic makes SQLAlchemy tell SQLite to check foreign key constraints
    # whenever we insert data into thd database, which it doesn't do by default

    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        if isinstance(dbapi_connection, SQLite3Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute(
                f"PRAGMA foreign_keys={'ON' if check_foreign_keys else 'OFF'};"
            )
            cursor.close()

    bad_sqlite_version = version.parse(sqlite_version) < version.parse(
        MINIMUM_SQLITE_VERSION
    )
    check_types = context.op_config["check_types"]
    if bad_sqlite_version and check_types:
        check_types = False
        logger.warning(
            f"Found SQLite {sqlite_version} which is less than "
            f"the minimum required version {MINIMUM_SQLITE_VERSION} "
            "As a result, data type constraint checking has been disabled."
        )

    # Generate a SQLAlchemy MetaData object from dataframe names:
    md = Package.from_resource_ids(resource_ids=tuple(sorted(dfs))).to_sql(
        check_types=check_types,
        check_values=context.op_config["check_values"],
    )
    # Delete any existing tables, and create them anew:
    md.drop_all(engine)
    md.create_all(engine)

    # Load any tables that exist in our dictionary of dataframes into the
    # corresponding table in the newly create database:
    for table in md.sorted_tables:
        logger.info(f"Loading {table.name} into PUDL SQLite DB.")
        try:
            dfs[table.name].to_sql(
                table.name,
                engine,
                if_exists="append",
                index=False,
                dtype={c.name: c.type for c in table.columns},
            )
        except IntegrityError as err:
            logger.info(find_foreign_key_errors(dfs))
            logger.info(err)
            sys.exit(1)
