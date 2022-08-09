"""Routines for loading PUDL data into various storage formats."""

import logging
import sys
from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version

import sqlalchemy as sa
from dagster import Field, resource
from packaging import version
from sqlalchemy.exc import IntegrityError

from pudl.helpers import find_foreign_key_errors
from pudl.metadata.classes import Package

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


class SQLiteManager:
    """Class for creating a sqlite database, schema and loading data."""

    def __init__(
        self,
        pudl_engine,
        check_foreign_keys: bool = True,
        check_types: bool = True,
        check_values: bool = True,
        clobber: bool = True,
    ) -> None:
        """Create database schemas for all PUDL tables."""
        engine = pudl_engine
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
        if bad_sqlite_version and check_types:
            check_types = False
            logger.warning(
                f"Found SQLite {sqlite_version} which is less than "
                f"the minimum required version {MINIMUM_SQLITE_VERSION} "
                "As a result, data type constraint checking has been disabled."
            )

        # Generate a SQLAlchemy MetaData object from dataframe names:
        # TODO: This currently create all of the schemas.
        md = Package.from_resource_ids().to_sql(
            check_types=check_types,
            check_values=check_values,
        )
        # Delete any existing tables, and create them anew:
        if clobber:
            md.drop_all(engine)
        md.create_all(engine)

        self.metadata = md
        self.pudl_engine = pudl_engine

    def dfs_to_sqlite(self, dfs):
        """Load dictionary of dataframes to the sqlite database."""
        df_schemas = list(set(dfs.keys()).intersection(self.metadata.sorted_tables))
        # TODO: raise exception if df_schemas isn't the same length of dfs

        for table in df_schemas:
            logger.info(f"Loading {table.name} into PUDL SQLite DB.")
            try:
                dfs[table.name].to_sql(
                    table.name,
                    self.pudl_engine,
                    if_exists="append",
                    index=False,
                    dtype={c.name: c.type for c in table.columns},
                )
            except IntegrityError as err:
                logger.info(find_foreign_key_errors(dfs))
                logger.info(err)
                sys.exit(1)


@resource(
    config_schema={
        "check_foreign_keys": Field(bool, default_value=True),
        "check_types": Field(bool, default_value=True),
        "check_values": Field(bool, default_value=True),
        "clobber": Field(bool, default_value=True),
    },
    required_resource_keys={"pudl_engine"},
)
def sqlite_manager(init_context):
    """Create SQLite manager that can be configured by dagster."""
    check_foreign_keys = init_context.resource_config["check_foreign_keys"]
    check_types = init_context.resource_config["check_types"]
    check_values = init_context.resource_config["check_values"]
    clobber = init_context.resource_config["clobber"]
    pudl_engine = init_context.resources.pudl_engine

    return SQLiteManager(
        pudl_engine=pudl_engine,
        check_foreign_keys=check_foreign_keys,
        check_types=check_types,
        check_values=check_values,
        clobber=clobber,
    )
