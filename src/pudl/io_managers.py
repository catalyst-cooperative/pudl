"""Dagster IO Managers."""
import logging
import os
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version

import pandas as pd
import sqlalchemy as sa
from dagster import Field, IOManager, io_manager
from packaging import version

from pudl.helpers import get_pudl_etl_tables
from pudl.metadata.classes import Package

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


class SQLiteIOManager(IOManager):
    """Dagster IO manager that stores and retrieves dataframes from a SQLite database.

    Args:
        base_dir (Optional[str]): base directory where all the step outputs which use this object
            manager will be stored in.
    """

    def __init__(
        self,
        base_dir: str = None,
        db_name: str = None,
        check_foreign_keys: bool = True,
        check_types: bool = True,
        check_values: bool = True,
    ):
        """Init a SQLiteIOmanager."""
        self.base_dir = Path(base_dir)
        self.db_name = db_name
        self.check_foreign_keys = check_foreign_keys

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

        # TODO: get_pudl_etl_tables is a temporary function until we add dagster op name to metadata
        self.md = Package.from_resource_ids(resource_ids=get_pudl_etl_tables()).to_sql(
            check_types=check_types,
            check_values=check_values,
        )

    def _setup_database(self, engine):
        print("Setting up the database!")
        print(self.check_foreign_keys)

        @sa.event.listens_for(sa.engine.Engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, SQLite3Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute(
                    f"PRAGMA foreign_keys={'ON' if self.check_foreign_keys else 'OFF'};"
                )
                cursor.close()

        # Create tables
        self.md.create_all(engine)

    def _get_database_engine(self, context) -> sa.engine.Engine:
        """Create database and metadata if they don't exist."""
        # TODO: is the run_id gauranteed to be the first one?
        run_id = context.get_identifier()[0]
        run_dir = self.base_dir / run_id / "sqlite"

        # If the sqlite directory doesn't exist, create it.
        if not run_dir.exists():
            run_dir.mkdir(parents=True)
        db_path = run_dir / f"{self.db_name}.sqlite"

        engine = sa.create_engine(f"sqlite:///{db_path}")

        # If the database doesn't exist, create it
        if not db_path.exists():
            db_path.touch()
            self._setup_database(engine)

        return engine

    def handle_output(self, context, obj):
        """Handle an op or asset output."""
        if context.has_asset_key:
            table_name = context.get_asset_identifier()[0]
        else:
            table_name = context.get_identifier()
        print(f"table_name: {table_name}")

        sa_tables = self.md.sorted_tables
        try:
            sa_table = [table for table in sa_tables if table.name == table_name][0]
        except IndexError:
            raise KeyError(f"{table_name} does not exist in metadata.")

        engine = self._get_database_engine(context)
        with engine.connect() as con:
            obj.to_sql(
                table_name,
                con,
                if_exists="append",
                index=False,
                dtype={c.name: c.type for c in sa_table.columns},
            )

    def load_input(self, context):
        """Load a dataframe from a sqlite database."""
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.name

        engine = self._get_database_engine(context)
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)


@io_manager(
    config_schema={
        "pudl_output_path": Field(
            str,
            description="Clobber and recreate the database if True.",
            default_value=os.environ.get("PUDL_OUTPUT"),
        ),
        "check_foreign_keys": Field(bool, default_value=True),
        "check_types": Field(bool, default_value=True),
        "check_values": Field(bool, default_value=True),
    }
)
def pudl_sqlite_io_manager(init_context) -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource."""
    base_dir = init_context.resource_config["pudl_output_path"]
    check_foreign_keys = init_context.resource_config["check_foreign_keys"]
    check_types = init_context.resource_config["check_types"]
    check_values = init_context.resource_config["check_values"]
    return SQLiteIOManager(
        base_dir=base_dir,
        db_name="pudl",
        check_foreign_keys=check_foreign_keys,
        check_types=check_types,
        check_values=check_values,
    )
