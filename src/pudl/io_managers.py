"""Dagster IO Managers."""
import os
from pathlib import Path
from sqlite3 import sqlite_version

import pandas as pd
import sqlalchemy as sa
from dagster import Field, IOManager, io_manager
from packaging import version

import pudl
from pudl.metadata.classes import Package

logger = pudl.logging_helpers.get_logger(__name__)

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
        check_types: bool = True,
        check_values: bool = True,
    ):
        """Init a SQLiteIOmanager."""
        self.base_dir = Path(base_dir)
        self.db_name = db_name

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

        # Create all database metadata
        self.md = Package.from_resource_ids().to_sql(
            check_types=check_types,
            check_values=check_values,
        )

    def _get_table_name(self, context) -> str:
        """Get asset name from dagster context object."""
        if context.has_asset_key:
            table_name = context.asset_key.to_python_identifier()
        else:
            table_name = context.get_identifier()
        return table_name

    def _get_database_engine(self) -> sa.engine.Engine:
        """Create database and metadata if they don't exist.

        Returns:
            engine: SQL Alchemy engine that connects to a database in the base_dir.
        """
        # If the sqlite directory doesn't exist, create it.
        if not self.base_dir.exists():
            self.base_dir.mkdir(parents=True)
        db_path = self.base_dir / f"{self.db_name}.sqlite"

        engine = sa.create_engine(f"sqlite:///{db_path}")

        # Create the database and schemas
        if not db_path.exists():
            db_path.touch()
            self.md.create_all(engine)

        return engine

    def _get_sqlalchemy_table(self, table_name: str) -> sa.Table:
        """Get SQL Alchemy Table object from metadata given a table_name.

        Args:
            table_name: The name of the table to look up.

        Returns:
            table: Corresponding SQL Alchemy Table in SQLiteIOManager metadata.

        Raises:
            RuntimeError: if table_name does not exist in the SQLiteIOManager metadata.
        """
        sa_table = self.md.tables.get(table_name, None)
        if sa_table is None:
            raise RuntimeError(
                f"{sa_table} not found in database metadata. Either add the table to the metadata or use a different IO Manager."
            )
        return sa_table

    def handle_output(self, context, df):
        """Handle an op or asset output."""
        table_name = self._get_table_name(context)

        sa_table = self._get_sqlalchemy_table(table_name)
        engine = self._get_database_engine()

        with engine.connect() as con:
            # Remove old table records before loading to db
            con.execute(sa_table.delete())
            df.to_sql(
                table_name,
                con,
                if_exists="append",
                index=False,
                dtype={c.name: c.type for c in sa_table.columns},
            )

    def load_input(self, context) -> pd.DataFrame:
        """Load a dataframe from a sqlite database."""
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = self._get_table_name(context)
        _ = self._get_sqlalchemy_table(table_name)

        engine = self._get_database_engine()

        with engine.connect() as con:
            return pudl.metadata.fields.apply_pudl_dtypes(
                pd.read_sql_table(table_name, con)
            )


@io_manager(
    config_schema={
        "pudl_output_path": Field(
            str,
            description="Path of directory to store the database in.",
            default_value=os.environ.get("PUDL_OUTPUT"),
        ),
        "check_types": Field(bool, default_value=True),
        "check_values": Field(bool, default_value=True),
    }
)
def pudl_sqlite_io_manager(init_context) -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource."""
    base_dir = init_context.resource_config["pudl_output_path"]
    check_types = init_context.resource_config["check_types"]
    check_values = init_context.resource_config["check_values"]
    return SQLiteIOManager(
        base_dir=base_dir,
        db_name="pudl",
        check_types=check_types,
        check_values=check_values,
    )
