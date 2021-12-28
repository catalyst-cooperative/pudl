"""Routines for loading PUDL data into various storage formats."""

import logging
import sys
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version
from typing import Dict, List, Literal, Union

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa
from packaging import version
from pyarrow import parquet as pq
from sqlalchemy.exc import IntegrityError

from pudl.helpers import find_foreign_key_errors
from pudl.metadata.classes import Package, Resource

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


def dfs_to_sqlite(
    dfs: Dict[str, pd.DataFrame],
    engine: sa.engine.Engine,
    check_foreign_keys: bool = True,
    check_types: bool = True,
    check_values: bool = True,
) -> None:
    """
    Load a dictionary of dataframes into the PUDL SQLite DB.

    Args:
        dfs: Dictionary mapping table names to dataframes.
        engine: PUDL DB connection engine.
        check_foreign_keys: if True, enforce foreign key constraints.
        check_types: if True, enforce column data types.
        check_values: if True, enforce value constraints.

    """
    # This magic makes SQLAlchemy tell SQLite to check foreign key constraints
    # whenever we insert data into thd database, which it doesn't do by default
    @sa.event.listens_for(sa.engine.Engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        if isinstance(dbapi_connection, SQLite3Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute(
                f"PRAGMA foreign_keys={'ON' if check_foreign_keys else 'OFF'};")
            cursor.close()

    bad_sqlite_version = (
        version.parse(sqlite_version) < version.parse(MINIMUM_SQLITE_VERSION)
    )
    if bad_sqlite_version and check_types:
        check_types = False
        logger.warning(
            f"Found SQLite {sqlite_version} which is less than "
            f"the minimum required version {MINIMUM_SQLITE_VERSION} "
            "As a result, data type constraint checking has been disabled."
        )

    # Generate a SQLAlchemy MetaData object from dataframe names:
    md = (
        Package.from_resource_ids(resource_ids=tuple(sorted(dfs)))
        .to_sql(check_types=check_types, check_values=check_values)
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


def df_to_parquet(
    df: pd.DataFrame,
    resource_id: str,
    root_path: Union[str, Path],
    partition_cols: Union[List[str], Literal[None]] = None,
) -> None:
    """
    Write a PUDL table out to a partitioned Parquet dataset.

    Uses the name of the table to look up appropriate metadata and construct
    a PyArrow schema.

    Args:
        df: The tabular data to be written to a Parquet dataset.
        resource_id: Name of the table that's being written to Parquet.
        root_path: Top level directory for the partitioned dataset.
        partition_cols: Columns to use to partition the Parquet dataset. For
            EPA CEMS we use ["year", "state"].

    """
    pq.write_to_dataset(
        pa.Table.from_pandas(
            df,
            schema=Resource.from_id(resource_id).to_pyarrow(),
            preserve_index=False,
        ),
        root_path=root_path,
        partition_cols=partition_cols,
        compression="snappy",
    )
