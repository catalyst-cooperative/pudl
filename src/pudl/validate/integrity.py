"""Database integrity validation checks for PUDL data.

This module implements checks for structural database constraints such as foreign key
relationships. These checks are applied after all data has been loaded into the database,
since the parallel nature of the ETL pipeline means that foreign key constraints cannot
be enforced during loading. As these checks are migrated into dbt, this module should
shrink accordingly.
"""

from typing import cast

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class ForeignKeyError(sa_exc.SQLAlchemyError):
    """Raised when data in a database violates a foreign key constraint."""

    def __init__(
        self, child_table: str, parent_table: str, foreign_key: str, rowids: list[int]
    ):
        """Initialize a new ForeignKeyError object.

        Args:
            child_table: The table that a foreign key constraint is applied to.
            parent_table: The table that a foreign key constraint refers to.
            foreign_key: Comma separated string of key(s) that make up foreign key.
            rowids: Rowid(s) of child_table where constraint failed (See
                https://www.sqlite.org/lang_createtable.html#rowid for more on rowid's).
        """
        self.child_table = child_table
        self.parent_table = parent_table
        self.foreign_key = foreign_key
        self.rowids = rowids

    def __str__(self):
        """Create string representation of ForeignKeyError object."""
        return (
            f"Foreign key error for table: {self.child_table} -- {self.parent_table} "
            f"{self.foreign_key} -- on rows {self.rowids}\n"
        )

    def __eq__(self, other):
        """Compare a ForeignKeyError with another object."""
        if isinstance(other, ForeignKeyError):
            return (
                (self.child_table == other.child_table)
                and (self.parent_table == other.parent_table)
                and (self.foreign_key == other.foreign_key)
                and (self.rowids == other.rowids)
            )
        return False


class ForeignKeyErrors(sa_exc.SQLAlchemyError):  # noqa: N818
    """Raised when data in a database violate multiple foreign key constraints."""

    def __init__(self, fk_errors: list[ForeignKeyError]):
        """Initialize a new ForeignKeyErrors object."""
        self.fk_errors = fk_errors

    def __str__(self):
        """Create string representation of ForeignKeyErrors object."""
        fk_errors = [str(x) for x in self.fk_errors]
        return "\n".join(fk_errors)

    def __iter__(self):
        """Iterate over the fk errors."""
        return self.fk_errors

    def __getitem__(self, idx):
        """Index the fk errors."""
        return self.fk_errors[idx]


def _get_fk_list(engine: sa.Engine, table: str) -> pd.DataFrame:
    """Retrieve a dataframe of foreign keys for a table.

    Description from the SQLite Docs: 'This pragma returns one row for each foreign
    key constraint created by a REFERENCES clause in the CREATE TABLE statement of
    table "table-name".'

    The PRAGMA returns one row for each field in a foreign key constraint. This
    method collapses foreign keys with multiple fields into one record for
    readability.
    """
    with engine.begin() as con:
        table_fks = pd.read_sql_query(f"PRAGMA foreign_key_list({table});", con)

    # Foreign keys with multiple fields are reported in separate records.
    # Combine the multiple fields into one string for readability.
    # Drop duplicates so we have one FK for each table and foreign key id
    table_fks["fk"] = table_fks.groupby("table")["to"].transform(
        lambda field: "(" + ", ".join(field) + ")"
    )
    table_fks = table_fks[["id", "table", "fk"]].drop_duplicates()

    # Rename the fields so we can easily merge with the foreign key errors.
    table_fks = table_fks.rename(columns={"id": "fkid", "table": "parent"})
    table_fks["table"] = table
    return table_fks


def check_foreign_keys(engine: sa.Engine):
    """Check foreign key relationships in the database.

    The order assets are loaded into the database will not satisfy foreign key
    constraints so we can't enable foreign key constraints. However, we can
    check for foreign key failures once all of the data has been loaded into
    the database using the `foreign_key_check` and `foreign_key_list` PRAGMAs.

    You can learn more about the PRAGMAs in the `SQLite docs
    <https://www.sqlite.org/pragma.html#pragma_foreign_key_check>`__.

    Raises:
        ForeignKeyErrors: if data in the database violate foreign key constraints.
    """
    logger.info(f"Running foreign key check on {engine.url} database.")
    with engine.begin() as con:
        fk_errors = pd.read_sql_query("PRAGMA foreign_key_check;", con)

    if not fk_errors.empty:
        # Merge in the actual FK descriptions
        tables_with_fk_errors = fk_errors.table.unique().tolist()
        table_foreign_keys = pd.concat(
            [_get_fk_list(engine, table) for table in tables_with_fk_errors]
        )

        fk_errors_with_keys = fk_errors.merge(
            table_foreign_keys,
            how="left",
            on=["parent", "fkid", "table"],
            validate="m:1",
        )

        errors = []
        # For each foreign key error, raise a ForeignKeyError
        for (
            table_name,
            parent_name,
            parent_fk,
        ), parent_fk_df in fk_errors_with_keys.groupby(["table", "parent", "fk"]):
            errors.append(
                ForeignKeyError(
                    child_table=cast(str, table_name),
                    parent_table=cast(str, parent_name),
                    foreign_key=cast(str, parent_fk),
                    rowids=cast(list[int], parent_fk_df["rowid"].tolist()),
                )
            )
        raise ForeignKeyErrors(errors)

    logger.info("Success! No foreign key constraint errors found.")
