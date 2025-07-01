"""Check that foreign key constraints in the PUDL database are respected."""

import pathlib
import sys

import click
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
@click.option(
    "--db_path",
    help="Path to PUDL SQLite database where foreign keys should be checked.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default=None,
)
def pudl_check_fks(logfile: pathlib.Path, loglevel: str, db_path: pathlib.Path):
    """Check that foreign key constraints in the PUDL database are respected.

    Dagster manages the dependencies between various assets in our ETL pipeline,
    attempting to materialize tables only after their upstream dependencies have been
    satisfied. However, this order is non deterministic because they are executed in
    parallel, and doesn't necessarily correspond to the foreign-key constraints within
    the database, so durint the ETL we disable foreign key constraints within
    ``pudl.sqlite``.

    However, we still expect foreign key constraints to be satisfied once all of the
    tables have been loaded, so we check that they are valid after the ETL has
    completed. This script runs the same check.
    """
    load_dotenv()

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    # Using PudlPaths to get default value for CLI causes validation issues
    if not db_path:
        db_path = PudlPaths().output_dir / "pudl.sqlite"

    check_foreign_keys(sa.create_engine(f"sqlite:///{db_path}"))
    return 0


class ForeignKeyError(sa.exc.SQLAlchemyError):
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


class ForeignKeyErrors(sa.exc.SQLAlchemyError):  # noqa: N818
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
                    child_table=table_name,
                    parent_table=parent_name,
                    foreign_key=parent_fk,
                    rowids=parent_fk_df["rowid"].values,
                )
            )
        raise ForeignKeyErrors(errors)

    logger.info("Success! No foreign key constraint errors found.")


if __name__ == "__main__":
    sys.exit(pudl_check_fks())
