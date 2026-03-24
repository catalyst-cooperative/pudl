"""PUDL data validation functions and test case specifications.

Note that this module is being cannibalized and translated into dbt tests.
"""

from typing import cast

import numpy as np
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class ExcessiveNullRowsError(ValueError):
    """Exception raised when rows have excessive null values."""

    def __init__(self, message: str, null_rows: pd.DataFrame):
        """Initialize the ExcessiveNullRowsError with a message and DataFrame of null rows."""
        super().__init__(message)
        self.null_rows = null_rows


def no_null_rows(
    df: pd.DataFrame,
    cols: list[str] | str = "all",
    df_name: str = "",
    max_null_fraction: float = 0.9,
) -> pd.DataFrame:
    """Check for rows with excessive missing values, usually due to a merge gone wrong.

    Sum up the number of NA values in each row and the columns specified by ``cols``.
    If the NA values make up more than ``max_null_fraction`` of the columns overall, the
    row is considered Null and the check fails.

    Args:
        df: Table to check for null rows.
        cols: Columns to check for excessive null value. If "all" check all columns.
        df_name: Name of the dataframe, to aid in debugging/logging.
        max_null_fraction: The maximum fraction of NA values allowed in any row.

    Returns:
        The input DataFrame, for use with DataFrame.pipe().

    Raises:
        ExcessiveNullRowsError: If the fraction of NA values in any row is greater than
        ``max_null_fraction``.
    """
    if cols == "all":
        cols = list(df.columns)

    null_rows = df[cols].isna().sum(axis="columns") / len(cols) > max_null_fraction
    if null_rows.any():
        raise ExcessiveNullRowsError(
            message=(
                f"Found {null_rows.sum()} excessively null rows in {df_name}.\n"
                f"{df[null_rows]}"
            ),
            null_rows=df[null_rows],
        )

    return df


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


def weighted_quantile(data: pd.Series, weights: pd.Series, quantile: float) -> float:
    """Calculate the weighted quantile of a Series or DataFrame column.

    This function allows us to take two columns from a :class:`pandas.DataFrame` one of
    which contains an observed value (data) like heat content per unit of fuel, and the
    other of which (weights) contains a quantity like quantity of fuel delivered which
    should be used to scale the importance of the observed value in an overall
    distribution, and calculate the values that the scaled distribution will have at
    various quantiles.

    Args:
        data: A series containing numeric data.
        weights: Weights to use in scaling the data. Must have the same length as data.
        quantile: A number between 0 and 1, representing the quantile at which we want
            to find the value of the weighted data.

    Returns:
        The value in the weighted data corresponding to the given quantile. If there are
        no values in the data, return :mod:`numpy.nan`.
    """
    if (quantile < 0) or (quantile > 1):
        raise ValueError("quantile must have a value between 0 and 1.")
    if len(data) != len(weights):
        raise ValueError("data and weights must have the same length")
    df = (
        pd.DataFrame({"data": data, "weights": weights})
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        # dbt weighted quantiles detour: the following group/sum operation is necessary to
        # match our weighted quantile definition in dbt, which treats repeated data values
        # as an n-way tie and pools the weights. see "Migrate vs_bounds" issue on github
        # for details:
        # https://github.com/catalyst-cooperative/pudl/issues/4106#issuecomment-2810774598
        .groupby("data")
        .sum()
        .reset_index()
        # /end dbt weighted quantiles detour
        .sort_values(by="data")
    )
    Sn = df.weights.cumsum()  # noqa: N806
    # This conditional is necessary because sometimes new columns get
    # added to the EIA data, and so they won't show up in prior years.
    if len(Sn) > 0:
        Pn = (Sn - 0.5 * df.weights) / Sn.iloc[-1]  # noqa: N806
        return np.interp(quantile, Pn, df.data)

    return np.nan
