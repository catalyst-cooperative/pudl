"""Shared utilities for ferc extraction process."""

from typing import Optional

import sqlalchemy as sa


def add_key_constraints(
    meta: sa.MetaData, pk_table: str, column: str, pk_column: str | None = None
) -> sa.MetaData:
    """Adds primary and foreign key to tables present in meta.

    Args:
        pk_table: name of the table that contains primary-key
        column: foreign key column name. Tables that contain this column will
        have foreign-key constraint added.
        pk_column: (optional) if specified, this is the primary key column name in
        the table. If not specified, it is assumed that this is the same as pk_column.
    """
    pk_column = pk_column or column
    for table in meta.tables.values():
        constraint = None
        if table.name == pk_table:
            constraint = sa.PrimaryKeyConstraint(
                pk_column, sqlite_on_conflict="REPLACE"
            )
        elif column in table.columns:
            constraint = sa.ForeignKeyConstraint(
                columns=[column],
                refcolumns=[f"{pk_table}.{pk_column}"],
            )
        if constraint:
            table.append_constraint(constraint)
    return meta
