"""Shared utilities for ferc extraction process."""

from typing import Optional

import sqlalchemy as sa


def add_key_constraints(meta: sa.MetaData, pk_table: str, fk_column: str, pk_column: Optional[str] = None) -> sa.MetaData:
    """Adds primary and foreign key to tables present in meta.

    Args:
        pk_table: name of the table that contains primary-key
        fk_column: foreign key column name. Tables that contain this column will
        have foreign-key constraint added.
        pk_column: (optional) if specified, this is the primary key column name in
        the table. If not specified, it is assumed that this is the same as pk_column.
    """
    if fk_column is None:
        fk_column = pk_column

    for table in meta.tables.values():
        constraint = None
        if table.name == pk_table:
            constraint = sa.PrimaryKeyConstraint(pk_column, sqlite_on_conflict="REPLACE")
        elif fk_column in table.columns:
            constraint = sa.ForeignKeyConstraint(
                columns=[fk_column],
                refcolumns=[f"{pk_table}.{pk_column}"],
            )
        if constraint:
            table.append_constraint(constraint)
    return meta
