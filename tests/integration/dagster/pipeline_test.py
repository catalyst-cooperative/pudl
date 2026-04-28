"""Integration tests verifying the Dagster pipeline produces expected outputs."""

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


@pytest.mark.order(2)
def test_pudl_engine(pudl_engine: sa.Engine):
    """Verify that key PUDL tables exist and are populated.

    Foreign key validation lives in a separate data-validation test so the nightly
    build can report it independently from the rest of the integration suite.
    """
    assert isinstance(pudl_engine, sa.Engine)
    insp: Inspector = sa.inspect(pudl_engine)
    required_tables = (
        "core_pudl__entity_plants_pudl",
        "core_pudl__entity_utilities_pudl",
    )

    for table_name in required_tables:
        assert table_name in insp.get_table_names()

    with pudl_engine.connect() as connection:
        for table_name in required_tables:
            first_row: int | None = connection.execute(
                sa.select(sa.literal(1)).select_from(sa.table(table_name)).limit(1)
            ).scalar()
            assert first_row is not None, f"Expected {table_name} to contain data."
