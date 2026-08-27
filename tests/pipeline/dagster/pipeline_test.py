"""Integration tests verifying the Dagster pipeline produces expected outputs."""

import pytest
import sqlalchemy as sa

from pudl.helpers import get_parquet_table

REQUIRED_TABLES = (
    "core_pudl__entity_plants_pudl",
    "core_pudl__entity_utilities_pudl",
)


@pytest.mark.order(2)
@pytest.mark.usefixtures("prebuilt_outputs")
def test_pudl_parquet_outputs():
    """Verify that key PUDL tables exist and are populated in the Parquet outputs.

    Foreign key validation lives in a separate data-validation test so the nightly
    build can report it independently from the rest of the integration suite.
    """
    for table_name in REQUIRED_TABLES:
        df = get_parquet_table(table_name)
        assert not df.empty, f"Expected {table_name} to contain data."


@pytest.mark.order(2)
def test_pudl_engine(pudl_engine: sa.Engine):
    """Verify that key PUDL tables exist and are populated in pudl.sqlite."""
    insp = sa.inspect(pudl_engine)
    for table_name in REQUIRED_TABLES:
        assert table_name in insp.get_table_names()
    with pudl_engine.connect() as connection:
        for table_name in REQUIRED_TABLES:
            first_row = connection.execute(
                sa.select(sa.literal(1)).select_from(sa.table(table_name)).limit(1)
            ).scalar()
            assert first_row is not None, f"Expected {table_name} to contain data."
