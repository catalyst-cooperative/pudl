"""Integration tests verifying the Dagster pipeline produces expected outputs."""

import pytest

from pudl.helpers import get_parquet_table


@pytest.mark.order(2)
@pytest.mark.usefixtures("prebuilt_outputs")
def test_pudl_parquet_outputs():
    """Verify that key PUDL tables exist and are populated in the Parquet outputs.

    Foreign key validation lives in a separate data-validation test so the nightly
    build can report it independently from the rest of the integration suite.
    """
    required_tables = (
        "core_pudl__entity_plants_pudl",
        "core_pudl__entity_utilities_pudl",
    )

    for table_name in required_tables:
        df = get_parquet_table(table_name)
        assert not df.empty, f"Expected {table_name} to contain data."
