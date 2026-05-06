"""Integration tests for data validation checks that run on prebuilt outputs."""

import pytest
import sqlalchemy as sa

from pudl.dbt_wrapper import build_with_context
from pudl.etl.check_foreign_keys import check_foreign_keys


@pytest.mark.order(3)
def test_pudl_foreign_keys(pudl_engine: sa.Engine):
    """Validate foreign key constraints on the prebuilt PUDL SQLite database."""
    check_foreign_keys(pudl_engine)


@pytest.mark.order(4)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt(dbt_target: str):
    """Run the dbt data validations programmatically.

    Because dbt reads data from our Parquet outputs, and the location of the Parquet
    outputs is determined by the PUDL_OUTPUT environment variable, and that environment
    variable is set during the test setup, we shouldn't need to do any special setup
    here to point dbt at the correct outputs.

    This test relies on the prebuilt outputs so the Parquet files are available.

    Note that the row count checks will automatically be disabled unless dbt_target is
    'etl-full'. See the ``check_row_counts_per_partition.sql`` generic test.
    """
    test_result = build_with_context(
        node_selection="*",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )
