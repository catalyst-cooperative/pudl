"""Integration tests for data validation checks that run on prebuilt outputs."""

import pytest
import sqlalchemy as sa

from pudl.validate.dbt import build_with_context
from pudl.validate.integrity import check_foreign_keys


@pytest.mark.order(3)
def test_pudl_foreign_keys(pudl_engine: sa.Engine):
    """Validate foreign key constraints on the prebuilt PUDL SQLite database."""
    check_foreign_keys(pudl_engine)


@pytest.mark.order(4)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt(dbt_target: str):
    """Run the dbt data validations programmatically, excluding row count checks.

    Row count checks are run separately in ``test_dbt_row_counts`` so that their
    failures can be reported independently. See ``tests/validate/row_counts_test.py``.

    Because dbt reads data from our Parquet outputs, and the location of the Parquet
    outputs is determined by the PUDL_OUTPUT environment variable, and that environment
    variable is set during the test setup, we shouldn't need to do any special setup
    here to point dbt at the correct outputs.

    This test relies on the prebuilt outputs so the Parquet files are available.
    """
    test_result = build_with_context(
        node_selection="*",
        node_exclusion="test_name:check_row_counts_per_partition",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )
