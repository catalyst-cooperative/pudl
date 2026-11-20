"""Integration tests for data validation checks that run on prebuilt outputs."""

import pytest

from pudl.validate.dbt import DBT_UNIT_TESTS_PATH, build_with_context


@pytest.mark.order(4)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt(dbt_target: str, dbt_source: str):
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
        node_selection=f"source:{dbt_source}+",
        node_exclusion="test_name:check_row_counts_per_partition",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )


@pytest.mark.order(4)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt_macro_unit_tests(dbt_target: str, dbt_source: str):
    """Run the unit tests of our dbt macros (``dbt/tests/unit_tests``).

    These exercise the macros behind our generic data tests using mock data, so they
    don't depend on the source being validated. They are run once, with the ``pudl``
    source, instead of again for every ``--dbt-source``. They live in this file so
    that every CI task that runs the dbt data validations also runs them, but as a
    separate test so that a macro regression isn't mixed in with data failures.
    """
    if dbt_source != "pudl":
        pytest.skip(
            f"dbt macro unit tests only run with the pudl source ({dbt_source=})"
        )

    test_result = build_with_context(
        node_selection=f"path:{DBT_UNIT_TESTS_PATH}",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"dbt macro unit test failures:\n{test_result.format_failure_contexts()}"
        )
