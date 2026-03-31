"""Data validation tests for dbt row count checks.

Row count checks are separated from the rest of the dbt validation suite because
``check_row_counts_per_partition`` behaves differently in development and nightly
builds, and is the most frequently failing check. Running it as a separate pytest
stage means a row count failure produces a clearly labelled report of its own rather
than failing the broader data validation stage.

Row count checks are only meaningful against a full ETL build (``dbt_target ==
'etl-full'``). The underlying dbt test disables itself automatically for other
targets, so this test skips explicitly to avoid a misleading green result.
"""

import pytest

from pudl.validate.dbt import build_with_context


@pytest.mark.order(5)
@pytest.mark.usefixtures("prebuilt_outputs", "test_dir")
def test_dbt_row_counts(dbt_target: str):
    """Run only the dbt ``check_row_counts_per_partition`` checks.

    This test is intentionally kept separate from :func:`test_dbt` in
    ``data_test.py`` so that row count failures can be reported as a distinct
    build stage and don't pollute the broader data validation results.

    Skipped when ``dbt_target`` is not ``'etl-full'`` because row counts are only
    populated for full builds — running them against a partial build would always
    pass vacuously.
    """
    if dbt_target != "etl-full":
        pytest.skip(
            f"Row count checks only apply to full ETL builds (dbt_target={dbt_target!r})"
        )

    test_result = build_with_context(
        node_selection="test_name:check_row_counts_per_partition",
        dbt_target=dbt_target,
    )

    if not test_result.success:
        raise AssertionError(
            f"Row count check failures:\n{test_result.format_failure_contexts()}"
        )
