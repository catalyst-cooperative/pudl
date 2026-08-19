"""Unit tests for pudl.transform.ferceqr's cross-quarter diagnostics asset."""

from types import SimpleNamespace

import dagster as dg
import pytest

from pudl.transform.ferceqr import (
    _build_ferceqr_diagnostics_rows,
    _latest_check_evaluations_by_quarter,
    _latest_extraction_stats_by_quarter,
    _summarize_check_failures,
    ferceqr_pipeline_diagnostics,
)

_YEAR_QUARTERS = dg.StaticPartitionsDefinition(["2024q1", "2024q2"])


def test_latest_extraction_stats_by_quarter_uses_most_recent_materialization():
    """Re-materializing a partition should replace, not duplicate, its stats.

    Uses a real ephemeral Dagster instance and real materialization, since
    ``fetch_materializations`` correctly threads a run's ``partition_key``
    through to the resulting event under this test harness (unlike asset check
    evaluations -- see the mocked test below).
    """
    stats_by_quarter = {
        "2024q1": {
            "total_filings": 1,
            "corrupt_filings": 0,
            "table_file_counts": {},
            "rejected_record_counts": {},
        },
        "2024q2": {
            "total_filings": 2,
            "corrupt_filings": 0,
            "table_file_counts": {},
            "rejected_record_counts": {},
        },
    }

    @dg.asset(name="raw_ferceqr__extract_errors", partitions_def=_YEAR_QUARTERS)
    def _fake_extract_errors(context: dg.AssetExecutionContext):
        context.add_output_metadata(
            metadata={
                "extraction_stats": dg.MetadataValue.json(
                    stats_by_quarter[context.partition_key]
                )
            }
        )

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([_fake_extract_errors], instance=instance, partition_key="2024q1")
    dg.materialize([_fake_extract_errors], instance=instance, partition_key="2024q2")

    # Re-materialize 2024q1 with different stats -- the reader should pick up
    # this newer materialization, not the original one.
    stats_by_quarter["2024q1"] = {
        "total_filings": 99,
        "corrupt_filings": 1,
        "table_file_counts": {},
        "rejected_record_counts": {},
    }
    dg.materialize([_fake_extract_errors], instance=instance, partition_key="2024q1")

    result = _latest_extraction_stats_by_quarter(instance)

    assert result["2024q1"]["total_filings"] == 99
    assert result["2024q1"]["corrupt_filings"] == 1
    assert result["2024q2"]["total_filings"] == 2


def _fake_check_evaluation_record(
    asset_key: dg.AssetKey,
    check_name: str,
    partition: str | None,
    passed: bool,
    metadata: dict,
):
    """Build a minimal stand-in for the event record shape ``get_event_records`` returns.

    ``dg.materialize()``'s single-process test harness doesn't thread a run's
    ``partition_key`` through to ASSET_CHECK_EVALUATION events the way it does
    for materializations (confirmed by inspection -- real scheduled/backfill
    runs populate this correctly, as seen in this project's actual Dagster
    history), so a real end-to-end check-evaluation test isn't possible here.
    This fakes just enough of the record structure that
    ``_latest_check_evaluations_by_quarter`` actually reads. ``evaluation`` itself is
    a real ``AssetCheckEvaluation``, not a stand-in, since
    ``_latest_check_evaluations_by_quarter`` narrows to that type with ``isinstance``
    before reading it.
    """
    evaluation = dg.AssetCheckEvaluation(
        asset_key=asset_key,
        check_name=check_name,
        passed=passed,
        metadata=metadata,
        target_materialization_data=None,
        severity=dg.AssetCheckSeverity.ERROR,
        description=None,
        blocking=True,
        partition=partition,
    )
    dagster_event = SimpleNamespace(event_specific_data=evaluation)
    event_log_entry = SimpleNamespace(dagster_event=dagster_event)
    return SimpleNamespace(event_log_entry=event_log_entry)


def test_latest_check_evaluations_by_quarter_groups_by_quarter_and_table(mocker):
    """Evaluations are grouped by quarter and table label, keeping only the latest."""
    index_pub_key = dg.AssetKey(["core_ferceqr__quarterly_index_pub"])
    contracts_key = dg.AssetKey(["core_ferceqr__contracts"])
    records = [
        # Most recent first, matching get_event_records(ascending=False).
        _fake_check_evaluation_record(
            index_pub_key, "pandera_schema_check", "2024q1", passed=False, metadata={}
        ),
        _fake_check_evaluation_record(
            # A stale, earlier run for the same table+quarter -- should be ignored.
            index_pub_key,
            "pandera_schema_check",
            "2024q1",
            passed=True,
            metadata={},
        ),
        _fake_check_evaluation_record(
            contracts_key, "pandera_schema_check", "2024q1", passed=True, metadata={}
        ),
        _fake_check_evaluation_record(
            # A different check on the same asset -- should be ignored.
            index_pub_key,
            "some_other_check",
            "2024q1",
            passed=False,
            metadata={},
        ),
        _fake_check_evaluation_record(
            # No partition at all -- should be ignored.
            index_pub_key,
            "pandera_schema_check",
            None,
            passed=True,
            metadata={},
        ),
    ]
    instance = mocker.MagicMock()
    instance.get_event_records.return_value = records

    result = _latest_check_evaluations_by_quarter(instance)

    assert set(result) == {"2024q1"}
    assert result["2024q1"]["index_pub"].passed is False
    assert result["2024q1"]["contracts"].passed is True


@pytest.mark.parametrize(
    ("passed", "metadata", "expected"),
    [
        (True, {}, ""),
        (False, {}, "FAILED (no detail available)"),
        (
            False,
            {
                "detailed_errors": dg.MetadataValue.json(
                    [
                        {
                            "check": "multiple_fields_uniqueness",
                            "error_message": "2 duplicate combinations",
                            "failure_case_count": 2,
                        }
                    ]
                )
            },
            "2x multiple_fields_uniqueness (2 duplicate combinations)",
        ),
    ],
)
def test_summarize_check_failures(passed, metadata, expected):
    """A passing check summarizes to "", a failing one names the failure(s)."""
    # SimpleNamespace stands in for AssetCheckEvaluation, which only needs to
    # supply the two attributes _summarize_check_failures actually reads.
    evaluation = SimpleNamespace(passed=passed, metadata=metadata)
    assert _summarize_check_failures(evaluation) == expected  # type: ignore[bad-argument-type]


def test_build_ferceqr_diagnostics_rows_combines_stats_and_checks():
    """Rows combine extraction stats and check results, keyed by year_quarter."""
    extraction_stats_by_quarter = {
        "2024q1": {
            "total_filings": 10,
            "corrupt_filings": 1,
            "table_file_counts": {"ident": 9, "contracts": 5},
            "rejected_record_counts": {"TOO MANY COLUMNS": 3},
        },
    }
    # SimpleNamespace stands in for AssetCheckEvaluation, which only needs to
    # supply the two attributes _build_ferceqr_diagnostics_rows actually reads.
    evaluation = SimpleNamespace(
        passed=False,
        metadata={
            "asset_shape": dg.MetadataValue.json([50, 6]),
            "detailed_errors": dg.MetadataValue.json(
                [
                    {
                        "check": "multiple_fields_uniqueness",
                        "error_message": "1 duplicate combination",
                        "failure_case_count": 1,
                    }
                ]
            ),
        },
    )
    check_evaluations_by_quarter = {"2024q1": {"index_pub": evaluation}}

    rows = _build_ferceqr_diagnostics_rows(
        extraction_stats_by_quarter,
        check_evaluations_by_quarter,  # type: ignore[bad-argument-type]
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["year_quarter"] == "2024q1"
    assert row["total_filings"] == 10
    assert row["corrupt_filings"] == 1
    assert row["n_ident"] == 9
    assert row["n_contracts"] == 5
    assert row["rejected_too_many_columns"] == 3
    assert row["rows_index_pub"] == 50
    assert row["rows_contracts"] is None
    assert "index_pub" in row["check_failures"]
    assert "multiple_fields_uniqueness" in row["check_failures"]


def test_build_ferceqr_diagnostics_rows_empty_when_no_data():
    """No extraction stats or check evaluations means no rows, not an error."""
    assert _build_ferceqr_diagnostics_rows({}, {}) == []


def test_ferceqr_pipeline_diagnostics_wraps_rows_in_table_metadata(mocker):
    """The asset attaches the computed rows as table metadata, not real data."""
    fake_rows = [
        {"year_quarter": "2024q1", "total_filings": 10, "check_failures": ""},
        {"year_quarter": "2024q2", "total_filings": 12, "check_failures": "oops"},
    ]
    mocker.patch(
        "pudl.transform.ferceqr._latest_extraction_stats_by_quarter",
        return_value={"unused": "stats"},
    )
    mocker.patch(
        "pudl.transform.ferceqr._latest_check_evaluations_by_quarter",
        return_value={"unused": "evaluations"},
    )
    mocker.patch(
        "pudl.transform.ferceqr._build_ferceqr_diagnostics_rows",
        return_value=fake_rows,
    )

    context = dg.build_asset_context()
    result = ferceqr_pipeline_diagnostics(context=context)

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata is not None
    n_quarters = result.metadata["n_quarters"]
    assert isinstance(n_quarters, dg.IntMetadataValue)
    assert n_quarters.value == 2
    table = result.metadata["summary"]
    assert isinstance(table, dg.TableMetadataValue)
    assert [record.data for record in table.records] == fake_rows
