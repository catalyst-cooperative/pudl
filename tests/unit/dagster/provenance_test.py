"""Unit tests for the FERC SQLite provenance helpers."""

import dagster as dg
import pytest

from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSqliteProvenance,
    FercSqliteProvenanceRecord,
    ferc_sqlite_provenance_is_compatible,
)
from pudl.settings import FercToSqliteDataConfig

# ---------------------------------------------------------------------------
# FercSqliteProvenanceRecord serialization round-trip
# ---------------------------------------------------------------------------


def test_ferc_sqlite_provenance_record_round_trip() -> None:
    """A complete record round-trips through Dagster metadata without data loss."""
    data_config = FercToSqliteDataConfig()
    record = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        data_config=data_config,
        ferc_xbrl_extractor_version="1.0.0",
    )
    dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            record.model_dump(mode="json")
        )
    }
    assert set(dagster_meta) == {FERC_TO_SQLITE_METADATA_KEY}
    recovered = FercSqliteProvenanceRecord.model_validate(
        dagster_meta[FERC_TO_SQLITE_METADATA_KEY].value
    )

    assert recovered == record
    assert recovered.data_config == data_config


def test_ferc_sqlite_provenance_record_round_trip_sqlite(tmp_path):
    """Test ``FercSqliteProvenanceRecord`` to sqlite and back again."""
    data_config = FercToSqliteDataConfig()
    sqlite_path = tmp_path / "test.sqlite"
    record = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        data_config=data_config,
        ferc_xbrl_extractor_version="1.0.0",
    )

    # Write to sqlite
    record.to_sqlite(sqlite_path)

    # Recover and compare
    recovered = FercSqliteProvenanceRecord.from_sqlite(sqlite_path)
    assert recovered == record
    assert recovered.data_config == data_config


def test_ferc_sqlite_provenance_record_minimal_round_trip() -> None:
    """A minimal record still round-trips through the nested Dagster metadata."""
    status = "not_configured"
    record = FercSqliteProvenanceRecord(
        dataset="ferc714",
        data_format="xbrl",
        status=status,
        ferc_xbrl_extractor_version="1.0.0",
    )
    dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            record.model_dump(mode="json")
        )
    }
    recovered = FercSqliteProvenanceRecord.model_validate(
        dagster_meta[FERC_TO_SQLITE_METADATA_KEY].value
    )

    assert recovered == record


# ---------------------------------------------------------------------------
# ferc_sqlite_provenance_is_compatible tests
# ---------------------------------------------------------------------------


@pytest.fixture
def warning_log(mocker):
    """Mock ``logger.warning`` to check for expected warning messages."""
    # TODO: Using caplog would probably be better, but it's failing in CI
    return mocker.patch("pudl.dagster.provenance.logger.warning")


def assert_warning_message(log_mock, message: str):
    """Assert that ``logger.warning`` was called with expected message."""
    assert any(message in call_args.args[0] for call_args in log_mock.call_args_list)


def test_fetch_stored_ferc_sqlite_provenance_metadata(mocker):
    """Test loading stored metadata from dagster intance."""
    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        ferc_xbrl_extractor_version="1.0.0",
    )

    # First try to fetch before correctly mocking instance to mimick no provenance being available
    instance = mocker.MagicMock()
    with pytest.raises(
        RuntimeError, match="No Dagster provenance metadata is available for"
    ):
        FercSqliteProvenanceRecord.from_dagster_instance(
            instance=instance, dataset="ferc1", data_format="dbf"
        )

    stored_dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            stored.model_dump(mode="json")
        )
    }
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_dagster_meta)
    )

    assert stored == FercSqliteProvenanceRecord.from_dagster_instance(
        instance=instance, dataset="ferc1", data_format="dbf"
    )


@pytest.mark.parametrize(
    ("stored_years", "required_years", "should_fail"),
    [
        ([2020, 2021, 2022], [2020, 2021, 2022], False),  # exact match
        ([2020, 2021, 2022], [2021], False),  # stored ⊃ required
        ([2020, 2021, 2022], [2021, 2023], True),  # one year missing
        ([2020, 2021], [2022, 2023], True),  # all years missing
    ],
)
def test_ferc_sqlite_provenance_is_compatible_year_subset_check(
    warning_log,
    mocker,
    stored_years: list[int],
    required_years: list[int],
    should_fail: bool,
) -> None:
    """Compatibility requires stored years to be a superset of required years.

    Passes when stored ⊇ required; raises RuntimeError when any required year is absent.
    """
    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="fake DOI",
        years=stored_years,
        ferc_xbrl_extractor_version="1.0.0",
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=required_years,
        ferc_xbrl_extractor_version="1.0.0",
    )
    if should_fail:
        assert not ferc_sqlite_provenance_is_compatible(
            observed_provenance=stored, required_provenance=required
        )
        assert_warning_message(warning_log, "missing required years")
    else:
        assert ferc_sqlite_provenance_is_compatible(
            observed_provenance=stored, required_provenance=required
        )


def test_ferc_sqlite_provenance_is_compatible_rejects_doi_mismatch(
    mocker,
    warning_log,
) -> None:
    """A Zenodo DOI mismatch should raise a descriptive RuntimeError."""

    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="stale DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    assert not ferc_sqlite_provenance_is_compatible(
        observed_provenance=stored, required_provenance=required
    )
    assert_warning_message(warning_log, "Zenodo DOI mismatch")


@pytest.mark.parametrize(
    ("data_format", "should_raise"),
    [
        ("dbf", False),
        ("xbrl", True),
    ],
)
def test_ferc_sqlite_provenance_is_compatible_rejects_xbrl_extractor_mismatch(
    mocker,
    warning_log,
    data_format: str,
    should_raise: bool,
) -> None:
    """A Zenodo DOI mismatch should raise a descriptive RuntimeError."""

    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format=data_format,
        status="complete",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format=data_format,
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.1.0",
    )
    if should_raise:
        assert not ferc_sqlite_provenance_is_compatible(
            observed_provenance=stored, required_provenance=required
        )
        assert_warning_message(
            warning_log,
            "FERC SQLite DB created with incompatible version of the XBRL extractor",
        )
    else:
        assert ferc_sqlite_provenance_is_compatible(
            observed_provenance=stored, required_provenance=required
        )


def test_ferc_sqlite_provenance_is_compatible_rejects_non_complete_status(
    mocker,
    warning_log,
) -> None:
    """A DB materialized with a non-complete status should raise RuntimeError."""
    status = "not_configured"

    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status=status,
        ferc_xbrl_extractor_version="1.0.0",
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    assert not ferc_sqlite_provenance_is_compatible(
        observed_provenance=stored, required_provenance=required
    )
    assert_warning_message(warning_log, status)
