"""Unit tests for the FERC SQLite provenance helpers."""

import os
from pathlib import Path
from typing import Literal

import dagster as dg
import pytest

from pudl.dagster.provenance import (
    FERC_TO_SQLITE_METADATA_KEY,
    FercSqliteProvenance,
    FercSqliteProvenanceRecord,
    assert_ferc_sqlite_compatible,
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
        sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
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


@pytest.mark.parametrize("status", ["skipped", "not_configured"])
def test_ferc_sqlite_provenance_record_minimal_round_trip(
    status: Literal["skipped", "not_configured"],
) -> None:
    """A minimal record still round-trips through the nested Dagster metadata."""
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
# assert_ferc_sqlite_compatible tests
# ---------------------------------------------------------------------------


def test_assert_ferc_sqlite_compatible_skips_without_instance(
    mocker,
) -> None:
    """Provenance check is skipped with a warning when no Dagster instance is available."""
    # TODO replace mocker with caplog
    mock_warn = mocker.patch("pudl.dagster.provenance.logger.warning")
    provenance = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        ferc_xbrl_extractor_version="1.0.0",
    )
    assert_ferc_sqlite_compatible(instance=None, provenance=provenance)
    assert mock_warn.call_count == 1
    assert "No Dagster instance is available" in mock_warn.call_args[0][0]


def test_assert_ferc_sqlite_compatible_skips_with_env_var(
    mocker,
) -> None:
    """Provenance check is skipped with a warning when PUDL_SKIP_FERC_SQLITE_PROVENANCE is set."""
    # TODO use monkeypatch.setenv instead of mocker for the env, and caplog for the warning
    mocker.patch.dict(os.environ, {"PUDL_SKIP_FERC_SQLITE_PROVENANCE": "true"})
    mock_instance = mocker.MagicMock()
    mock_warn = mocker.patch("pudl.dagster.provenance.logger.warning")

    provenance = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        ferc_xbrl_extractor_version="1.0.0",
    )

    assert_ferc_sqlite_compatible(instance=mock_instance, provenance=provenance)

    assert mock_warn.call_count == 1
    assert "PUDL_SKIP_FERC_SQLITE_PROVENANCE" in mock_warn.call_args[0][0]
    mock_instance.get_latest_materialization_event.assert_not_called()


@pytest.mark.parametrize("truthy_value", ["1", "true", "yes", "TRUE", "YES"])
def test_assert_ferc_sqlite_compatible_env_var_truthy_values(
    truthy_value: str,
    mocker,
) -> None:
    """All recognised truthy env var values skip the check."""
    # TODO combine parametrization with above test
    mocker.patch.dict(os.environ, {"PUDL_SKIP_FERC_SQLITE_PROVENANCE": truthy_value})
    mock_instance = mocker.MagicMock()
    # Should not raise.
    provenance = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[2018, 2019],
        ferc_xbrl_extractor_version="1.0.0",
    )

    assert_ferc_sqlite_compatible(instance=mock_instance, provenance=provenance)
    mock_instance.get_latest_materialization_event.assert_not_called()


@pytest.mark.parametrize(
    ("stored_years", "required_years", "should_raise"),
    [
        ([2020, 2021, 2022], [2020, 2021, 2022], False),  # exact match
        ([2020, 2021, 2022], [2021], False),  # stored ⊃ required
        ([2020, 2021, 2022], [2021, 2023], True),  # one year missing
        ([2020, 2021], [2022, 2023], True),  # all years missing
    ],
)
def test_assert_ferc_sqlite_compatible_year_subset_check(
    mocker,
    stored_years: list[int],
    required_years: list[int],
    should_raise: bool,
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

    stored_dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            stored.model_dump(mode="json")
        )
    }
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_dagster_meta)
    )
    if should_raise:
        with pytest.raises(RuntimeError, match="missing required years"):
            assert_ferc_sqlite_compatible(instance=instance, provenance=required)
    else:
        assert_ferc_sqlite_compatible(instance=instance, provenance=required)


def test_assert_ferc_sqlite_compatible_rejects_doi_mismatch(
    mocker,
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

    stored_dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            stored.model_dump(mode="json")
        )
    }
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_dagster_meta)
    )
    with pytest.raises(RuntimeError, match="Zenodo DOI mismatch"):
        assert_ferc_sqlite_compatible(instance=instance, provenance=required)


def test_assert_ferc_sqlite_compatible_rejects_xbrl_extractor_mismatch(
    mocker,
) -> None:
    """A Zenodo DOI mismatch should raise a descriptive RuntimeError."""

    stored = FercSqliteProvenanceRecord(
        dataset="ferc1",
        data_format="dbf",
        status="complete",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.1.0",
    )

    stored_dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            stored.model_dump(mode="json")
        )
    }
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_dagster_meta)
    )
    with pytest.raises(
        RuntimeError,
        match="FERC SQLite DB created with incompatible version of the XBRL extractor",
    ):
        assert_ferc_sqlite_compatible(instance=instance, provenance=required)


def test_assert_ferc_sqlite_compatible_rejects_missing_materialization(
    mocker,
) -> None:
    """Missing materialization event should raise a descriptive RuntimeError."""
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = None
    with pytest.raises(RuntimeError, match="No Dagster provenance metadata"):
        assert_ferc_sqlite_compatible(instance=instance, provenance=required)


@pytest.mark.parametrize(
    ("status", "expected_match"),
    [
        ("skipped", "status="),
        ("not_configured", "not_configured"),
    ],
)
def test_assert_ferc_sqlite_compatible_rejects_non_complete_status(
    mocker,
    status: Literal["skipped", "not_configured"],
    expected_match: str,
) -> None:
    """A DB materialized with a non-complete status should raise RuntimeError.

    Both 'skipped' and 'not_configured' mean the SQLite file was never fully
    populated, so downstream IO managers must refuse to read from it.
    """

    dagster_meta = {
        FERC_TO_SQLITE_METADATA_KEY: dg.MetadataValue.json(
            FercSqliteProvenanceRecord(
                dataset="ferc1",
                data_format="dbf",
                status=status,
                ferc_xbrl_extractor_version="1.0.0",
            ).model_dump(mode="json")
        )
    }
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=dagster_meta)
    )
    required = FercSqliteProvenance(
        dataset="ferc1",
        data_format="dbf",
        zenodo_doi="fake DOI",
        years=[],
        ferc_xbrl_extractor_version="1.0.0",
    )
    with pytest.raises(RuntimeError, match=expected_match):
        assert_ferc_sqlite_compatible(instance=instance, provenance=required)
