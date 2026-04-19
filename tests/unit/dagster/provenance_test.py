"""Unit tests for the FERC SQLite provenance helpers."""

import os
from pathlib import Path

import dagster as dg
import pytest

from pudl.dagster.provenance import (
    FercSQLiteProvenance,
    FercSQLiteProvenanceRecord,
    _parse_db_name,
    assert_ferc_sqlite_compatible,
    build_ferc_sqlite_provenance_metadata,
)
from pudl.settings import EtlSettings, FercToSqliteSettings
from pudl.workspace.datastore import ZenodoDoiSettings

# pytestmark: MarkDecorator = pytest.mark.ferc1_sqlite_provenance


@pytest.fixture()
def etl_settings() -> EtlSettings:
    """Minimal ETL settings with FERC-to-SQLite config for provenance tests."""
    return EtlSettings(ferc_to_sqlite_settings=FercToSqliteSettings())


@pytest.fixture()
def zenodo_dois() -> ZenodoDoiSettings:
    """Default Zenodo DOI settings."""
    return ZenodoDoiSettings()


# ---------------------------------------------------------------------------
# FercSQLiteProvenance factory tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dataset", "data_format"),
    [
        ("ferc1", "dbf"),
        ("ferc1", "xbrl"),
        ("ferc714", "xbrl"),
        ("ferc2", "dbf"),
    ],
)
def test_ferc_sqlite_provenance_from_dataset_and_format(
    dataset: str,
    data_format: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """from_dataset_and_format builds the correct provenance fingerprint."""
    provenance = FercSQLiteProvenance.from_dataset_and_format(
        dataset=dataset,
        data_format=data_format,
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert isinstance(provenance, FercSQLiteProvenance)
    assert provenance.dataset == dataset
    assert provenance.data_format == data_format
    assert provenance.asset_key == dg.AssetKey(f"raw_{dataset}_{data_format}__sqlite")


@pytest.mark.parametrize(
    ("db_name", "expected_dataset", "expected_format"),
    [
        ("ferc1_dbf", "ferc1", "dbf"),
        ("ferc1_xbrl", "ferc1", "xbrl"),
        ("ferc714_xbrl", "ferc714", "xbrl"),
        ("ferc2_dbf", "ferc2", "dbf"),
    ],
)
def test_parse_db_name(
    db_name: str, expected_dataset: str, expected_format: str
) -> None:
    """_parse_db_name correctly splits a db_name into dataset and format."""
    dataset, data_format = _parse_db_name(db_name)
    assert dataset == expected_dataset
    assert data_format == expected_format


def test_ferc_sqlite_provenance_years_reflect_settings(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Years in the provenance fingerprint must match those in the ETL settings.

    The compatibility check uses set equality, so order does not matter, and an
    empty year list is a valid statement of provenance (no years were processed).
    """
    from pudl.settings import Ferc1DbfToSqliteSettings

    configured_years = [2020, 2021]
    settings = EtlSettings(
        ferc_to_sqlite_settings=etl_settings.ferc_to_sqlite.model_copy(
            update={
                "ferc1_dbf_to_sqlite_settings": Ferc1DbfToSqliteSettings(
                    years=configured_years
                )
            }
        )
    )
    provenance = FercSQLiteProvenance.from_dataset_and_format(
        dataset="ferc1",
        data_format="dbf",
        etl_settings=settings,
        zenodo_dois=zenodo_dois,
    )
    assert set(provenance.years) == set(configured_years)


@pytest.mark.parametrize(
    "db_name",
    [
        "not_a_ferc_db",
        "ferc1",  # missing _dbf or _xbrl suffix
    ],
)
def test_parse_db_name_rejects_bad_input(db_name: str) -> None:
    """Malformed db_names should raise ValueError."""
    with pytest.raises(ValueError):
        _parse_db_name(db_name)


# ---------------------------------------------------------------------------
# FercSQLiteProvenanceRecord serialization round-trip
# ---------------------------------------------------------------------------


def test_ferc_sqlite_provenance_record_round_trip(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """A complete record round-trips through Dagster metadata without data loss."""
    record = build_ferc_sqlite_provenance_metadata(
        dataset="ferc1",
        data_format="dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
        status="complete",
    )
    dagster_meta = record.to_dagster_metadata()
    recovered = FercSQLiteProvenanceRecord.from_dagster_metadata(dagster_meta)

    assert recovered.status == record.status
    assert recovered.zenodo_doi == record.zenodo_doi
    assert recovered.years == record.years
    assert recovered.dataset == record.dataset


@pytest.mark.parametrize("status", ["skipped", "not_configured"])
def test_ferc_sqlite_provenance_record_minimal_round_trip(status: str) -> None:
    """A skipped or not_configured record only stores dataset and status."""
    record = FercSQLiteProvenanceRecord(dataset="ferc714", status=status)
    dagster_meta = record.to_dagster_metadata()
    recovered = FercSQLiteProvenanceRecord.from_dagster_metadata(dagster_meta)

    assert recovered.status == status
    assert recovered.dataset == "ferc714"
    assert recovered.zenodo_doi is None
    assert recovered.years is None


# ---------------------------------------------------------------------------
# assert_ferc_sqlite_compatible tests
# ---------------------------------------------------------------------------


def test_assert_ferc_sqlite_compatible_skips_without_instance(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Provenance check is skipped with a warning when no Dagster instance is available."""
    mock_warn = mocker.patch("pudl.dagster.provenance.logger.warning")
    assert_ferc_sqlite_compatible(
        instance=None,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert mock_warn.call_count == 1
    assert "No Dagster instance is available" in mock_warn.call_args[0][0]


def test_assert_ferc_sqlite_compatible_skips_with_env_var(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Provenance check is skipped with a warning when PUDL_SKIP_FERC_SQLITE_PROVENANCE is set."""
    mocker.patch.dict(os.environ, {"PUDL_SKIP_FERC_SQLITE_PROVENANCE": "true"})
    mock_instance = mocker.MagicMock()
    mock_warn = mocker.patch("pudl.dagster.provenance.logger.warning")

    assert_ferc_sqlite_compatible(
        instance=mock_instance,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )

    assert mock_warn.call_count == 1
    assert "PUDL_SKIP_FERC_SQLITE_PROVENANCE" in mock_warn.call_args[0][0]
    mock_instance.get_latest_materialization_event.assert_not_called()


@pytest.mark.parametrize("truthy_value", ["1", "true", "yes", "TRUE", "YES"])
def test_assert_ferc_sqlite_compatible_env_var_truthy_values(
    truthy_value: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """All recognised truthy env var values skip the check."""
    mocker.patch.dict(os.environ, {"PUDL_SKIP_FERC_SQLITE_PROVENANCE": truthy_value})
    mock_instance = mocker.MagicMock()
    # Should not raise.
    assert_ferc_sqlite_compatible(
        instance=mock_instance,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    mock_instance.get_latest_materialization_event.assert_not_called()


def test_assert_ferc_sqlite_compatible_passes_matching_provenance(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Compatible provenance should not raise."""
    dagster_meta = build_ferc_sqlite_provenance_metadata(
        dataset="ferc1",
        data_format="dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="complete",
    ).to_dagster_metadata()
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=dagster_meta)
    )
    # Should not raise.
    assert_ferc_sqlite_compatible(
        instance=instance,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )


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
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
    stored_years: list[int],
    required_years: list[int],
    should_raise: bool,
) -> None:
    """Compatibility requires stored years to be a superset of required years.

    Passes when stored ⊇ required; raises RuntimeError when any required year is absent.
    """
    from pudl.settings import Ferc1DbfToSqliteSettings

    stored_settings = EtlSettings(
        ferc_to_sqlite_settings=etl_settings.ferc_to_sqlite.model_copy(
            update={
                "ferc1_dbf_to_sqlite_settings": Ferc1DbfToSqliteSettings(
                    years=stored_years
                )
            }
        )
    )
    required_settings = EtlSettings(
        ferc_to_sqlite_settings=etl_settings.ferc_to_sqlite.model_copy(
            update={
                "ferc1_dbf_to_sqlite_settings": Ferc1DbfToSqliteSettings(
                    years=required_years
                )
            }
        )
    )
    stored_dagster_meta = build_ferc_sqlite_provenance_metadata(
        dataset="ferc1",
        data_format="dbf",
        etl_settings=stored_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="complete",
    ).to_dagster_metadata()
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_dagster_meta)
    )
    if should_raise:
        with pytest.raises(RuntimeError, match="missing required years"):
            assert_ferc_sqlite_compatible(
                instance=instance,
                db_name="ferc1_dbf",
                etl_settings=required_settings,
                zenodo_dois=zenodo_dois,
            )
    else:
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=required_settings,
            zenodo_dois=zenodo_dois,
        )


def test_assert_ferc_sqlite_compatible_rejects_doi_mismatch(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """A Zenodo DOI mismatch should raise a descriptive RuntimeError."""
    stale_dois = ZenodoDoiSettings(ferc1="10.5281/zenodo.9999999")
    dagster_meta = build_ferc_sqlite_provenance_metadata(
        dataset="ferc1",
        data_format="dbf",
        etl_settings=etl_settings,
        zenodo_dois=stale_dois,
        sqlite_path=None,
        status="complete",
    ).to_dagster_metadata()
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=dagster_meta)
    )
    with pytest.raises(RuntimeError, match="Zenodo DOI mismatch"):
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=etl_settings,
            zenodo_dois=zenodo_dois,
        )


def test_assert_ferc_sqlite_compatible_rejects_missing_materialization(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Missing materialization event should raise a descriptive RuntimeError."""
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = None
    with pytest.raises(RuntimeError, match="No Dagster provenance metadata"):
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=etl_settings,
            zenodo_dois=zenodo_dois,
        )


@pytest.mark.parametrize(
    ("status", "expected_match"),
    [
        ("skipped", "status="),
        ("not_configured", "not_configured"),
    ],
)
def test_assert_ferc_sqlite_compatible_rejects_non_complete_status(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
    status: str,
    expected_match: str,
) -> None:
    """A DB materialized with a non-complete status should raise RuntimeError.

    Both 'skipped' and 'not_configured' mean the SQLite file was never fully
    populated, so downstream IO managers must refuse to read from it.
    """
    dagster_meta = FercSQLiteProvenanceRecord(
        dataset="ferc1",
        status=status,
    ).to_dagster_metadata()
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=dagster_meta)
    )
    with pytest.raises(RuntimeError, match=expected_match):
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=etl_settings,
            zenodo_dois=zenodo_dois,
        )
