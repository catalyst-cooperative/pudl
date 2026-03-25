"""Unit tests for the FERC SQLite provenance helpers."""

from pathlib import Path

import dagster as dg
import pytest

from pudl.ferc_sqlite_provenance import (
    FercSqliteProvenance,
    assert_ferc_sqlite_compatible,
    build_ferc_sqlite_provenance_metadata,
    get_ferc_sqlite_provenance,
)
from pudl.settings import EtlSettings, FercToSqliteSettings
from pudl.workspace.datastore import ZenodoDoiSettings


@pytest.fixture()
def etl_settings() -> EtlSettings:
    """Minimal ETL settings with FERC-to-SQLite config for provenance tests."""
    return EtlSettings(ferc_to_sqlite_settings=FercToSqliteSettings())


@pytest.fixture()
def zenodo_dois() -> ZenodoDoiSettings:
    """Default Zenodo DOI settings."""
    return ZenodoDoiSettings()


@pytest.mark.parametrize(
    ("db_name", "expected_dataset", "expected_format"),
    [
        ("ferc1_dbf", "ferc1", "dbf"),
        ("ferc1_xbrl", "ferc1", "xbrl"),
        ("ferc714_xbrl", "ferc714", "xbrl"),
        ("ferc2_dbf", "ferc2", "dbf"),
    ],
)
def test_get_ferc_sqlite_provenance_dataset_and_format(
    db_name: str,
    expected_dataset: str,
    expected_format: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Provenance fingerprint extracts dataset and format from the db_name."""
    provenance = get_ferc_sqlite_provenance(
        db_name=db_name,
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert isinstance(provenance, FercSqliteProvenance)
    assert provenance.dataset == expected_dataset
    assert provenance.data_format == expected_format
    assert provenance.asset_key == dg.AssetKey(f"raw_{db_name}__sqlite")


def test_get_ferc_sqlite_provenance_settings_hash_is_stable(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """The same settings should always produce the same hash."""
    p1 = get_ferc_sqlite_provenance(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    p2 = get_ferc_sqlite_provenance(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert p1.settings_hash == p2.settings_hash


def test_build_ferc_sqlite_provenance_metadata_keys(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Metadata dict should contain all required provenance keys."""
    metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=Path("test-data/ferc1_dbf.sqlite"),
        status="complete",
    )
    required_keys = {
        "pudl_ferc_sqlite_dataset",
        "pudl_ferc_sqlite_status",
        "pudl_ferc_sqlite_zenodo_doi",
        "pudl_ferc_sqlite_etl_settings",
        "pudl_ferc_sqlite_etl_settings_hash",
        "pudl_ferc_sqlite_path",
    }
    assert required_keys <= set(metadata.keys())


def test_assert_ferc_sqlite_compatible_skips_without_instance(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Provenance check should be a no-op when no Dagster instance is available."""
    # Should not raise even though no instance is available.
    assert_ferc_sqlite_compatible(
        instance=None,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )


def test_assert_ferc_sqlite_compatible_passes_matching_provenance(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Compatible provenance should not raise."""
    metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="complete",
    )
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=metadata)
    )
    # Should not raise.
    assert_ferc_sqlite_compatible(
        instance=instance,
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )


def test_assert_ferc_sqlite_compatible_rejects_doi_mismatch(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """A Zenodo DOI mismatch should raise a descriptive RuntimeError."""
    stale_dois = ZenodoDoiSettings(ferc1="10.5281/zenodo.9999999")
    metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=stale_dois,
        sqlite_path=None,
        status="complete",
    )
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=metadata)
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


def test_assert_ferc_sqlite_compatible_rejects_incomplete_status(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """A non-'complete' status should raise a descriptive RuntimeError."""
    metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="skipped",
    )
    instance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=metadata)
    )
    with pytest.raises(RuntimeError, match="status="):
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=etl_settings,
            zenodo_dois=zenodo_dois,
        )


@pytest.mark.parametrize(
    "db_name",
    [
        "not_a_ferc_db",
        "ferc1",  # missing _dbf or _xbrl suffix
    ],
)
def test_get_ferc_sqlite_provenance_rejects_bad_db_name(
    db_name: str,
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """Malformed db_names should raise ValueError."""
    with pytest.raises(ValueError):
        get_ferc_sqlite_provenance(
            db_name=db_name,
            etl_settings=etl_settings,
            zenodo_dois=zenodo_dois,
        )
