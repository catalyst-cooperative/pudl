"""Unit tests for the FERC SQLite provenance helpers."""

from pathlib import Path

import dagster as dg
import pytest

from pudl.dagster.provenance import (
    FercSQLiteProvenance,
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
    provenance: FercSQLiteProvenance = get_ferc_sqlite_provenance(
        db_name=db_name,
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert isinstance(provenance, FercSQLiteProvenance)
    assert provenance.dataset == expected_dataset
    assert provenance.data_format == expected_format
    assert provenance.asset_key == dg.AssetKey(f"raw_{db_name}__sqlite")


def test_get_ferc_sqlite_provenance_years_are_non_empty(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
) -> None:
    """The provenance fingerprint must include a non-empty, sorted list of years."""
    provenance = get_ferc_sqlite_provenance(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    )
    assert isinstance(provenance.years, list)
    assert len(provenance.years) > 0
    assert provenance.years == sorted(provenance.years)


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
    required_keys: set[str] = {
        "pudl_ferc_sqlite_dataset",
        "pudl_ferc_sqlite_status",
        "pudl_ferc_sqlite_zenodo_doi",
        "pudl_ferc_sqlite_etl_settings",
        "pudl_ferc_sqlite_years",
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
    instance: dg.DagsterInstance = mocker.MagicMock()
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


def test_assert_ferc_sqlite_compatible_passes_superset_years(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """A stored DB covering more years than required should be compatible."""
    # Build metadata as if the DB was built with the full settings (all years).
    stored_metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="complete",
    )
    instance: dg.DagsterInstance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_metadata)
    )

    # Downstream run requests only a single year — a strict subset of what is stored.
    stored_years: list[int] = get_ferc_sqlite_provenance(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
    ).years
    one_year: int = stored_years[len(stored_years) // 2]  # pick a year from the middle

    from pudl.settings import Ferc1DbfToSqliteSettings

    fast_settings = EtlSettings(
        ferc_to_sqlite_settings=etl_settings.ferc_to_sqlite.model_copy(
            update={
                "ferc1_dbf_to_sqlite_settings": Ferc1DbfToSqliteSettings(
                    years=[one_year]
                )
            }
        )
    )
    # Should not raise: stored years ⊇ required years.
    assert_ferc_sqlite_compatible(
        instance=instance,
        db_name="ferc1_dbf",
        etl_settings=fast_settings,
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
    instance: dg.DagsterInstance = mocker.MagicMock()
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


def test_assert_ferc_sqlite_compatible_rejects_missing_years(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Stored DB that lacks required years should raise a descriptive RuntimeError."""
    from pudl.settings import Ferc1DbfToSqliteSettings

    # DB was built with only year 2021.
    narrow_settings = EtlSettings(
        ferc_to_sqlite_settings=etl_settings.ferc_to_sqlite.model_copy(
            update={
                "ferc1_dbf_to_sqlite_settings": Ferc1DbfToSqliteSettings(years=[2021])
            }
        )
    )
    stored_metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=narrow_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="complete",
    )
    instance: dg.DagsterInstance = mocker.MagicMock()
    instance.get_latest_materialization_event.return_value = mocker.MagicMock(
        asset_materialization=mocker.MagicMock(metadata=stored_metadata)
    )
    # Downstream run requests all years — a strict superset of what is stored.
    with pytest.raises(RuntimeError, match="missing required years"):
        assert_ferc_sqlite_compatible(
            instance=instance,
            db_name="ferc1_dbf",
            etl_settings=etl_settings,  # full years
            zenodo_dois=zenodo_dois,
        )


def test_assert_ferc_sqlite_compatible_rejects_missing_materialization(
    etl_settings: EtlSettings,
    zenodo_dois: ZenodoDoiSettings,
    mocker,
) -> None:
    """Missing materialization event should raise a descriptive RuntimeError."""
    instance: dg.DagsterInstance = mocker.MagicMock()
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
    """A DB built with status='skipped' (e.g. years=[]) should raise RuntimeError.

    A skipped extraction means the SQLite file was never populated, so downstream
    IO managers must refuse to read from it.
    """
    metadata = build_ferc_sqlite_provenance_metadata(
        db_name="ferc1_dbf",
        etl_settings=etl_settings,
        zenodo_dois=zenodo_dois,
        sqlite_path=None,
        status="skipped",
    )
    instance: dg.DagsterInstance = mocker.MagicMock()
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
