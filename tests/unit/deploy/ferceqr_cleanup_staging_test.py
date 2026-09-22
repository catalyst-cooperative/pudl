"""Unit tests for ``builds/ferceqr_cleanup_staging.py``."""

import importlib.util
from pathlib import Path

import pytest
import yaml

_SCRIPT = Path(__file__).parents[3] / "builds" / "ferceqr_cleanup_staging.py"
_spec = importlib.util.spec_from_file_location("ferceqr_cleanup_staging", _SCRIPT)
assert _spec is not None and _spec.loader is not None
cleanup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cleanup)


@pytest.fixture
def config_file(tmp_path):
    """Write a deployment config pointing at ``<tmp>/dist/ferceqr`` and return it."""
    path = tmp_path / "targets.yml"
    path.write_text(
        yaml.safe_dump(
            {"deployment_targets": [{"path": str(tmp_path / "dist" / "ferceqr")}]}
        ),
        encoding="utf-8",
    )
    return path


def test_scan_roots_covers_target_and_its_parent(config_file, tmp_path):
    roots = {str(r) for r in cleanup._staging_scan_roots(str(config_file))}
    assert roots == {
        str(tmp_path / "dist" / "ferceqr"),
        str(tmp_path / "dist"),
    }


def test_removes_staging_dirs_beside_and_under_target_but_spares_the_rest(
    config_file, tmp_path
):
    dist = tmp_path / "dist"
    (dist / "._staging_sibling" / "data").mkdir(parents=True)
    (dist / "ferceqr" / "._staging_child").mkdir(parents=True)
    (dist / "._ferceqr_previous" / "core_ferceqr__contracts").mkdir(parents=True)
    (dist / "ferceqr" / "core_ferceqr__contracts").mkdir(parents=True)

    removed = cleanup._remove_staging_dirs(
        cleanup._staging_scan_roots(str(config_file))
    )

    assert removed == 2
    assert not (dist / "._staging_sibling").exists()
    assert not (dist / "ferceqr" / "._staging_child").exists()
    assert (dist / "._ferceqr_previous").exists()
    assert (dist / "ferceqr" / "core_ferceqr__contracts").exists()


def test_missing_roots_are_skipped(config_file):
    # Nothing on disk: scanning non-existent roots must not raise.
    assert (
        cleanup._remove_staging_dirs(cleanup._staging_scan_roots(str(config_file))) == 0
    )
