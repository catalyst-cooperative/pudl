"""Tests for the packaged Dagster run config files."""

import yaml

from pudl import PUDL_PACKAGE_DATA_PATH

SETTINGS_DIR = PUDL_PACKAGE_DATA_PATH / "settings"


def _load(name: str) -> dict:
    return yaml.safe_load((SETTINGS_DIR / name).read_text())


def test_dg_unmapped_ids_matches_dg_full_without_ops():
    """dg_unmapped_ids.yml must be dg_full.yml minus its ``ops`` section.

    Dagster rejects config for ops that aren't in the job being launched, so the
    ``unmapped_ids`` job can't use dg_full.yml directly. Keep the two in sync.
    """
    full = _load("dg_full.yml")
    unmapped_ids = _load("dg_unmapped_ids.yml")
    assert "ops" in full
    assert "ops" not in unmapped_ids
    assert unmapped_ids == {k: v for k, v in full.items() if k != "ops"}
