"""Integration tests for IO manager behaviors that depend on migrations."""

from pathlib import Path

import alembic.config

from pudl.io_managers import PudlSQLiteIOManager
from pudl.metadata.classes import PUDL_PACKAGE


def test_migrations_match_metadata(tmp_path, monkeypatch) -> None:
    """If you create a `PudlSQLiteIOManager` that points at a non-existing
    `pudl.sqlite` - it will initialize the DB based on the `package`.

    If you create a `PudlSQLiteIOManager` that points at an existing
    `pudl.sqlite`, like one initialized via `alembic upgrade head`, it
    will compare the existing db schema with the db schema in `package`.

    We want to make sure that the schema defined in `package` is the same as
    the one we arrive at by applying all the migrations.
    """
    monkeypatch.chdir(Path(__file__).parent.parent.parent)
    monkeypatch.setenv("PUDL_OUTPUT", str(tmp_path))
    alembic.config.main(["upgrade", "head"])

    PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=PUDL_PACKAGE)

    assert True
