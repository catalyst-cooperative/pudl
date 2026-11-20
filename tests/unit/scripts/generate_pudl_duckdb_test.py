"""Unit tests for the per-datapackage DuckDB views script."""

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from click.testing import CliRunner

from pudl.metadata.classes import DATAPACKAGES, Package, find_resource
from pudl.scripts import generate_pudl_duckdb
from pudl.scripts.generate_pudl_duckdb import _view_location, create_views, main

SINGLE_FILE = "core_pudl__codes_subdivisions"
PARTITIONED = "core_ferceqr__contracts"


def _write_parquet(path: Path, n_rows: int) -> None:
    """Write a small Parquet file; the views just ``SELECT *`` so contents don't matter."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.sql(
            f"COPY (SELECT range AS x FROM range({n_rows})) TO '{path}' (FORMAT parquet)"  # noqa: S608
        )


@pytest.mark.parametrize(
    ("name", "expected_location", "expected_exists"),
    [
        (SINGLE_FILE, "/base/core_pudl__codes_subdivisions.parquet", None),
        (
            PARTITIONED,
            "/base/core_ferceqr__contracts/*.parquet",
            "/base/core_ferceqr__contracts/",
        ),
    ],
)
def test_view_location_follows_the_resources_own_layout(
    name: str, expected_location: str, expected_exists: str | None
) -> None:
    """A directory path is read as a glob; a bare file name is read as one file."""
    location, must_exist = _view_location(find_resource(name), "/base")

    assert location == expected_location
    assert must_exist == (expected_exists or expected_location)


def test_create_views_handles_a_package_mixing_both_layouts(tmp_path: Path) -> None:
    """One package can hold single-file and partitioned resources side by side."""
    mixed = Package.from_resource_ids(
        resource_ids=(SINGLE_FILE, PARTITIONED, "core_pudl__codes_data_maturities"),
        name="mixed",
    )
    _write_parquet(tmp_path / f"{SINGLE_FILE}.parquet", n_rows=3)
    # Two partitions of the partitioned resource, at the paths its metadata declares.
    declared = find_resource(PARTITIONED)
    assert declared.extrapaths
    for partition_path in (declared.path, declared.extrapaths[0]):
        _write_parquet(tmp_path / partition_path, n_rows=5)
    # The third resource has no files at all and must simply be skipped.

    with duckdb.connect() as conn:
        created = create_views(
            conn,
            mixed,
            {"local": str(tmp_path), "published": str(tmp_path / "does_not_exist")},
        )
        n_single = conn.sql(f"SELECT count(*) FROM local.{SINGLE_FILE}").fetchall()  # noqa: S608
        n_partitioned = conn.sql(
            f"SELECT count(*) FROM local.{PARTITIONED}"  # noqa: S608
        ).fetchall()

    assert sorted(created) == sorted([f"local.{SINGLE_FILE}", f"local.{PARTITIONED}"])
    assert n_single == [(3,)]
    assert n_partitioned == [(10,)]


def test_main_writes_one_views_database_per_registered_package(
    mocker, tmp_path: Path
) -> None:
    """Every registered datapackage gets its own ``<name>_views.duckdb`` file."""
    paths = SimpleNamespace(
        parquet_path=lambda: tmp_path / "parquet",
        duckdb_path=lambda name: tmp_path / f"{name}.duckdb",
    )
    (tmp_path / "parquet").mkdir()
    mocker.patch("pudl.workspace.setup.PudlPaths", return_value=paths)
    # Never touch the network: only the local schema is created.
    mocker.patch.object(generate_pudl_duckdb, "REMOTE_BASE_PATHS", {})
    extra = Package.from_resource_ids(resource_ids=(SINGLE_FILE,), name="extra")
    mocker.patch.dict(DATAPACKAGES, {"extra": extra})

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    expected = {f"{name}_views.duckdb" for name in DATAPACKAGES}
    assert {p.name for p in tmp_path.glob("*.duckdb")} == expected
    assert {"pudl_views.duckdb", "ferceqr_views.duckdb", "extra_views.duckdb"} <= (
        expected
    )
