"""Test the PUDL console scripts from within PyTest."""

import os
from pathlib import Path

import geopandas as gpd
import pytest
import sqlalchemy as sa


@pytest.mark.parametrize(
    "command",
    [
        # Force the download of one small partition from Zenodo
        "pudl_datastore --dataset ferc60 --bypass-local-cache --partition year=2020",
        # Force the download of one small partition from GCS
        "pudl_datastore -d ferc60 --bypass-local-cache --gcs-cache-path gs://zenodo-cache.catalyst.coop --partition year=2020",
        # Exercise the datastore validation code
        "pudl_datastore -d ferc60 --validate --partition year=2020",
        # Ensure that all data source partitions are legible
        "pudl_datastore --list-partitions --gcs-cache-path gs://zenodo-cache.catalyst.coop",
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_datastore(script_runner, command: str):
    """CLI tests specific to the pudl_datastore script."""
    runner_args = command.split(" ")
    ret = script_runner.run(runner_args, print_result=True)
    assert ret.success


@pytest.mark.parametrize(
    "command,filename,expected_cols",
    [
        (
            "pudl_service_territories --entity-type balancing_authority -y 2022 --limit-by-state --no-dissolve -o ",
            "balancing_authority_geometry_limited.parquet",
            {
                "area_km2",
                "balancing_authority_id_eia",
                "county",
                "county_id_fips",
                "county_name_census",
                "geometry",
                "population",
                "report_date",
                "state",
                "state_id_fips",
            },
        ),
        (
            "pudl_service_territories --entity-type balancing_authority -y 2021 -y 2022 --no-dissolve -o ",
            "balancing_authority_geometry.parquet",
            {
                "area_km2",
                "balancing_authority_id_eia",
                "county",
                "county_id_fips",
                "county_name_census",
                "geometry",
                "population",
                "report_date",
                "state",
                "state_id_fips",
            },
        ),
        (
            "pudl_service_territories --entity-type utility -y 2022 --dissolve -o ",
            "utility_geometry_dissolved.parquet",
            {
                "area_km2",
                "geometry",
                "population",
                "report_date",
                "utility_id_eia",
            },
        ),
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_service_territories(
    script_runner,
    command: str,
    tmp_path: Path,
    filename: str,
    expected_cols: set[str],
    pudl_engine: sa.Engine,
):
    """CLI tests specific to the pudl_service_territories script.

    Depends on the ``pudl_engine`` fixture to ensure that the censusdp1tract.sqlite
    database has been generated, since that data is required for the script to run.
    """
    out_path = tmp_path / filename
    assert not out_path.exists()
    command += str(tmp_path)
    ret = script_runner.run(command.split(" "), print_result=True)
    assert ret.success
    assert out_path.exists()
    assert out_path.is_file()
    gdf = gpd.read_parquet(out_path)
    assert set(gdf.columns) == expected_cols
    assert not gdf.empty


@pytest.mark.parametrize(
    "resource_id",
    [
        # One migrated table
        "core_epacems__hourly_emissions",
        # One unmigrated table (TODO: drop after migration)
        "_core_eia860__cooling_equipment",
        # One nonexistent table
        "imaginary_resource",
    ],
)
@pytest.mark.script_launch_mode("inprocess")
def test_resource_description(script_runner, resource_id: str):
    """CLI tests specific to the resource_description script."""
    ret = script_runner.run(
        ["resource_description", "-n", resource_id], print_result=True
    )
    assert ret.success


@pytest.mark.script_launch_mode("inprocess")
@pytest.mark.skipif(
    "ZENODO_SANDBOX_TOKEN_PUBLISH" not in os.environ,
    reason="Zenodo sandbox token required to exercise uploads",
)
def test_zenodo_data_release(script_runner, tmp_path: Path):
    """Round-trip upload a tiny release directory to the Zenodo sandbox.

    The CLI accepts any fsspec-compatible path. Using ``Path.as_uri()`` yields a
    ``file://`` prefix so the integration test exercises that codepath while
    keeping all artifacts confined to pytest's temporary directory management.
    """

    source_dir = tmp_path / "release"
    source_dir.mkdir()
    (source_dir / "README.txt").write_text("Test release contents", encoding="utf-8")
    (source_dir / "data.csv").write_text("value\n1\n", encoding="utf-8")
    (source_dir / "metadata.json").write_text("{}\n", encoding="utf-8")

    result = script_runner.run(
        [
            "zenodo_data_release",
            "--env",
            "sandbox",
            "--source-dir",
            source_dir.as_uri(),
            "--publish",
        ],
        print_result=True,
    )

    assert result.success
