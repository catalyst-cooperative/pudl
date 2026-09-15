"""Test the PUDL console scripts from within PyTest."""

from pathlib import Path

import geopandas as gpd  # noqa: ICN002
import pytest


@pytest.mark.parametrize(
    "command,filename,expected_cols",
    [
        (
            "pudl_service_territories --entity-type balancing_authority -y 2025 --limit-by-state --no-dissolve -o ",
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
            "pudl_service_territories --entity-type balancing_authority -y 2020 -y 2025 --no-dissolve -o ",
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
            "pudl_service_territories --entity-type utility -y 2025 --dissolve -o ",
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
@pytest.mark.usefixtures("prebuilt_outputs")
def test_pudl_service_territories(
    script_runner,
    command: str,
    tmp_path: Path,
    filename: str,
    expected_cols: set[str],
):
    """CLI tests specific to the pudl_service_territories script."""
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
