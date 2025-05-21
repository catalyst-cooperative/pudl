from dagster import load_assets_from_modules

from pudl.etl import dbt_asset_checks


def test_asset_checks_exist():
    loaded_checks = load_assets_from_modules([dbt_asset_checks])[0].check_specs
    assert loaded_checks != []
