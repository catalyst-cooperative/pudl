from dagster import load_asset_checks_from_modules

from pudl.etl import dbt_asset_checks


def test_asset_checks_exist():
    loaded_checks = load_asset_checks_from_modules([dbt_asset_checks])
    assert loaded_checks != []
