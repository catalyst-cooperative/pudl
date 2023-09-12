"""Dagster definitions for the PUDL ETL and Output tables."""
import importlib.resources

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

import pudl
from pudl.io_managers import (
    epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.resources import dataset_settings, datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings

from . import glue_assets  # noqa: F401
from . import eia_bulk_elec_assets, epacems_assets, static_assets

logger = pudl.logging_helpers.get_logger(__name__)

default_assets = (
    *load_assets_from_modules([eia_bulk_elec_assets], group_name="eia_bulk_elec"),
    *load_assets_from_modules([epacems_assets], group_name="core_epacems"),
    *load_assets_from_modules([pudl.extract.eia860], group_name="raw_eia860"),
    *load_assets_from_modules([pudl.transform.eia860], group_name="_core_eia860"),
    *load_assets_from_modules([pudl.extract.eia861], group_name="raw_eia861"),
    *load_assets_from_modules(
        [pudl.transform.eia861], group_name="core_eia861"
    ),  # TODO: move one _core asset to separate module?
    *load_assets_from_modules([pudl.extract.eia923], group_name="raw_eia923"),
    *load_assets_from_modules([pudl.transform.eia923], group_name="_core_eia923"),
    *load_assets_from_modules([pudl.transform.eia], group_name="core_eia"),
    *load_assets_from_modules([pudl.extract.ferc1], group_name="raw_ferc1"),
    *load_assets_from_modules([pudl.transform.ferc1], group_name="core_ferc1"),
    *load_assets_from_modules([pudl.extract.ferc714], group_name="raw_ferc714"),
    *load_assets_from_modules([pudl.transform.ferc714], group_name="core_ferc714"),
    *load_assets_from_modules([pudl.output.ferc714], group_name="respondents_ferc714"),
    *load_assets_from_modules(
        [pudl.convert.censusdp1tract_to_sqlite, pudl.output.censusdp1tract],
        group_name="censusdp1",
    ),
    *load_assets_from_modules([glue_assets], group_name="glue"),
    *load_assets_from_modules([static_assets], group_name="static"),
    *load_assets_from_modules(
        [
            pudl.output.eia,
            pudl.output.eia860,
            pudl.output.eia923,
            pudl.output.eia_bulk_elec,
        ],
        group_name="denorm_eia",
    ),
    *load_assets_from_modules(
        [pudl.analysis.allocate_gen_fuel], group_name="allocate_gen_fuel"
    ),
    *load_assets_from_modules([pudl.analysis.mcoe], group_name="mcoe"),
    *load_assets_from_modules([pudl.output.ferc1], group_name="denorm_ferc1"),
    *load_assets_from_modules(
        [pudl.analysis.service_territory], group_name="service_territory_eia861"
    ),
    *load_assets_from_modules(
        [pudl.analysis.state_demand], group_name="state_demand_ferc714"
    ),
)

default_resources = {
    "datastore": datastore,
    "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
    "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
    "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
    "dataset_settings": dataset_settings,
    "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
    "epacems_io_manager": epacems_io_manager,
}


def create_non_cems_selection(all_assets: list[AssetsDefinition]) -> AssetSelection:
    """Create a selection of assets excluding CEMS and all downstream assets.

    Args:
        all_assets: A list of asset definitions to remove CEMS assets from.

    Returns:
        An asset selection with all_assets assets excluding CEMS assets.
    """
    all_asset_keys = pudl.helpers.get_asset_keys(all_assets)
    all_selection = AssetSelection.keys(*all_asset_keys)

    cems_selection = AssetSelection.keys(AssetKey("core_epacems__hourly_emissions"))
    return all_selection - cems_selection.downstream()


def load_dataset_settings_from_file(setting_filename: str) -> dict:
    """Load dataset settings from a settings file in `pudl.package_data.settings`.

    Args:
        setting_filename: name of settings file.

    Returns:
        Dictionary of dataset settings.
    """
    dataset_settings = EtlSettings.from_yaml(
        importlib.resources.files("pudl.package_data.settings")
        / f"{setting_filename}.yml"
    ).datasets.dict()

    return dataset_settings


defs: Definitions = Definitions(
    assets=default_assets,
    resources=default_resources,
    jobs=[
        define_asset_job(
            name="etl_full", description="This job executes all years of all assets."
        ),
        define_asset_job(
            name="etl_full_no_cems",
            selection=create_non_cems_selection(default_assets),
            description="This job executes all years of all assets except the "
            "core_epacems__hourly_emissions asset and all assets downstream.",
        ),
        define_asset_job(
            name="etl_fast",
            config={
                "resources": {
                    "dataset_settings": {
                        "config": load_dataset_settings_from_file("etl_fast")
                    }
                }
            },
            description="This job executes the most recent year of each asset.",
        ),
        define_asset_job(
            name="etl_fast_no_cems",
            selection=create_non_cems_selection(default_assets),
            config={
                "resources": {
                    "dataset_settings": {
                        "config": load_dataset_settings_from_file("etl_fast")
                    }
                }
            },
            description="This job executes the most recent year of each asset except the "
            "core_epacems__hourly_emissions asset and all assets downstream.",
        ),
    ],
)
"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""
