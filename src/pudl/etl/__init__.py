"""Dagster definitions for the PUDL ETL and Output tables."""
import importlib.resources
import itertools
import warnings

import pandera as pr
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    Definitions,
    ExperimentalWarning,
    SourceAsset,
    asset_check,
    define_asset_job,
    load_assets_from_modules,
)
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition

import pudl
from pudl.io_managers import (
    epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_mixed_format_io_manager,
)
from pudl.resources import dataset_settings, datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings

from . import (
    check_foreign_keys,
    cli,
    eia_bulk_elec_assets,
    epacems_assets,
    glue_assets,
    static_assets,
)

logger = pudl.logging_helpers.get_logger(__name__)


default_assets = (
    *load_assets_from_modules([eia_bulk_elec_assets], group_name="core_eia_bulk_elec"),
    *load_assets_from_modules([epacems_assets], group_name="core_epacems"),
    *load_assets_from_modules([pudl.extract.eia176], group_name="raw_eia176"),
    *load_assets_from_modules([pudl.extract.phmsagas], group_name="raw_phmsagas"),
    *load_assets_from_modules([pudl.extract.eia860m], group_name="raw_eia860m"),
    *load_assets_from_modules([pudl.extract.eia860], group_name="raw_eia860"),
    *load_assets_from_modules([pudl.transform.eia860], group_name="_core_eia860"),
    *load_assets_from_modules([pudl.transform.eia860m], group_name="core_eia860m"),
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
    *load_assets_from_modules(
        [pudl.output.ferc714], group_name="out_respondents_ferc714"
    ),
    *load_assets_from_modules(
        [pudl.convert.censusdp1tract_to_sqlite, pudl.output.censusdp1tract],
        group_name="core_censusdp1tract",
    ),
    *load_assets_from_modules([glue_assets], group_name="core_assn"),
    *load_assets_from_modules([static_assets], group_name="core_codes"),
    *load_assets_from_modules(
        [
            pudl.output.eia,
            pudl.output.eia860,
            pudl.output.eia923,
            pudl.output.eia_bulk_elec,
        ],
        group_name="out_eia",
    ),
    *load_assets_from_modules(
        [pudl.analysis.allocate_gen_fuel], group_name="out_allocate_gen_fuel"
    ),
    *load_assets_from_modules(
        [pudl.analysis.mcoe], group_name="out_derived_gen_attributes"
    ),
    *load_assets_from_modules([pudl.output.ferc1], group_name="out_ferc1"),
    *load_assets_from_modules(
        [pudl.analysis.service_territory], group_name="out_service_territory_eia861"
    ),
    *load_assets_from_modules(
        [pudl.analysis.state_demand], group_name="out_state_demand_ferc714"
    ),
    *load_assets_from_modules(
        [pudl.analysis.record_linkage.classify_plants_ferc1],
        group_name="out_ferc1",
    ),
    *load_assets_from_modules(
        [
            pudl.analysis.plant_parts_eia,
            pudl.analysis.record_linkage.eia_ferc1_record_linkage,
        ],
        group_name="eia_ferc1_record_linkage",
    ),
)


def asset_check_from_schema(
    asset_key: AssetKey,
    package: pudl.metadata.classes.Package,
) -> AssetChecksDefinition | None:
    """Create a dagster asset check based on the resource schema, if defined."""
    resource_id = asset_key.to_user_string()
    try:
        resource = package.get_resource(resource_id)
    except ValueError:
        return None
    pandera_schema = resource.schema.to_pandera()

    @asset_check(asset=asset_key)
    def pandera_schema_check(asset_value) -> AssetCheckResult:
        try:
            pandera_schema.validate(asset_value, lazy=True)
        except pr.errors.SchemaErrors as schema_errors:
            return AssetCheckResult(
                passed=False,
                metadata={
                    "errors": [
                        {
                            "failure_cases": str(err.failure_cases),
                            "data": str(err.data),
                        }
                        for err in schema_errors.schema_errors
                    ],
                },
            )
        return AssetCheckResult(passed=True)

    return pandera_schema_check


def _get_keys_from_assets(
    asset_def: AssetsDefinition | SourceAsset | CacheableAssetsDefinition,
) -> list[AssetKey]:
    """Get a list of asset keys.

    Most assets have one key, which can be retrieved as a list from
    ``asset.keys``.

    Multi-assets have multiple keys, which can also be retrieved as a list from
    ``asset.keys``.

    SourceAssets always only have one key, and don't have ``asset.keys``. So we
    look for ``asset.key`` and wrap it in a list.

    We don't handle CacheableAssetsDefinitions yet.
    """
    if isinstance(asset_def, AssetsDefinition):
        return list(asset_def.keys)
    if isinstance(asset_def, SourceAsset):
        return [asset_def.key]
    return []


# Asset Checks are still Experimental, silence the warning since we use them
# everywhere.
warnings.filterwarnings("ignore", category=ExperimentalWarning)
_package = pudl.metadata.classes.Package.from_resource_ids()
_asset_keys = itertools.chain.from_iterable(
    _get_keys_from_assets(asset_def) for asset_def in default_assets
)
default_asset_checks = [
    check
    for check in (
        asset_check_from_schema(asset_key, _package)
        for asset_key in _asset_keys
        if asset_key.to_user_string() != "core_epacems__hourly_emissions"
    )
    if check is not None
]

default_resources = {
    "datastore": datastore,
    "pudl_io_manager": pudl_mixed_format_io_manager,
    "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
    "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
    "dataset_settings": dataset_settings,
    "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
    "epacems_io_manager": epacems_io_manager,
}

# By default, limit CEMS year processing concurrency to prevent memory overload.
default_tag_concurrency_limits = [
    {
        "key": "datasource",
        "value": "epacems",
        "limit": 2,
    }
]
default_config = pudl.helpers.get_dagster_execution_config(
    tag_concurrency_limits=default_tag_concurrency_limits
)
default_config |= pudl.analysis.ml_tools.get_ml_models_config()


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
    ).datasets.model_dump()

    return dataset_settings


defs: Definitions = Definitions(
    assets=default_assets,
    asset_checks=default_asset_checks,
    resources=default_resources,
    jobs=[
        define_asset_job(
            name="etl_full",
            description="This job executes all years of all assets.",
            config=default_config,
        ),
        define_asset_job(
            name="etl_full_no_cems",
            selection=create_non_cems_selection(default_assets),
            description="This job executes all years of all assets except the "
            "core_epacems__hourly_emissions asset and all assets downstream.",
        ),
        define_asset_job(
            name="etl_fast",
            config=default_config
            | {
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

warnings.resetwarnings()

"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""
