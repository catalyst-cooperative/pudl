"""Dagster definitions for the PUDL ETL and Output tables."""

import importlib.resources
import itertools

import pandera as pr
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Definitions,
    asset_check,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition

import pudl
from pudl.io_managers import (
    epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    ferc714_xbrl_sqlite_io_manager,
    parquet_io_manager,
    pudl_mixed_format_io_manager,
)
from pudl.metadata import PUDL_PACKAGE
from pudl.resources import dataset_settings, datastore, ferc_to_sqlite_settings
from pudl.settings import EtlSettings

from . import (
    eia_bulk_elec_assets,
    epacems_assets,
    glue_assets,
    static_assets,
)

logger = pudl.logging_helpers.get_logger(__name__)

raw_module_groups = {
    "raw_censuspep": [pudl.extract.censuspep],
    "raw_eia176": [pudl.extract.eia176],
    "raw_eia191": [pudl.extract.eia191],
    "raw_eia757a": [pudl.extract.eia757a],
    "raw_eia860": [pudl.extract.eia860],
    "raw_eia860m": [pudl.extract.eia860m],
    "raw_eia861": [pudl.extract.eia861],
    "raw_eia923": [pudl.extract.eia923],
    "raw_eia930": [pudl.extract.eia930],
    "raw_eiaaeo": [pudl.extract.eiaaeo],
    "raw_ferc1": [pudl.extract.ferc1],
    "raw_ferc714": [pudl.extract.ferc714],
    "raw_gridpathratoolkit": [pudl.extract.gridpathratoolkit],
    "raw_nrelatb": [pudl.extract.nrelatb],
    "raw_phmsagas": [pudl.extract.phmsagas],
    "raw_sec10k": [pudl.extract.sec10k],
    "raw_vcerare": [pudl.extract.vcerare],
}


core_module_groups = {
    "core_assn": [glue_assets],
    "core_censusdp1tract": [
        pudl.convert.censusdp1tract_to_sqlite,
        pudl.output.censusdp1tract,
    ],
    "core_censuspep": [pudl.transform.censuspep],
    "core_codes": [static_assets],
    "core_eia": [pudl.transform.eia],
    "core_eiaaeo": [pudl.transform.eiaaeo],
    "core_eia_bulk_elec": [eia_bulk_elec_assets],
    "core_eia176": [pudl.transform.eia176],
    "core_eia860": [pudl.transform.eia860, pudl.transform.eia860m],
    "core_eia861": [pudl.transform.eia861],
    "core_eia923": [pudl.transform.eia923],
    "core_eia930": [pudl.transform.eia930],
    "core_epacems": [epacems_assets],
    "core_ferc1": [pudl.transform.ferc1],
    "core_ferc714": [pudl.transform.ferc714],
    "core_gridpathratoolkit": [pudl.transform.gridpathratoolkit],
    "core_sec10k": [pudl.transform.sec10k],
    "core_nrelatb": [pudl.transform.nrelatb],
    "core_vcerare": [pudl.transform.vcerare],
}

out_module_groups = {
    "eia_ferc1_record_linkage": [
        pudl.analysis.plant_parts_eia,
        pudl.analysis.record_linkage.eia_ferc1_record_linkage,
    ],
    "out_allocate_gen_fuel": [pudl.analysis.allocate_gen_fuel],
    "out_derived_gen_attributes": [pudl.analysis.mcoe],
    "out_eia": [
        pudl.output.eia,
        pudl.output.eia860,
        pudl.output.eia923,
        pudl.output.eia930,
        pudl.output.eiaapi,
    ],
    "out_ferc1": [
        pudl.output.ferc1,
        pudl.analysis.record_linkage.classify_plants_ferc1,
    ],
    "out_respondents_ferc714": [pudl.output.ferc714],
    "out_sec10k": [pudl.output.sec10k],
    "out_service_territory_eia861": [pudl.analysis.service_territory],
    "out_state_demand_ferc714": [pudl.analysis.state_demand],
}

all_asset_modules = raw_module_groups | core_module_groups | out_module_groups
default_assets = list(
    itertools.chain.from_iterable(
        load_assets_from_modules(
            modules,
            group_name=group_name,
            include_specs=True,
        )
        for group_name, modules in all_asset_modules.items()
    )
)


default_asset_checks = list(
    itertools.chain.from_iterable(
        load_asset_checks_from_modules(
            modules,
        )
        for modules in all_asset_modules.values()
    )
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

    @asset_check(asset=asset_key, blocking=True)
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
    asset_def: AssetsDefinition | AssetSpec | CacheableAssetsDefinition,
) -> list[AssetKey]:
    """Get a list of asset keys.

    Most assets have one key, which can be retrieved as a list from
    ``asset.keys``.

    Multi-assets have multiple keys, which can also be retrieved as a list from
    ``asset.keys``.

    AssetSpecs always only have one key, and don't have ``asset.keys``. So we
    look for ``asset.key`` and wrap it in a list.

    We don't handle CacheableAssetsDefinitions yet.
    """
    if isinstance(asset_def, AssetsDefinition):
        return list(asset_def.keys)
    if isinstance(asset_def, AssetSpec):
        return [asset_def.key]
    return []


_package = PUDL_PACKAGE
_asset_keys = itertools.chain.from_iterable(
    _get_keys_from_assets(asset_def) for asset_def in default_assets
)
default_asset_checks += [
    check
    for check in (
        asset_check_from_schema(asset_key, _package)
        for asset_key in _asset_keys
        if (
            asset_key.to_user_string()
            not in [
                "core_epacems__hourly_emissions",
                "out_vcerare__hourly_available_capacity_factor",
            ]
        )
    )
    if check is not None
]

default_resources = {
    "datastore": datastore,
    "pudl_io_manager": pudl_mixed_format_io_manager,
    "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
    "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
    "ferc714_xbrl_sqlite_io_manager": ferc714_xbrl_sqlite_io_manager,
    "dataset_settings": dataset_settings,
    "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
    "epacems_io_manager": epacems_io_manager,
    "parquet_io_manager": parquet_io_manager,
}

# Limit the number of concurrent workers when launch assets that use a lot of memory.
default_tag_concurrency_limits = [
    {
        "key": "memory-use",
        "value": "high",
        "limit": 4,
    },
]
default_config = pudl.helpers.get_dagster_execution_config(
    tag_concurrency_limits=default_tag_concurrency_limits
)
default_config |= pudl.analysis.ml_tools.get_ml_models_config()


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
            config=default_config
            | {
                "resources": {
                    "dataset_settings": {
                        "config": load_dataset_settings_from_file("etl_full")
                    }
                }
            },
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
    ],
)

"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""
