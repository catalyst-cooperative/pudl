"""Dagster asset loading for PUDL."""

import itertools
import os

import dagster as dg

import pudl
from pudl.deploy import ferceqr
from pudl.etl import (
    eia_bulk_elec_assets,
    ferc_to_sqlite_assets,
    glue_assets,
    static_assets,
)

raw_module_groups = {
    "raw_ferc_to_sqlite": [ferc_to_sqlite_assets],
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
    "raw_ferccid": [pudl.extract.ferccid],
    "raw_gridpathratoolkit": [pudl.extract.gridpathratoolkit],
    "raw_nrelatb": [pudl.extract.nrelatb],
    "raw_phmsagas": [pudl.extract.phmsagas],
    "raw_rus7": [pudl.extract.rus7],
    "raw_sec10k": [pudl.extract.sec10k],
    "raw_vcerare": [pudl.extract.vcerare],
    "raw_ferceqr": [pudl.extract.ferceqr],
    "raw_rus12": [pudl.extract.rus12],
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
    "core_epacems": [pudl.transform.epacems],
    "core_ferc1": [pudl.transform.ferc1],
    "core_ferc714": [pudl.transform.ferc714],
    "core_ferccid": [pudl.transform.ferccid],
    "core_ferceqr": [pudl.transform.ferceqr],
    "core_gridpathratoolkit": [pudl.transform.gridpathratoolkit],
    "core_sec10k": [pudl.transform.sec10k],
    "core_nrelatb": [pudl.transform.nrelatb],
    "core_vcerare": [pudl.transform.vcerare],
    "core_phmsagas": [pudl.transform.phmsagas],
    "core_rus7": [pudl.transform.rus7],
    "core_rus12": [pudl.transform.rus12],
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
    "out_rus": [pudl.output.rus],
    "out_sec10k": [pudl.output.sec10k],
    "out_service_territory_eia861": [pudl.analysis.service_territory],
    "out_state_demand_ferc714": [pudl.analysis.state_demand],
}

ferceqr_deployment_assets = (
    {"ferceqr_deployment": [ferceqr]} if os.getenv("FERCEQR_BUILD", None) else {}
)

all_asset_modules = (
    raw_module_groups
    | core_module_groups
    | out_module_groups
    | ferceqr_deployment_assets
)

default_assets = list(
    itertools.chain.from_iterable(
        dg.load_assets_from_modules(
            modules,
            group_name=group_name,
            include_specs=True,
        )
        for group_name, modules in all_asset_modules.items()
    )
)


def get_keys_from_assets(
    asset_def: dg.AssetsDefinition | dg.AssetSpec,
) -> list[dg.AssetKey]:
    """Get a list of asset keys for an asset definition or spec."""
    if isinstance(asset_def, dg.AssetsDefinition):
        return list(asset_def.keys)
    if isinstance(asset_def, dg.AssetSpec):
        return [asset_def.key]
    return []


asset_keys = list(
    itertools.chain.from_iterable(
        get_keys_from_assets(asset_def) for asset_def in default_assets
    )
)


__all__ = [
    "all_asset_modules",
    "asset_keys",
    "core_module_groups",
    "default_assets",
    "ferceqr_deployment_assets",
    "get_keys_from_assets",
    "out_module_groups",
    "raw_module_groups",
]
