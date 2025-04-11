"""Shim to associate DBT checks with the assets they check."""

import json
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    ResourceParam,
    asset_check,
)
from dagster_dbt import DbtCliResource

from pudl.settings import DatasetsSettings
from pudl.workspace.setup import PUDL_ROOT_DIR


def make_dbt_asset_checks(
    dagster_assets: list[AssetKey],
) -> list[AssetChecksDefinition]:
    """Associate dbt resources with dagster assets, then run the build at asset-check time.

    1. get the possible dbt resources to run
    2. get the dagster assets
    3. build checks for the intersection.
    """
    dbt_dir = PUDL_ROOT_DIR / "dbt"
    manifest_path = dbt_dir / "target" / "manifest.json"
    resources = __get_dbt_selection_names(manifest_path)

    dagster_asset_names = {da.to_user_string() for da in dagster_assets}
    assets = {
        short_name: select_id
        for select_id, short_name in resources
        if short_name in dagster_asset_names
    }
    return [
        __make_dbt_asset_check(
            dagster_asset_name=dagster_asset_name,
            dbt_resource_name=dbt_resource_name,
        )
        for dagster_asset_name, dbt_resource_name in assets.items()
    ]


def __make_dbt_asset_check(
    dagster_asset_name: str, dbt_resource_name: str
) -> AssetChecksDefinition:
    @asset_check(
        asset=dagster_asset_name,
        blocking=True,
    )
    def dbt_build_resource(
        dbt_cli: ResourceParam[DbtCliResource],
        dataset_settings: ResourceParam[DatasetsSettings],
    ) -> AssetCheckResult:
        """Build DBT resource, along with all attendant checks."""
        # default to etl-fast target if ad-hoc ETL.
        dbt_target = "etl-full" if dataset_settings.etl_type == "full" else "etl-fast"
        dbt_cli.cli(
            args=["seed", "--target", dbt_target, "--quiet"],
            raise_on_error=True,
            manifest=PUDL_ROOT_DIR / "dbt" / "target" / "manifest.json",
        ).wait()
        build_run = dbt_cli.cli(
            args=[
                "build",
                "--target",
                dbt_target,
                "--select",
                dbt_resource_name,
                "--store-failures",
            ],
            raise_on_error=False,
            manifest=PUDL_ROOT_DIR / "dbt" / "target" / "manifest.json",
        )
        build_success = build_run.wait().is_successful()
        return AssetCheckResult(
            passed=build_success,
        )

    return dbt_build_resource


def __get_dbt_selection_names(manifest_path: Path):
    """Associate dbt selection strings with dagster asset names.

    The strings that work with ``dbt build --select`` and the strings that
    correspond to dagster assets are different.

    In all cases, if the asset exists within dbt, the asset name corresponds to
    the final segment of the fully qualified name. You can find this in
    ``dbt/target/manifest.json`` as ``{nodes, sources}.[].[].name``.

    ``--select`` takes different strings depending on if it's a node or a source.

    For nodes, the ``name`` seems to work just fine.

    But for sources, we need ``source:pudl_dbt.pudl.*``. The closest thing in
    the manifest.json is the key ``source.pudl_dbt.pudl.*`` but we need to
    replace that period. That's stored in ``manifest.json`` as
    ``.sources.[].unique_id``.
    """
    # TODO 2025-04-04: return a list of namedtuple(select_id, short_name)
    with manifest_path.open() as f:
        manifest = json.load(f)

    nodes = [
        (res["name"], res["name"])
        for res in manifest["nodes"].values()
        if res["resource_type"] != "test"
    ]
    sources = [
        (res["unique_id"].replace("source.", "source:", 1), res["name"])
        for res in manifest["sources"].values()
    ]
    return nodes + sources
