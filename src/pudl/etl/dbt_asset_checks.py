"""Shim to associate DBT checks with the assets they check."""

import json
from pathlib import Path
from typing import NamedTuple

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    ResourceParam,
    asset_check,
)
from dagster_dbt import DbtCliResource
from filelock import FileLock

from pudl.settings import DatasetsSettings
from pudl.workspace.setup import PUDL_ROOT_DIR

DBT_LOCKFILE = PUDL_ROOT_DIR / "dbt" / "dbt.lock"


def make_dbt_asset_checks(
    dagster_assets: list[AssetKey],
) -> list[AssetChecksDefinition]:
    """Associate dbt resource checks with dagster assets.

    Requires there to be a ``dbt/target/manifest.json`` file in the
    PUDL_ROOT_DIR. If you're running ``dagster dev``, this should be getting
    updated automatically - but if not, ``dagster-dbt project
    prepare-and-package --file src/pudl/etl/dbt_config.py`` should do the
    trick.

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
        """Build DBT resource, along with all attendant checks.

        Will build against the ``etl-fast`` target unless explicitly run with
        the ``etl-full` job in Dagster. This means that ad-hoc materializations
        are checked against the ``etl-fast`` dbt target.

        NOTE 2025-04-15: We can potentially add an ``etl-adhoc`` target in the
        future that can be adjusted for local development, but seemed out of
        scope.

        NOTE 2025-04-15: Does not expose the actual failing rows - though the
        query to find them is captured in the Dagster logs and in the
        ``stdout`` tab of the Dagster UI. For some reason the dbt_cli.cli()
        output doesn't want to ingest any of the events that occur for ``dbt
        build``. We *could* use the ``dbt.cli.main.dbtRunner`` class, but for
        some reason that was having concurrency issues accessing DuckDB, even
        wrapped with FileLock, while the ``dagster_dbt.DbtCliResource`` does
        not.
        """
        dbt_target = "etl-full" if dataset_settings.etl_type == "full" else "etl-fast"
        with FileLock(DBT_LOCKFILE, timeout=60 * 30):
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


class DbtSelection(NamedTuple):
    """Named tuple to hold dbt selection strings and their corresponding asset names.

    Attributes:
        dbt_selector: The dbt selection string.
        dagster_name: The name of the asset in Dagster.
    """

    dbt_selector: str
    dagster_name: str


def __get_dbt_selection_names(manifest_path: Path) -> list[DbtSelection]:
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
    with manifest_path.open() as f:
        manifest = json.load(f)

    nodes = [
        DbtSelection(dbt_selector=res["name"], dagster_name=res["name"])
        for res in manifest["nodes"].values()
        if res["resource_type"] != "test"
    ]
    sources = [
        DbtSelection(
            dbt_selector=res["unique_id"].replace("source.", "source:", 1),
            dagster_name=res["name"],
        )
        for res in manifest["sources"].values()
    ]
    return nodes + sources
