"""Shim to associate DBT checks with the assets they check."""

import json
from contextlib import chdir
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    ConfigurableResource,
    ResourceDependency,
    ResourceParam,
    asset_check,
)
from filelock import FileLock

from dbt.cli.main import dbtRunner
from pudl.settings import DatasetsSettings
from pudl.workspace.setup import PUDL_ROOT_DIR


class DbtRunner(ConfigurableResource):
    """Resource to manage only having one DBT runner active at once.

    Uses FileLock to manage cross-process concurrency.

    """

    # NOTE 2025-04-04 dbt_dir is a str, not a Path, to work around Dagster
    # resource attr type limitations.
    dbt_dir: str
    dataset_settings: ResourceDependency[DatasetsSettings]

    @property
    def target(self):
        """The DBT target, derived from dataset settings.

        For runs that aren't associated with any predefined ETL type (e.g. if
        you just call ``materialize()``), assume that the DBT target should be
        "etl-fast". We could add the option to define ad-hoc seed data, but
        haven't done so yet as of 2025-04-10.
        """
        etl_settings = self.dataset_settings.etl_type
        target = etl_settings if etl_settings != "adhoc" else "fast"
        return f"etl-{target}"

    def build(self, resource_name: str):
        """Build a dagster resource, including tests.

        Args:
        resource_name: the string you'd pass in to dbt build --select to get
            dbt to find your resource.

        Returns:
        success: whether or not the dang thing worked

        """
        lock_path = Path(self.dbt_dir) / "dbt.lock"
        with FileLock(lock_path), chdir(self.dbt_dir):
            dbt = dbtRunner()
            dbt.invoke(["deps", "--target", self.target, "--quiet"])
            dbt.invoke(["seed", "--target", self.target, "--quiet"])
            build_result = dbt.invoke(
                [
                    "build",
                    "--store-failures",
                    "--threads",
                    "1",
                    "--target",
                    self.target,
                    "--select",
                    resource_name,
                ]
            )
            if build_result.exception:
                raise RuntimeError(build_result.exception)
            failure_infos = []
            for res in build_result.result.results:
                if res.failures != 0:
                    show_result = dbt.invoke(
                        [
                            "show",
                            "--target",
                            self.target,
                            "--select",
                            resource_name,
                        ]
                    )
                    if show_result.exception:
                        raise RuntimeError(show_result.exception)
                    for r in show_result.result.results:
                        failure_infos += [
                            dict(zip(r.agate_table.column_names, row, strict=True))
                            for row in r.agate_table.rows
                        ]
            return {"success": build_result.success, "failures": failure_infos}


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
    lock_path = dbt_dir / "dbt.lock"
    with FileLock(lock_path), chdir(dbt_dir):
        dbt = dbtRunner()
        _ = dbt.invoke(["deps", "--quiet"])
        parse_result = dbt.invoke(["parse", "--quiet"])
        del dbt
        if parse_result.exception:
            raise RuntimeError(parse_result.exception)
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
        dbt_runner: ResourceParam[DbtRunner],
    ) -> AssetCheckResult:
        """Build DBT resource, along with all attendant checks."""
        test_results = dbt_runner.build(dbt_resource_name)

        return AssetCheckResult(
            passed=test_results["success"],
            metadata={"failures": test_results["failures"]}
            if not test_results["success"]
            else {},
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
