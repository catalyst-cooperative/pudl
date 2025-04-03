"""Shim to associate DBT checks with the assets they check."""

<<<<<<< HEAD
=======
import json
>>>>>>> 37a990a6d (feat: factorify asset checks.)
from contextlib import chdir
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    ConfigurableResource,
    InitResourceContext,
    ResourceParam,
    asset_check,
)
from filelock import FileLock

from dbt.cli.main import dbtRunner
from pudl.settings import EtlSettings
<<<<<<< HEAD


class DbtRunner(ConfigurableResource):
=======
from pudl.workspace.setup import PUDL_ROOT_DIR


class DbtRunner(ConfigurableResource):
    """Resource to manage only having one DBT runner active at once.

    Uses FileLock to manage cross-process concurrency.

    """

    # NOTE 2025-04-04 dbt_dir is a str, not a Path, to work around Dagster
    # resource attr type limitations.
>>>>>>> 37a990a6d (feat: factorify asset checks.)
    dbt_dir: str
    target: str

    def setup_for_execution(self, context: InitResourceContext):
<<<<<<< HEAD
=======
        """Dagster resource lifecycle hook - runs once-per-run setup for DBT."""
>>>>>>> 37a990a6d (feat: factorify asset checks.)
        with FileLock(Path(self.dbt_dir) / "dbt.lock"), chdir(self.dbt_dir):
            dbt = dbtRunner()
            dbt.invoke(["deps"])
            dbt.invoke(["seed"])

    def build(self, resource_name: str):
<<<<<<< HEAD
        with FileLock(Path(self.dbt_dir) / "dbt.lock"), chdir(self.dbt_dir):
            dbt = dbtRunner()
            return dbt.invoke(
                [
                    "build",
=======
        """Build a dagster resource, including tests.

        Args:
        resource_name: the string you'd pass in to dbt build --select to get
            dbt to find your resource.

        Returns:
        success: whether or not the dang thing worked

        """
        with FileLock(Path(self.dbt_dir) / "dbt.lock"), chdir(self.dbt_dir):
            dbt = dbtRunner()
            build_result = dbt.invoke(
                [
                    "build",
                    "--store-failures",
>>>>>>> 37a990a6d (feat: factorify asset checks.)
                    "--threads",
                    "1",
                    "--target",
                    self.target,
                    "--select",
                    resource_name,
                ]
            )
<<<<<<< HEAD
=======
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
>>>>>>> 37a990a6d (feat: factorify asset checks.)


def make_dbt_asset_checks(
    dagster_assets: list[AssetKey],
) -> list[AssetChecksDefinition]:
<<<<<<< HEAD
    """Associate dbt resources with dagster assets, then run the build at asset-check time."""
    dbt = dbtRunner()
    dbt_dir = Path(__file__).parent.parent.parent.parent / "dbt"
    with FileLock(dbt_dir / "dbt.lock"), chdir(dbt_dir):
        _ = dbt.invoke(["deps"])
        invocation = dbt.invoke(["ls"])
        if invocation.exception:
            raise RuntimeError(invocation.exception)
        resources = [name.rsplit(".", 1) for name in dbt.invoke(["ls"]).result]

    dagster_asset_names = {da.to_user_string() for da in dagster_assets}
    assets = {
        short_name: f"{prefix}.{short_name}"
        for prefix, short_name in resources
=======
    """Associate dbt resources with dagster assets, then run the build at asset-check time.

    1. get the possible dbt resources to run
    2. get the dagster assets
    3. build checks for the intersection.
    """
    dbt = dbtRunner()
    dbt_dir = PUDL_ROOT_DIR / "dbt"
    manifest_path = dbt_dir / "target" / "manifest.json"
    with FileLock(dbt_dir / "dbt.lock"), chdir(dbt_dir):
        _ = dbt.invoke(["deps", "--quiet"])
        parse_result = dbt.invoke(["parse", "--quiet"])
        if parse_result.exception:
            raise RuntimeError(parse_result.exception)
        resources = __get_dbt_selection_names(manifest_path)

    dagster_asset_names = {da.to_user_string() for da in dagster_assets}
    assets = {
        short_name: select_id
        for select_id, short_name in resources
>>>>>>> 37a990a6d (feat: factorify asset checks.)
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
        dataset_settings: ResourceParam[EtlSettings],
        dbt_runner: ResourceParam[DbtRunner],
    ) -> AssetCheckResult:
        """Build DBT resource, along with all attendant checks."""
        test_results = dbt_runner.build(dbt_resource_name)
<<<<<<< HEAD
        error_infos = []
        if test_results.exception:
            error_infos.append({"message": repr(test_results.exception)})
        else:
            for res in test_results.result.results:
                error_infos.append({"message": res.message, "n_failures": res.failures})

        return AssetCheckResult(
            passed=test_results.success, metadata={"failures": error_infos}
        )

    return dbt_build_resource
=======

        return AssetCheckResult(
            passed=test_results["success"],
            metadata={"failures": test_results["failures"]},
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
>>>>>>> 37a990a6d (feat: factorify asset checks.)
