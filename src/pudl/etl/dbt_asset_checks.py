"""Shim to associate DBT checks with the assets they check."""

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


class DbtRunner(ConfigurableResource):
    dbt_dir: str
    target: str

    def setup_for_execution(self, context: InitResourceContext):
        with FileLock(Path(self.dbt_dir) / "dbt.lock"), chdir(self.dbt_dir):
            dbt = dbtRunner()
            dbt.invoke(["deps"])
            dbt.invoke(["seed"])

    def build(self, resource_name: str):
        with FileLock(Path(self.dbt_dir) / "dbt.lock"), chdir(self.dbt_dir):
            dbt = dbtRunner()
            return dbt.invoke(
                [
                    "build",
                    "--threads",
                    "1",
                    "--target",
                    self.target,
                    "--select",
                    resource_name,
                ]
            )


def make_dbt_asset_checks(
    dagster_assets: list[AssetKey],
) -> list[AssetChecksDefinition]:
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
