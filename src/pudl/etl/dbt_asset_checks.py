"""Configure DBT <> Dagster integration."""

from collections.abc import Mapping
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from pudl.workspace.setup import PUDL_ROOT_DIR


class PudlDagsterDbtTranslator(DagsterDbtTranslator):
    """Dagster DBT translator that identifies which source checks should point at which assets."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Remove auto-generated ``pudl`` prefix if it exists."""
        stock_asset_key = super().get_asset_key(dbt_resource_props)
        if stock_asset_key.has_prefix(["pudl"]):
            return AssetKey(stock_asset_key.path[1:])
        return stock_asset_key


dagster_dbt_translator = PudlDagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_source_tests_as_checks=True)
)

dbt_project = DbtProject(project_dir=PUDL_ROOT_DIR / "dbt")
dbt_project.preparer.prepare(dbt_project)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=dagster_dbt_translator,
)
def dbt_asset_checks(context: AssetExecutionContext, dbt: DbtCliResource):
    """Automatically import assets from DBT.

    Questions:
    * do these assets get picked up at all? should I add them to another module
      and import them using load_asset_checks_from_modules()?
    * does it assign source tests to actual Dagster assets?
      * seems like we might need to override DagsterDbtTranslator methods to get this working right.
    * does it run checks after asset materialization?
    * does it handle concurrency well? will it try to re-run dbt seed etc. every check?
    * does it create assets for the validation views?
    """
    yield from dbt.cli(["build"], context=context).stream()
