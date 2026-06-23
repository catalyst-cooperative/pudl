"""Assembles Dagster Definitions from default values and specified overrides.

Define helpers here that compose those building blocks into a code location, especially
when tests, CLI entrypoints, or specialized environments need to override part of the
default assembly. Avoid putting asset or resource implementations here.

For the underlying Dagster concept, see
https://docs.dagster.io/getting-started/concepts#definitions
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from pudl.dagster.asset_checks import default_asset_checks
from pudl.dagster.assets import default_assets
from pudl.dagster.io_managers import (
    FercDbfSqliteIOManager,
    FercXbrlSqliteIOManager,
    PudlGeoParquetIOManager,
    PudlMixedFormatIOManager,
    PudlParquetIOManager,
    default_io_managers,
)
from pudl.dagster.jobs import default_jobs
from pudl.dagster.resources import (
    DatastoreResource,
    GlobalDataConfigResource,
    PudlPathsResource,
    ZenodoDoiSettingsResource,
    default_resources,
)
from pudl.dagster.sensors import default_sensors


def build_defs(
    *,
    resource_overrides: Mapping[str, Any] | None = None,
    asset_overrides: Sequence[Any] | None = None,
    asset_check_overrides: Sequence[dg.AssetChecksDefinition] | None = None,
    job_overrides: Sequence[Any] | None = None,
    sensor_overrides: Sequence[dg.SensorDefinition] | None = None,
) -> dg.Definitions:
    """Build a fresh PUDL ``Definitions`` object with optional overrides.

    Note that resource_overrides are used to update the default resources, while all
    other overrides replace the defaults entirely.
    """
    resources: dict[str, Any] = {
        **default_resources,
        **default_io_managers,
    }
    if resource_overrides:
        # Merge the overrides into the existing resources
        resources.update(resource_overrides)

    return dg.Definitions(
        assets=list(default_assets if asset_overrides is None else asset_overrides),
        asset_checks=list(
            default_asset_checks
            if asset_check_overrides is None
            else asset_check_overrides
        ),
        resources=resources,
        jobs=list(default_jobs if job_overrides is None else job_overrides),
        sensors=list(default_sensors if sensor_overrides is None else sensor_overrides),
    )


def _build_interactive_resources(
    *,
    global_data_config: GlobalDataConfigResource,
    pudl_paths: PudlPathsResource,
    zenodo_dois: ZenodoDoiSettingsResource,
) -> dict[str, Any]:
    """Build interactive-only resources wired to concrete local resource instances."""
    return {
        "datastore": DatastoreResource(
            zenodo_dois=zenodo_dois,
            pudl_paths=pudl_paths,
        ),
        "parquet_io_manager": PudlParquetIOManager(pudl_paths=pudl_paths),
        "geoparquet_io_manager": PudlGeoParquetIOManager(pudl_paths=pudl_paths),
        "pudl_io_manager": PudlMixedFormatIOManager(pudl_paths=pudl_paths),
        "ferc1_dbf_sqlite_io_manager": FercDbfSqliteIOManager(
            global_data_config=global_data_config,
            pudl_paths=pudl_paths,
            zenodo_dois=zenodo_dois,
            dataset="ferc1",
        ),
        "ferc1_xbrl_sqlite_io_manager": FercXbrlSqliteIOManager(
            global_data_config=global_data_config,
            pudl_paths=pudl_paths,
            zenodo_dois=zenodo_dois,
            dataset="ferc1",
        ),
        "ferc714_xbrl_sqlite_io_manager": FercXbrlSqliteIOManager(
            global_data_config=global_data_config,
            pudl_paths=pudl_paths,
            zenodo_dois=zenodo_dois,
            dataset="ferc714",
        ),
    }


def build_interactive_defs(
    *,
    global_data_config_path: str | None = None,
    pudl_input: str | None = None,
    pudl_output: str | None = None,
    zenodo_dois_path: str | None = None,
) -> dg.Definitions:
    """Build defs for interactive in-process use with concrete default resources.

    Dagster's asset value loader does not resolve the FERC SQLite IO managers when
    they reference partially configured nested resources, which happens when you're
    trying to load assets outside of a `dg`-spawned environment. So, in notebooks,
    REPLs, and local scripts, we need to explicitly construct the FERC IO managers.
    """
    if global_data_config_path is None:
        global_data_config = GlobalDataConfigResource()
    else:
        global_data_config = GlobalDataConfigResource(
            global_data_config_path=global_data_config_path
        )

    pudl_paths_kwargs: dict[str, str] = {}
    if pudl_input is not None:
        pudl_paths_kwargs["pudl_input"] = pudl_input
    if pudl_output is not None:
        pudl_paths_kwargs["pudl_output"] = pudl_output
    pudl_paths = PudlPathsResource(**pudl_paths_kwargs)

    zenodo_dois = ZenodoDoiSettingsResource(zenodo_dois_path=zenodo_dois_path)

    return build_defs(
        resource_overrides={
            "global_data_config": global_data_config,
            "pudl_paths": pudl_paths,
            "zenodo_dois": zenodo_dois,
            **_build_interactive_resources(
                global_data_config=global_data_config,
                pudl_paths=pudl_paths,
                zenodo_dois=zenodo_dois,
            ),
        }
    )


__all__ = ["build_defs", "build_interactive_defs"]
