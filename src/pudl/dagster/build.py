"""Dagster definitions assembly for PUDL.

This module is where PUDL assembles a complete :class:`dagster.Definitions` object from the
package's default assets, asset checks, jobs, resources, and sensors. Define helpers
here that compose those building blocks into a code location, especially when tests,
CLI entrypoints, or specialized environments need to override part of the default
assembly. Avoid putting asset or resource implementations here; this module should stay
focused on top-level orchestration assembly.

For the underlying Dagster concept, see
https://docs.dagster.io/getting-started/concepts#definitions
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from pudl.dagster.asset_checks import default_asset_checks
from pudl.dagster.assets import default_assets
from pudl.dagster.io_managers import (
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    ferc714_xbrl_sqlite_io_manager,
)
from pudl.dagster.jobs import default_jobs
from pudl.dagster.resources import default_resources
from pudl.dagster.sensors import default_sensors


def build_defs(
    *,
    resource_overrides: Mapping[str, Any] | None = None,
    asset_overrides: Sequence[Any] | None = None,
    asset_check_overrides: Sequence[dg.AssetChecksDefinition] | None = None,
    job_overrides: Sequence[Any] | None = None,
    sensor_overrides: Sequence[dg.SensorDefinition] | None = None,
) -> dg.Definitions:
    """Build a fresh PUDL ``Definitions`` object with optional overrides."""
    resources = dict(default_resources)
    if resource_overrides:
        resources.update(resource_overrides)

        etl_settings_override = resource_overrides.get("etl_settings")
        zenodo_dois_override = resources.get("zenodo_dois")
        if etl_settings_override is not None:
            if "ferc1_dbf_sqlite_io_manager" not in resource_overrides:
                resources["ferc1_dbf_sqlite_io_manager"] = type(
                    ferc1_dbf_sqlite_io_manager
                )(
                    etl_settings=etl_settings_override,
                    zenodo_dois=zenodo_dois_override,
                    db_name=ferc1_dbf_sqlite_io_manager.db_name,
                )

            if "ferc1_xbrl_sqlite_io_manager" not in resource_overrides:
                resources["ferc1_xbrl_sqlite_io_manager"] = type(
                    ferc1_xbrl_sqlite_io_manager
                )(
                    etl_settings=etl_settings_override,
                    zenodo_dois=zenodo_dois_override,
                    db_name=ferc1_xbrl_sqlite_io_manager.db_name,
                )

            if "ferc714_xbrl_sqlite_io_manager" not in resource_overrides:
                resources["ferc714_xbrl_sqlite_io_manager"] = type(
                    ferc714_xbrl_sqlite_io_manager
                )(
                    etl_settings=etl_settings_override,
                    zenodo_dois=zenodo_dois_override,
                    db_name=ferc714_xbrl_sqlite_io_manager.db_name,
                )

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


__all__ = ["build_defs"]
