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
from pudl.dagster.io_managers import default_io_managers
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


__all__ = ["build_defs"]
