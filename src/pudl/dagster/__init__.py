"""Canonical Dagster orchestration package for PUDL.

This package is the main import surface for the Dagster objects that make up the PUDL
code location. It should expose the assembled ``Definitions`` object along with the
default assets, asset checks, jobs, resources, and sensors that other modules or tools
need to load. Keep this module focused on package-level exports and lazy import wiring
rather than the implementation details of individual Dagster abstractions.

For the underlying Dagster concept, see
https://docs.dagster.io/getting-started/concepts#definitions
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import dagster as dg

    from pudl.dagster.asset_checks import default_asset_checks
    from pudl.dagster.assets import default_assets
    from pudl.dagster.build import build_defs
    from pudl.dagster.jobs import default_jobs
    from pudl.dagster.resources import default_resources
    from pudl.dagster.sensors import default_sensors

_EXPORTS = {
    "build_defs": ("pudl.dagster.build", "build_defs"),
    "default_asset_checks": ("pudl.dagster.asset_checks", "default_asset_checks"),
    "default_assets": ("pudl.dagster.assets", "default_assets"),
    "default_jobs": ("pudl.dagster.jobs", "default_jobs"),
    "default_resources": ("pudl.dagster.resources", "default_resources"),
    "default_sensors": ("pudl.dagster.sensors", "default_sensors"),
}


def __getattr__(name: str) -> Any:
    """Resolve public Dagster exports lazily to avoid eager package imports."""
    if name == "defs":
        from pudl.dagster.build import build_defs as _build_defs

        value: dg.Definitions = _build_defs()
        globals()["defs"] = value
        return value
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as err:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from err

    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Expose lazily loaded public exports in interactive contexts."""
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "build_defs",
    "default_asset_checks",
    "default_assets",
    "default_jobs",
    "default_resources",
    "default_sensors",
    "defs",  # noqa: F822 - lazily resolved via __getattr__
]
