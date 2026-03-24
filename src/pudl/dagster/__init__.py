"""Canonical Dagster orchestration package for PUDL."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pudl.dagster.asset_checks import default_asset_checks
    from pudl.dagster.assets import default_assets
    from pudl.dagster.build import build_defs, defs
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
    "defs": ("pudl.dagster.build", "defs"),
}


def __getattr__(name: str) -> Any:
    """Resolve public Dagster exports lazily to avoid eager package imports."""
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
    "defs",
]
