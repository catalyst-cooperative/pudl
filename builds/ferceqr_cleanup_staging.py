#!/usr/bin/env python3
"""Remove orphaned ``._staging_*`` directories from FERC EQR deployment targets.

These directories are created by :func:`~pudl.dagster.assets.deploy.ferceqr._deploy_to_staging`
as part of the atomic staging-then-rename pattern. If the build is interrupted
(timeout, crash, container preemption) before ``_promote_staging`` runs, the
staging directories survive on the target and must be cleaned up.

This script is intended for the FERC EQR batch build only. It reads the
deployment-target configuration from the YAML file pointed at by the
``PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH`` environment variable and removes any
``._staging_*`` directories found beneath the resolved targets, using
:class:`~upath.UPath` for both local paths and cloud URIs (``gs://``, ``s3://``).

Usage in a batch script::

    python3 ${PUDL_ROOT_PATH}/builds/ferceqr_cleanup_staging.py
"""

import logging
import os
import sys
from pathlib import Path

import yaml
from upath import UPath

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Set up logging to stderr for container/script use."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )


def _resolve_targets(config_path: str) -> list[UPath]:
    """Load deployment targets from the YAML config and return UPath objects."""
    config = yaml.safe_load(Path(config_path).read_text()) or {}
    targets = config.get("deployment_targets", [])
    resolved: list[UPath] = []
    for target in targets:
        base = UPath(target["path"], **target.get("storage_options", {}))
        resolved.append(base)
    return resolved


def _remove_staging_dirs(targets: list[UPath]) -> int:
    """Remove any ``._staging_*`` directories under *targets*.

    Returns the number of directories removed.
    """
    removed = 0
    for base in targets:
        try:
            is_dir = base.is_dir()
        except Exception:  # noqa: S112
            continue
        if not is_dir:
            continue
        for child in base.iterdir():
            if child.is_dir() and child.name.startswith("._staging_"):
                try:
                    child.fs.rm(child.path, recursive=True)
                    logger.info(f"Removed orphaned staging dir: {child}")
                    removed += 1
                except Exception:  # noqa: S112
                    logger.warning(f"Failed to remove staging dir: {child}")
    return removed


def main() -> int:
    """Run the cleanup and return an exit code."""
    _configure_logging()
    config_path = os.environ.get("PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH")
    if not config_path:
        logger.info(
            "PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH is not set; nothing to clean up."
        )
        return 0

    config_file = Path(config_path)
    if not config_file.exists():
        logger.info(
            f"Deployment config not found at {config_path}; nothing to clean up."
        )
        return 0

    targets = _resolve_targets(config_path)
    if not targets:
        logger.info("No deployment targets configured; nothing to clean up.")
        return 0

    removed = _remove_staging_dirs(targets)
    logger.info(f"Staging directory cleanup complete. Removed {removed} dir(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
