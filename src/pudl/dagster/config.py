"""Dagster run configuration helpers for PUDL.

This module collects reusable run-configuration fragments and helpers that jobs,
automation, and tests can share. Define execution settings, concurrency controls, and
resource-config loaders here when they are meant to be combined into Dagster job or
launch configuration, rather than hard-coding them in individual jobs or scripts.

For the underlying Dagster concept, see
https://docs.dagster.io/guides/operate/configuration/run-configuration
"""

from pudl.analysis.ml_tools import get_ml_models_config
from pudl.settings import load_packaged_etl_settings

default_tag_concurrency_limits = [
    {
        "key": "memory-use",
        "value": "high",
        "limit": 4,
    },
]

default_execution_config = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 0,
                "tag_concurrency_limits": default_tag_concurrency_limits,
            },
        },
    },
}

default_pudl_job_config = default_execution_config | get_ml_models_config()


def load_etl_run_config_from_file(setting_filename: str) -> dict:
    """Load ETL run config from a packaged settings profile."""
    settings = load_packaged_etl_settings(setting_filename)
    if settings.ferc_to_sqlite_settings is None:
        raise ValueError("Missing ferc_to_sqlite_settings in ETL settings file.")

    etl_settings_path = f"src/pudl/package_data/settings/{setting_filename}.yml"

    return {
        "resources": {
            "etl_settings": {"config": {"etl_settings_path": etl_settings_path}},
            "runtime_settings": {"config": {}},
        }
    }


__all__ = [
    "default_execution_config",
    "default_pudl_job_config",
    "default_tag_concurrency_limits",
    "load_etl_run_config_from_file",
]
