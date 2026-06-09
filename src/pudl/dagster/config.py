"""Dagster run configuration helpers for PUDL.

This module collects reusable run-configuration fragments and helpers that jobs,
automation, and tests can share. Define execution settings, concurrency controls, and
resource-config loaders here when they are meant to be combined into Dagster job or
launch configuration, rather than hard-coding them in individual jobs or scripts.

For the underlying Dagster concept, see
https://docs.dagster.io/guides/operate/configuration/run-configuration
"""

from pudl.analysis.ml_tools import get_ml_models_config

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


__all__ = [
    "default_execution_config",
    "default_pudl_job_config",
    "default_tag_concurrency_limits",
]
