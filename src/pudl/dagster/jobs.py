"""Dagster jobs for PUDL.

This module defines the named jobs that package asset selections, execution settings,
and default run configuration into launchable units. Add job definitions here when the
team needs a stable execution target for a common workflow, such as running the main
ETL, refreshing prerequisites, or materializing a specialized asset subset. Avoid
placing asset implementations or resource classes here; those should remain in the
modules that define them.

For the underlying Dagster concept, see https://docs.dagster.io/guides/build/jobs
"""

import dagster as dg

from pudl.dagster.config import (
    default_execution_config,
    default_pudl_job_config,
    default_tag_concurrency_limits,
    load_etl_run_config_from_file,
)

default_jobs = [
    dg.define_asset_job(
        name="pudl",
        description=(
            "This job executes the main PUDL ETL without refreshing the FERC-to-SQLite "
            "prerequisites."
        ),
        config=default_pudl_job_config | load_etl_run_config_from_file("etl_full"),
        selection=dg.AssetSelection.all()
        - dg.AssetSelection.groups(
            "raw_ferc_to_sqlite",
            "raw_ferceqr",
            "core_ferceqr",
        ),
    ),
    dg.define_asset_job(
        name="ferc_to_sqlite",
        description="This job refreshes the FERC-to-SQLite prerequisite assets only.",
        config=default_execution_config | load_etl_run_config_from_file("etl_full"),
        selection=dg.AssetSelection.groups("raw_ferc_to_sqlite"),
    ),
    dg.define_asset_job(
        name="pudl_with_ferc_to_sqlite",
        description=(
            "This job executes the main PUDL ETL including the FERC-to-SQLite "
            "prerequisites (default: full settings profile)."
        ),
        config=default_pudl_job_config | load_etl_run_config_from_file("etl_full"),
        selection=dg.AssetSelection.all()
        - dg.AssetSelection.groups("raw_ferceqr", "core_ferceqr"),
    ),
    dg.define_asset_job(
        name="ferceqr",
        description="This job processes the FERC EQR data.",
        config={
            "execution": {
                "config": {
                    "multiprocess": {
                        "max_concurrent": 0,
                        "tag_concurrency_limits": default_tag_concurrency_limits,
                    },
                },
            },
        },
        selection=dg.AssetSelection.groups("raw_ferceqr", "core_ferceqr"),
    ),
]

__all__ = ["default_jobs"]
