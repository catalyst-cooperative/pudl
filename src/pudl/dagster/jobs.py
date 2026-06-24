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
)

pudl_job = dg.define_asset_job(
    name="pudl",
    description=(
        "This job executes the main PUDL ETL without refreshing the FERC-to-SQLite "
        "prerequisites."
    ),
    config=default_pudl_job_config,
    selection=dg.AssetSelection.all()
    - dg.AssetSelection.groups(
        "raw_ferc_to_sqlite",
        "raw_ferceqr",
        "core_ferceqr",
        "ferceqr_deployment",
    ),
)

ferc_to_sqlite_job = dg.define_asset_job(
    name="ferc_to_sqlite",
    description="This job refreshes the FERC-to-SQLite prerequisite assets only.",
    config=default_execution_config,
    selection=dg.AssetSelection.groups("raw_ferc_to_sqlite"),
)

pudl_with_ferc_to_sqlite_job = dg.define_asset_job(
    name="pudl_with_ferc_to_sqlite",
    description=(
        "This job executes the main PUDL ETL including the FERC-to-SQLite "
        "prerequisites (default: full settings profile)."
    ),
    config=default_pudl_job_config,
    selection=dg.AssetSelection.all()
    - dg.AssetSelection.groups(
        "raw_ferceqr",
        "core_ferceqr",
        "ferceqr_deployment",
    ),
)

ferceqr_job = dg.define_asset_job(
    name="ferceqr",
    description="This job processes the FERC EQR data.",
    selection=dg.AssetSelection.groups("raw_ferceqr", "core_ferceqr"),
)

ferceqr_deployment_job = dg.define_asset_job(
    name="ferceqr_deployment",
    description="This job handles FERC EQR deployment success and failure actions.",
    config=default_execution_config,
    selection=dg.AssetSelection.assets(
        "deploy_ferceqr",
        "handle_ferceqr_failure",
    ),
)

default_jobs = [
    pudl_job,
    ferc_to_sqlite_job,
    pudl_with_ferc_to_sqlite_job,
    ferceqr_job,
    ferceqr_deployment_job,
]

__all__ = [
    "default_jobs",
    "ferc_to_sqlite_job",
    "ferceqr_deployment_job",
    "ferceqr_job",
    "pudl_job",
    "pudl_with_ferc_to_sqlite_job",
]
