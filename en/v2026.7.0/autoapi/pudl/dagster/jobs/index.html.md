# pudl.dagster.jobs

Dagster jobs for PUDL.

This module defines the named jobs that package asset selections, execution settings,
and default run configuration into launchable units. Add job definitions here when the
team needs a stable execution target for a common workflow, such as running the main
ETL, refreshing prerequisites, or materializing a specialized asset subset. Avoid
placing asset implementations or resource classes here; those should remain in the
modules that define them.

For the underlying Dagster concept, see [https://docs.dagster.io/guides/build/jobs](https://docs.dagster.io/guides/build/jobs)

## Attributes

| [`pudl_job`](#pudl.dagster.jobs.pudl_job)                                         |    |
|-----------------------------------------------------------------------------------|----|
| [`ferc_to_sqlite_job`](#pudl.dagster.jobs.ferc_to_sqlite_job)                     |    |
| [`pudl_with_ferc_to_sqlite_job`](#pudl.dagster.jobs.pudl_with_ferc_to_sqlite_job) |    |
| [`ferceqr_job`](#pudl.dagster.jobs.ferceqr_job)                                   |    |
| [`ferceqr_deployment_job`](#pudl.dagster.jobs.ferceqr_deployment_job)             |    |
| [`default_jobs`](#pudl.dagster.jobs.default_jobs)                                 |    |

## Module Contents

### pudl.dagster.jobs.pudl_job

### pudl.dagster.jobs.ferc_to_sqlite_job

### pudl.dagster.jobs.pudl_with_ferc_to_sqlite_job

### pudl.dagster.jobs.ferceqr_job

### pudl.dagster.jobs.ferceqr_deployment_job

### pudl.dagster.jobs.default_jobs
