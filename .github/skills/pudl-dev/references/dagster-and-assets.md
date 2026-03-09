# Dagster and Assets

## Use this when

- Working with Dagster assets, jobs, schedules, or sensors.
- Understanding orchestration behavior during ETL/data builds.
- Deciding where validation logic should live.

## PUDL-specific guidance

- Keep asset dependencies explicit and readable.
- Prefer dbt for data validation where feasible.
- Use Dagster/Python checks when dbt is not a good fit.
- Distinguish internal/raw assets from distributed output artifacts.

## Operational framing

- Dagster drives the processing graph and persistence.
- Data release pipelines rely on this orchestration structure.
- Changes to orchestration should usually include docs updates.

## Canonical sources

- `docs/dev/dev_dagster.rst`
- `docs/dev/run_the_etl.rst`
- `docs/dev/nightly_data_builds.rst`
- `docs/dev/ferceqr_data_builds.rst`
- [Dagster Expert Skill](../../dagster-expert/SKILL.md)
