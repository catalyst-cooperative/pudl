# LLM coding agent instructions for the Public Utility Data Liberation (PUDL) Project

## PUDL project overview

- PUDL ingests raw public energy data (EIA, FERC, EPA, and others) and transforms it
  into clean, analysis-ready tables.
- The pipeline is orchestrated using Dagster assets and jobs.
- Raw inputs are managed through the datastore and are rooted at `$PUDL_INPUT`.
- Primary outputs are Parquet files rooted at `$PUDL_OUTPUT/parquet/`.

## Python environment and tooling

- PUDL uses `pixi` for dependency and task management.
- Use `pixi run <command>` to ensure commands run in the project environment.
- Never try to create or manage Python environments manually; always use `pixi` to
  ensure consistency.
- Project tasks and environments are defined in `pyproject.toml` under `[tool.pixi]`.
- Git pre-commit hooks are defined in `.pre-commit-config.yaml`.

## Sandbox-safe command execution

- Prefer already-installed binaries before invoking commands that may trigger package
  resolution or updates.
- Prefer direct binaries (for example `dg`, `rg`, `ruff`) when they are already
  available in the active environment.
- When using `pixi run`, prefer frozen/locked execution modes that avoid dependency
  updates.
- For sandboxed terminal runs, keep cache and temporary directories writable and local
  to the workspace when possible, e.g. `TMPDIR`, `PIXI_HOME`.

## Always-on coding expectations

- Prefer explicit, readable code and descriptive names over terse names.
- Follow existing naming conventions and data model conventions.
- Reuse existing project helpers and established patterns before introducing new ones.
- Prefer dbt for data validation by default; use Dagster/Python validation only when
  there is a clear project-specific reason.

## Where to find additional detailed instructions

- If the `pudl` agent skill is enabled, it should be used to read PUDL database schemas
  and descriptions of tables and columns.
- If the `pudl-dev` agent skill is enabled, it should be used to inform software
  development tasks in the PUDL project.
- If the `dagster-expert` agent skill is enabled, it should be used when adding or
  modifying code related to orchestration of the data processing pipeline, and any of
  the concepts and classes defined by Dagster. This includes assets, resources, jobs,
  IO managers, sensors, etc.

## Developer documentation references

- General contributor docs: `docs/dev/`
- Python testing: `docs/dev/testing.rst`
- Data validation quickstart and reference using dbt:
  `docs/dev/data_validation_quickstart.rst`,
  `docs/dev/data_validation_reference.rst`
- Dagster development: `docs/dev/dev_dagster.rst`, `docs/dev/run_the_etl.rst`
- Editing PUDL Metadata: `docs/dev/metadata.rst`
- PUDL naming conventions: `docs/dev/naming_conventions.rst`

## Release notes

- Significant user-visible or developer-visible changes should be summarized in
  `docs/release_notes.rst`.
