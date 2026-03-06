# LLM coding agent instructions for the Public Utility Data Liberation (PUDL) Project

## PUDL project overview

- PUDL ingests raw public energy data (EIA, FERC, EPA, and others) and transforms it
  into clean, analysis-ready tables.
- The pipeline is orchestrated using Dagster assets and jobs.
- Raw inputs are managed through the datastore and are rooted at `$PUDL_INPUT`.
- Primary outputs are Parquet files rooted at `$PUDL_OUTPUT/parquet/`.

## Environment and tooling baseline

- PUDL uses `pixi` for dependency and task management.
- Use `pixi run <command>` for project commands unless already in an activated pixi
  shell.
- Project tasks and environments are defined in `pyproject.toml` under `[tool.pixi]`.
- The canonical hook list is in `.pre-commit-config.yaml`.

## Always-on coding expectations

- Prefer explicit, readable code and descriptive names over terse names.
- Treat this file as high-level policy only; keep language- and task-specific rules in
  `.github/instructions/` and `.github/skills/`.
- Follow existing naming conventions and data model conventions.
- Reuse existing project helpers and established patterns before introducing new ones.
- Prefer dbt for data validation by default; use Dagster/Python validation only when
  there is a clear project-specific reason.

## Where detailed instructions now live

- `.github/instructions/python-style.instructions.md`
- `.github/instructions/testing.instructions.md`
- `.github/instructions/code-quality.instructions.md`
- `.github/instructions/dbt-data-validation.instructions.md`
- `.github/instructions/metadata.instructions.md`
- `.github/instructions/docs.instructions.md`

## Available workflow skills

- `.github/skills/pytest/SKILL.md`
- `.github/skills/dbt/SKILL.md`
- `.github/skills/code-quality/SKILL.md`
- `.github/skills/pudl-data-access/SKILL.md`

## Documentation references

- General contributor docs: `docs/dev/`
- Testing: `docs/dev/testing.rst`
- Data validation quickstart and reference:
  `docs/dev/data_validation_quickstart.rst`,
  `docs/dev/data_validation_reference.rst`
- Dagster development: `docs/dev/dev_dagster.rst`, `docs/dev/run_the_etl.rst`
- Metadata editing: `docs/dev/metadata.rst`
- Naming conventions: `docs/dev/naming_conventions.rst`

## Release notes

- Significant user-visible or developer-visible changes should be summarized in
  `docs/release_notes.rst`.
