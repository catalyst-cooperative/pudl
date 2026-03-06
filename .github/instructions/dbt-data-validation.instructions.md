---
description: "Use when running or updating dbt data validations, dbt schema.yml tests, or row-count expectations for PUDL tables."
name: "PUDL dbt Data Validation Instructions"
applyTo: "dbt/**/*.sql, dbt/**/*.yml, dbt/**/*.yaml, src/pudl/metadata/resources/**/*.py"
---

# dbt data validation instructions

- In PUDL, dbt is used for data validation, not transformations.
- Run dbt commands from the `dbt/` directory (or use tasks that set `cwd`).
- Validate against outputs in `$PUDL_OUTPUT/parquet/` produced by the current branch.

## Preferred validation workflow

```bash
# Rich diagnostics + Dagster-style asset selection support
pixi run dbt_helper validate

# Example: validate one asset and upstream dependencies
pixi run dbt_helper validate --asset-select "+key:out_eia__yearly_generators"
```

## Run dbt directly

```bash
cd dbt
pixi run dbt build

# Example: exclude row-count checks for fast ETL output validation
pixi run dbt build --exclude "test_name:check_row_counts_per_partition"
```

## Schema + metadata alignment

- If a table schema changes in `pudl.metadata.resources`, update corresponding
  `dbt/models/<source>/<table>/schema.yml`.
- Use `dbt_helper update-tables` where possible for schema and row-count updates.

See also:
- `docs/dev/data_validation_quickstart.rst`
- `docs/dev/data_validation_reference.rst`
