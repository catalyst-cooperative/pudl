---
name: dbt
description: Run and debug PUDL dbt data validation tests, update schema.yml metadata, and maintain row-count expectations.
argument-hint: "[validate|build|update-schema|update-row-counts] [selectors_or_tables]"
---

# dbt Data Validation Workflow

Use this skill for dbt-backed data validation in PUDL.

## Trigger phrases

- "run dbt validation"
- "validate this PUDL table with dbt"
- "run dbt build"
- "debug failing dbt tests"
- "update schema.yml for this table"
- "refresh row count expectations"
- "update dbt row counts"
- "run targeted dbt checks"
- "exclude row count tests"

## PUDL dbt model

- dbt is used for data validation, not ETL transformations.
- Tests run against Parquet outputs under `$PUDL_OUTPUT/parquet/`.
- Commands should be run from `dbt/` when invoking `dbt` directly.

## Preferred commands

```bash
# Rich validation workflow with better failure context
pixi run dbt_helper validate

# Validate a subset using Dagster-style asset selection
pixi run dbt_helper validate --asset-select "key:out_eia__yearly_generators"
pixi run dbt_helper validate --asset-select "+key:out_eia__yearly_generators"

# Direct dbt invocation
cd dbt
pixi run dbt build
pixi run dbt build --exclude "test_name:check_row_counts_per_partition"
```

## Schema and row-count maintenance

```bash
# Update schema info for a table when metadata fields change
pixi run dbt_helper update-tables --schema out_eia__yearly_generators

# Update row-count expectations for one or more tables
pixi run dbt_helper update-tables --row-counts out_eia__yearly_generators
pixi run dbt_helper update-tables --row-counts --clobber out_eia__yearly_generators
```

## When changing metadata resources

- If `src/pudl/metadata/resources/**/*.py` schema fields change, update matching
  `dbt/models/**/schema.yml`.
- Re-run targeted validations and inspect failures before broad test runs.

## References

- `docs/dev/data_validation_quickstart.rst`
- `docs/dev/data_validation_reference.rst`
- [dbt Data Validation Instructions](../../instructions/dbt-data-validation.instructions.md)
