# Testing and Validation

## Use this when

- Choosing test scope for a code or metadata change.
- Debugging pytest or dbt failures.
- Preparing a PR for review.

## Preferred sequence

1. Run targeted pytest without coverage while iterating.
2. Run dbt validation for affected outputs.
3. Run code-quality hooks before finalizing.

## Representative commands

```bash
# Unit tests
pixi run pytest --no-cov test/unit

# Integration tests with fast ETL settings
pixi run pytest --no-cov --etl-settings src/pudl/package_data/settings/etl_fast.yml test/integration

# dbt validation helper
pixi run dbt_helper validate

# Full hook suite
pixi run pre-commit-run
```

## Notes

- Use `dbt_helper` for richer diagnostics and targeted table selection.
- Keep test scope proportional to change size.
- Use `--live-dbs` for integration runs against existing local databases.

## Canonical sources

- `docs/dev/testing.rst`
- `docs/dev/data_validation_quickstart.rst`
- `docs/dev/data_validation_reference.rst`
- [Pytest Skill](../../pytest/SKILL.md)
- [dbt Skill](../../dbt/SKILL.md)
- [Code Quality Skill](../../code-quality/SKILL.md)
