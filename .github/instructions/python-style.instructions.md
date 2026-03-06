---
description: "Use when editing Python source code in PUDL ETL, analysis, IO managers, metadata helpers, or scripts."
name: "PUDL Python Style and Patterns"
applyTo: "src/**/*.py"
---

# Python style and patterns

- Prefer descriptive names and explicit transformations over terse logic.
- Follow snake_case for functions/variables and PascalCase for classes.
- Use f-strings for string interpolation.
- Use `pathlib.Path` for path handling.
- Use logging instead of `print()`.
- Keep lines readable (project standard targets 88 columns).

## Dataframe conventions

- Prefer vectorized pandas operations to row-wise loops and `apply`.
- Avoid chained indexing.
- Avoid `inplace=True` mutations.
- Use nullable dtypes where practical (`pd.Int64Dtype()`, `pd.StringDtype()`).
- Use method chaining when it improves readability.

## PUDL-specific implementation patterns

- Keep Dagster asset dependencies explicit and documented.
- Prefer data validation in dbt unless there is a clear reason to use Dagster asset
  checks or Python assertions.
- Use datastore patterns for raw data access instead of bespoke direct file IO.
- Reuse existing helpers in `pudl.helpers` when available.

See also:
- `docs/dev/naming_conventions.rst`
- `docs/dev/run_the_etl.rst`
