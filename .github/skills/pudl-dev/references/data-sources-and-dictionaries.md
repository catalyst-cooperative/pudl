# Data Sources and Dictionaries

## Use this when

- Mapping code changes to affected datasets.
- Finding source-specific caveats and partitioning rules.
- Looking up table/field/code descriptions for outputs.

## What to consult

- Data source pages for ingestion status and source-specific context.
- Data dictionaries for table/field/code semantics.
- Usage warnings for known caveats in interpretation.

## Typical workflow

1. Identify the affected source (e.g., EIA-923, FERC1).
2. Check source docs for dataset constraints and publication semantics.
3. Check the relevant data dictionary page for output table meanings.
4. Verify metadata definitions in `src/pudl/metadata/` match docs claims.

## When generated pages are unavailable

- Export metadata from `PUDL_PACKAGE.to_frictionless().to_json()`.
- Query resources and fields using `jq` or DuckDB.
- Inspect page-generation structure in `docs/templates/` and `docs/conf.py`.
- Rebuild docs with retained generated files:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
```

- Generated files retained by that build appear under:
  - `docs/data_sources/**/*.rst`
  - `docs/data_dictionaries/**/*.rst`
  - `docs/data_dictionaries/**/*.csv`

## Canonical sources

- `docs/data_sources/index.rst`
- `docs/data_dictionaries/index.rst`
- `docs/data_dictionaries/pudl_db.rst`
- `docs/data_dictionaries/ferc1_db.rst`
- `docs/data_dictionaries/codes_and_labels.rst`
- `docs/data_dictionaries/usage_warnings.rst`
- `docs/data_sources/other_data.rst`
- `docs/data_sources/future_data.rst`
- `docs/templates/`
- `.github/skills/pudl-dev/references/generated-docs-and-metadata-querying.md`
