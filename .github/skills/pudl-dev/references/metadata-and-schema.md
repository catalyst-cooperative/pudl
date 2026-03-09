# Metadata and Schema

## Use this when

- Adding or modifying table, field, or source metadata.
- Reconciling schema changes across metadata, dbt, and database migrations.
- Tracking where descriptive metadata comes from.

## Source-of-truth locations

- Table metadata: `src/pudl/metadata/resources/*.py`
- Field metadata: `src/pudl/metadata/fields.py`
- Data sources: `src/pudl/metadata/sources.py`
- Metadata models/helpers: `src/pudl/metadata/classes.py`

## Required follow-through

1. Update metadata definitions.
2. Update related dbt schema tests in `dbt/models/**/schema.yml`.
3. Run targeted dbt validations.
4. Add an Alembic migration if database schema changed.

## Useful API patterns

- `PUDL_PACKAGE.get_resource(table_name)` for table metadata.
- `Field.from_id(field_name)` for field-level metadata.
- `DataSource.from_id(source_name)` for source metadata.
- `PUDL_PACKAGE.to_frictionless().to_json()` for full machine-queryable metadata export.

## Generated docs fallback

- If rendered docs are unavailable, query Frictionless JSON and metadata classes directly.
- Data dictionary and data-source pages are generated via templates in `docs/templates/`
  and custom build operations in `docs/conf.py`:
  - `data_dictionary_metadata_to_rst()`
  - `data_sources_metadata_to_rst()`
  - `static_dfs_to_rst()`
- Keep generated intermediates while rebuilding docs with:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
```

## Canonical sources

- `docs/dev/metadata.rst`
- `docs/dev/naming_conventions.rst`
- `docs/data_dictionaries/index.rst`
- `docs/conf.py`
- `docs/templates/`
- [Alembic Skill](../../alembic/SKILL.md)
