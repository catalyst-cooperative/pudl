---
description: "Use when adding or editing table/resource/field/source metadata under src/pudl/metadata and related dbt schema definitions."
name: "PUDL Metadata Editing Instructions"
applyTo: "src/pudl/metadata/**/*.py, dbt/models/**/*.yml, dbt/models/**/*.yaml"
---

# Metadata editing instructions

- Treat metadata changes as schema/API changes: update all dependent metadata and tests.
- Keep resource descriptions, schema fields, primary keys, and foreign keys coherent.
- Note that our metadata structures generally follow the conventions of the Frictionless
  data package standard. Fields are columns. Resources are tables.

## Required follow-through for schema changes

- If a resource schema changes in `src/pudl/metadata/resources/**/*.py`, update the
  matching dbt `schema.yml` test definitions.
- Update row-count expectations or other data tests as needed.
- Create a new alembic migration to keep the PUDL database in sync with metadata, and
  then use it to upgrade the existing PUDL database.

## Naming and modeling consistency

- Follow established asset/table naming conventions and field naming conventions.
- Prefer consistent names for equivalent quantities across data sources.
- Include units in column names where applicable.

## Validation

- Run relevant dbt validations after metadata changes.
- Run targeted pytest checks touching metadata behavior.

See also:
- `docs/dev/metadata.rst`
- `docs/dev/naming_conventions.rst`
- `docs/dev/data_validation_quickstart.rst`
