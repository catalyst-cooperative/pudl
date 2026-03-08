---
name: alembic
description: Run alembic to create database schema migrations when the PUDL metadata
changes, and to apply those migrations to the PUDL database.
---

# Alembic workflow

Use this skill when changes have been made to the PUDL metadata that defines columns and
tables, including changes to table or column names, column dtypes, primary keys, ENUM
constraints, foreign key constraints, or any other changes that would show up in the
database schema.

## Trigger phrases

- "run alembic"
- "alembic migration"
- "alembic upgrade"
- "update table metadata"
- "change column dtype"
- "define a new PUDL table"

## Prerequisites

- Run commands in the pixi environment (`pixi run ...`) unless already in
  `pixi shell`.

## Common commands

```bash
# Create a new alembic migration reflecting updates to the PUDL metadata
pixi run alembic revision --autogenerate -m 'message describing changes'

# Upgrade the existing PUDL database to reflect the new migration
pixi run alembic upgrade head

# Remove the PUDL database and start over from scratch when there are complex issues
# This results in an empty database with the new schema
rm -f $PUDL_OUTPUT/pudl.sqlite
pixi run alembic upgrade head

# Merge multiple lineages of alembic migrations after merging in main
pixi run alembic merge heads

# Consolidate all alembic migrations that have accumulated on the working branch
# Must be run from the PUDL repository root
git rm -rf migrations/versions
git checkout main -- migrations
pixi run alembic revision --autogenerate -m 'message describing changes'
pixi run alembic upgrade head
```

## PUDL-specific alembic quirks

- These migrations are a development tool, and are not used in production.
- PUDL does not strictly need a continuous database migration history. We primarily use
  the database schema as a data quality assurance check.
- When there are complex conflicts between the alembic migrations on branches that need
  to be merged together, it is acceptable to remove existing migrations and start over
  from scratch, taking the current schema as a new base.

## References

- `docs/dev/run_the_etl.rst`
