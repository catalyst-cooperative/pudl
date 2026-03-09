---
name: pudl
description: >
  Access PUDL table data plus table/column/source metadata in Jupyter or Marimo
  notebooks for debugging and visualization. Use when users ask what a table contains,
  how to read it, or how columns are defined.
argument-hint: "[table_name] [local|s3] [data|metadata|both]"
---

# PUDL Data and Metadata Guide

Use this skill for data-user workflows: reading PUDL outputs, understanding table and
field semantics, tracing source context, and interpreting methodology.

## Trigger phrases

- "show me this PUDL table in a notebook"
- "how do I read table X from Parquet"
- "load this table from local PUDL output"
- "read this PUDL table from S3"
- "what columns are in this table"
- "what does this column mean"
- "show table metadata for X"
- "which data source does this table come from"
- "find field descriptions for this table"
- "I need notebook code to explore this PUDL data"

## Audience and scope

- Primary audience: analysts and data users, not core maintainers.
- Focus: using outputs and metadata confidently without digging into internals.
- Secondary audience: developers who need the same data context while coding.

## Source-of-truth policy

The reference files in this skill are the canonical AI-facing layer for:

- data access and output usage patterns,
- table/field/source metadata retrieval,
- source-doc and methodology interpretation.

`pudl-dev` should reuse these references instead of duplicating them.

## Reference index

- [Data Access and Outputs](./references/data-access-and-outputs.md)
- [Metadata and Querying](./references/metadata-and-querying.md)
- [Data Sources, Dictionaries, and Methodology](./references/data-sources-dictionaries-and-methodology.md)

## Escalation boundary

If a task requires modifying ETL code, schema migrations, dbt tests, or CI workflow,
hand off to `pudl-dev` and keep these references as the shared data context.
