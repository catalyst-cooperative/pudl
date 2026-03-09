---
name: pudl-dev
description: >
  Full-stack development guidance for the PUDL project, covering contributor workflows,
  local ETL and Dagster development, metadata/schema changes, dbt and pytest validation,
  and data-oriented documentation context (data access, data dictionaries, data sources,
  and methodology).
argument-hint: "[task] [dataset_or_table] [code|data|both]"
---

# PUDL Developer Guide

Use this skill for broad PUDL development tasks where code behavior, metadata, and data
products are tightly coupled.

## Trigger phrases

- "help me contribute to PUDL"
- "how do I run PUDL locally"
- "which commands should I run for this PUDL change"
- "where is the metadata for this table"
- "how do I validate this data change"
- "how does this PUDL table relate to source docs"
- "what docs should I update for this PUDL PR"
- "how do I release or operate PUDL builds"

## How to use this skill

1. Identify the primary workflow from the reference index below.
2. Read the relevant reference file(s) before answering.
3. Prefer repository-native commands (`pixi run ...`) and existing conventions.
4. For specialized workflows, also use related skills:
   - `pytest`
   - `dbt`
   - `code-quality`
   - `alembic`
   - `pudl` (data-access focused)
   - `dagster-expert`

## Shared data context

Data/metadata/source/methodology guidance is centralized in the `pudl` skill.
Use the linked "Shared" entries in the reference index below rather than
maintaining duplicates in `pudl-dev`.

## Canonical-source policy

The files in this skill are distilled for coding agents. Canonical project guidance
remains in PUDL documentation and source code. Each reference points to canonical docs.

## Reference index

- [Contributor Workflow](./references/contributor-workflow.md) - PR flow, contribution norms, communication, and planning.
- [Dev Setup and ETL](./references/dev-setup-and-etl.md) - environment setup, running ETL, datastore context, and local execution.
- [Testing and Validation](./references/testing-and-validation.md) - pytest scopes, dbt validation workflow, and quality checks.
- [Metadata and Schema](./references/metadata-and-schema.md) - metadata source-of-truth files, dbt schema coupling, and migrations.
- [Dagster and Assets](./references/dagster-and-assets.md) - Dagster-oriented project structure and asset orchestration context.
- [Shared: Data Access and Outputs](../pudl/references/data-access-and-outputs.md) - cloud/local output access patterns and output-layer usage.
- [Shared: Metadata and Querying](../pudl/references/metadata-and-querying.md) - machine-queryable metadata APIs and generated-doc fallbacks.
- [Shared: Data Sources, Dictionaries, and Methodology](../pudl/references/data-sources-dictionaries-and-methodology.md) - source coverage, dictionary semantics, and methodology interpretation.
- [Docs Generation Traceability](./references/docs-generation-traceability.md) - map generated artifacts to build hooks, templates, and metadata classes.
- [Releases and Operations](./references/releases-and-operations.md) - release workflow, nightly build context, and operational docs.
- [ID Mapping and Entity Linkages](./references/id-mapping-and-entity-linkages.md) - historical and practical guidance for cross-dataset entity matching.
