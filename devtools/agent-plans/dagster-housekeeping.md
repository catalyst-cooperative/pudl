# PUDL Dagster Housekeeping Plan (Draft) 🚀

## Purpose

This is a working draft for modernizing PUDL's Dagster setup while keeping the ETL reliable. It is meant to help with:

- Human planning and execution.
- Agent-assisted implementation.
- Straightforward translation into GitHub epics and sub-issues.

Context shaping this plan:

- PUDL can run a full ETL in about 1-2 hours on a laptop.
- Source data changes infrequently (monthly at most, often yearly).
- Full outputs are rebuilt nightly and published as static Parquet in S3.
- So for now, we care most about simplification and consistency, not fancy orchestration features.

## Scope and Guardrails

- In scope:
  - Phase 1 work only (for now): simplify how we run jobs and make project organization easier to maintain.
  - Dagster project ergonomics and modularity.
  - `dg`-native structure and workflows.
  - Incremental improvements that reduce maintenance burden.
- Out of scope (for this draft):
  - Detailed planning for Phase 2 and Phase 3 implementation.
  - Rewriting all asset logic at once.
  - Large behavioral changes to ETL semantics without validation.
- Guardrails:
  - Keep the system running while we migrate.
  - Prefer additive changes and temporary compatibility shims when helpful.
  - Minimize disruption to active data/modeling work.

## Baseline Snapshot (Today)

- `dg check defs` passes.
- The `src/pudl/defs/` package exists but is currently a compatibility placeholder.
- Definitions are assembled primarily in `src/pudl/etl/__init__.py`.
- Current inventory from `dg list defs --json`:
  - 656 assets
  - 310 asset checks
  - 5 jobs
  - 10 resources
  - 1 sensor
  - 0 schedules
- Observed metadata gaps in assets/checks:
  - Tags empty for all assets.
  - Owners empty for all assets.
  - Kinds empty for all assets.
  - Automation conditions unset for all assets.
  - Many check descriptions are missing.

## Guiding Principles 🤝

- Keep migration incremental, reversible, and measurable.
- Prefer changes that let local development and nightly builds run the same way.
- Move toward current Dagster best practices without forcing a one-shot redesign.
- Favor convention over bespoke wiring when `dg` supports a standard pattern.
- Capture key decisions in plain language inside each epic or issue.

## Migration Overview

- Phase 1: Execution-path simplification and foundational reorganization (detailed, implementation-ready).
- Phase 2 and Phase 3: Deferred until Phase 1 completion and retrospective.

## Phase 1 (Detailed): CLI-First Simplification and `dg` Foundations

### Phase 1 Goals

- Replace custom Dagster launch scripts with `dg launch`-first workflows while preserving behavior.
- Consolidate parameter handling into idiomatic Dagster config/resources rather than custom CLI glue.
- Ensure local development and nightly builds execute with the same invocation patterns.
- Improve confidence via targeted validation and smoke tests.
- Start structural cleanup only after launch-path migration is stable.

### Phase 1 Exit Criteria

- The `pudl.etl.cli` and `pudl.ferc_to_sqlite.cli` launch pathways are retired or reduced to thin compatibility wrappers.
- Day-to-day runbooks for local and nightly runs are based on `dg launch` plus shared config files.
- Parameters currently collected by custom scripts are represented in Dagster-native configuration patterns.
- CI includes lightweight `dg` validation/smoke checks.
- Structural reorganization in `src/pudl/defs/` has started, with clear next steps and no execution regressions.

### Important Dependency Note: P1-E1, P1-E2, and P1-E3

`dg launch` cutover and config/resource modeling are coupled work.

- We can start P1-E1 first (inventory and canonical command design).
- P1-E2 then provides the config/resource plumbing those commands depend on.
- P1-E3 is where we finish cutover and deprecate old launch paths.

### Phase 1 Epic Breakdown

## Epic P1-E1: Define Canonical `dg launch` Run Modes

### Objective

Define canonical `dg launch` commands for all primary run modes so everyone is using the same playbook.

### Suggested Sub-Issues

- P1-E1-S1: Inventory all run modes currently handled by custom launch scripts.
  - Deliverable: matrix of command, parameters, defaults, and consumers (dev, CI, nightly).
- P1-E1-S2: Define canonical `dg launch` commands for each run mode.
  - Deliverable: reproducible command set using `--job`, `--assets`, `--config-file`, and partition flags where needed.
- P1-E1-S3: Capture expected behavior per mode (runtime, outputs, key logs, failure behavior).
  - Deliverable: parity checklist to validate later cutover work.

### Verification

- `pixi run dg check defs`
- `pixi run dg list defs --json`
- Canonical command matrix reviewed and approved.

## Epic P1-E2: Dagster-Native Parameter and Config Modeling

### Objective

Represent script-collected parameters in Dagster-native config/resources so CLI migration stays maintainable.

### Suggested Sub-Issues

- P1-E2-S1: Classify each launch parameter as job config, resource config, env var, or execution setting.
  - Deliverable: parameter-to-Dagster mapping table.
- P1-E2-S2: Normalize config file structure used by `dg launch` commands.
  - Deliverable: minimal duplicated config across ETL modes.
- P1-E2-S3: Refactor legacy resources toward typed config where practical during this phase.
  - Deliverable: reduced reliance on ad hoc script-side parameter handling.
- P1-E2-S4: Document secrets handling and environment variable injection for local and CI/nightly contexts.
  - Deliverable: one source of truth for runtime config behavior.

### Verification

- `pixi run dg check defs`
- Targeted tests for config loading and expected run-config resolution.

## Epic P1-E3: Cut Over to `dg launch` and Retire Legacy Launch Paths

### Objective

Switch local/nightly execution to canonical `dg launch` commands and retire (or reduce) old script-based launch paths.

### Suggested Sub-Issues

- P1-E3-S1: Implement temporary compatibility wrappers/aliases that call canonical `dg` commands.
  - Deliverable: no operational interruption during transition.
- P1-E3-S2: Run parity checks against the behavior checklist from P1-E1.
  - Deliverable: confidence that `dg launch` behavior matches existing workflows.
- P1-E3-S3: Migrate nightly automation to canonical `dg` commands.
  - Deliverable: nightly runs succeed without custom launch scripts.
- P1-E3-S4: Remove or deprecate direct `execute_job` launch paths.
  - Deliverable: custom launch logic minimized or retired.

### Verification

- Command parity tests: old script invocation versus new `dg launch` invocation.
- Nightly dry-run or staging run using only canonical `dg` commands.
- Cutover checklist complete with rollback notes.

## Epic P1-E4: Establish `defs` as the Source of Truth (After CLI Parity)

### Objective

Move definition registration from legacy central assembly toward `src/pudl/defs/` after launch-path behavior is stable.

### Suggested Sub-Issues

- P1-E4-S1: Design target `defs` package layout.
  - Deliverable: agreed module map (for example: `assets/`, `jobs/`, `resources/`, `automation/`).
- P1-E4-S2: Create initial `defs` modules and wire existing definitions through them.
  - Deliverable: `dg list defs` output unchanged in key counts and names (or documented deltas).
- P1-E4-S3: Replace placeholder narrative in `src/pudl/defs/__init__.py` with actual registry behavior.
  - Deliverable: code comments and module docstrings reflect real structure.
- P1-E4-S4: Add compatibility shim if needed for existing imports.
  - Deliverable: downstream imports continue to work during transition.

### Verification

- `pixi run dg check defs`
- `pixi run dg list defs --json`
- Compare before/after inventory (counts and expected key names).

## Epic P1-E5: Decompose Monolithic ETL Definition Assembly

### Objective

Break apart centralized module registration logic into cohesive domain modules once launch pathways are standardized.

### Suggested Sub-Issues

- P1-E5-S1: Extract raw/core/out grouping config into dedicated modules.
  - Deliverable: small, focused grouping modules with clear ownership.
- P1-E5-S2: Introduce compositional assembly layer that imports domain registries.
  - Deliverable: easier-to-read top-level definitions wiring.
- P1-E5-S3: Preserve current job semantics (`etl_fast`, `etl_full`, `ferceqr_etl`) while relocating wiring.
  - Deliverable: no unintended job selection regressions.
- P1-E5-S4: Add regression test or snapshot test for selected asset keys/groups.
  - Deliverable: early warning on accidental graph drift.

### Verification

- `pixi run dg check defs`
- Targeted tests for asset/job registration shape.

## Epic P1-E6: Metadata and Ownership Baseline (After Structural Moves)

### Objective

Create and enforce a minimum metadata contract so the graph is navigable and maintainable.

### Suggested Sub-Issues

- P1-E6-S1: Define metadata policy for `owners`, `kinds`, and `tags`.
  - Deliverable: short policy doc with examples and naming conventions.
- P1-E6-S2: Apply metadata to a pilot subset (for example 1-2 domains).
  - Deliverable: visible improvements in `dg list defs --json`.
- P1-E6-S3: Add automated check(s) for metadata completeness on changed definitions.
  - Deliverable: CI fails on missing required metadata fields for newly touched assets.
- P1-E6-S4: Backfill check descriptions for highest-value checks first.
  - Deliverable: top-priority checks have meaningful descriptions.

### Verification

- Programmatic query of `dg list defs --json` to report metadata completeness.
- CI check runs on pull requests touching Dagster definitions.

### Main Caveats to Manage

- Caveat 1: Preserve run-config parity.
  - Risk: subtle differences between script-built run config and `dg launch --config-file` behavior.
  - Mitigation: maintain a parity matrix and test old/new invocation pairs.
- Caveat 2: Preserve execution semantics.
  - Risk: differences in job selection, op selection, or concurrency defaults.
  - Mitigation: explicit command standards and snapshot of effective run config.
- Caveat 3: Secrets/env contract consistency.
  - Risk: local, CI, and nightly environments resolve settings differently.
  - Mitigation: formalize env var contract and validate with smoke runs in each environment.
- Caveat 4: Temporary dual-path complexity.
  - Risk: supporting both old scripts and new commands for too long.
  - Mitigation: planned deprecation window with concrete removal criteria.

### Recommended Gate Before Structural Refactors

Only start broad `defs`/module reorganization after these conditions are met:

- All primary run modes have canonical `dg launch` commands.
- Nightly build executes successfully without relying on custom launch code.
- Basic smoke tests validate config parity and successful definitions loading.

### Phase 1 Recommended Sequencing

1. P1-E1: define canonical `dg launch` run modes.
2. P1-E2: parameter/config/resource modeling for those run modes.
3. P1-E3: cut over to `dg launch` and deprecate legacy launch paths.
4. P1-E4: `defs` source of truth.
5. P1-E5: decompose ETL assembly.
6. P1-E6: metadata baseline.

### Phase 1 Risks and Mitigations

- Risk: definition drift changes execution unintentionally.
  - Mitigation: snapshot/inventory checks before and after each epic.
- Risk: new `dg launch` commands do not fully preserve script behavior.
  - Mitigation: side-by-side run parity validation and staged rollout.
- Risk: migration churn blocks feature work.
  - Mitigation: narrow PRs, compatibility shims, and incremental rollout.
- Risk: unclear ownership of new modules.
  - Mitigation: enforce owners metadata and codeowners alignment.

## Epic and Issue Translation Guide

Use this mapping to convert plan sections into GitHub tracking artifacts.

### Epic Template Mapping

- Epic title: `[Dagster Housekeeping][Phase X] <Epic name>`
- Epic body sections:
  - Objective
  - Scope
  - Non-goals
  - Deliverables
  - Verification
  - Risks
  - Dependencies
  - Definition of done

### Sub-Issue Template Mapping

- Sub-issue title: `[P1-E#-S#] <Task name>`
- Required fields:
  - Parent epic link
  - Why this task exists
  - Implementation notes
  - Acceptance criteria
  - Test/validation commands

## Suggested Labels

- Labels:
  - `dagster`
  - `epic`
  - `developer experience`

## Tracking Metrics

Track these during Phase 1:

- Definition health:
  - `dg check defs` pass rate.
  - Definition load time trend.
- Structural progress:
  - Number of launch pathways migrated from custom scripts to canonical `dg launch` commands.
  - Number of definitions wired through `src/pudl/defs/` modules.
  - Reduction in central assembly complexity.
- Metadata quality:
  - Percent of assets with owners/tags/kinds.
  - Percent of checks with non-empty descriptions.
- Operational confidence:
  - Smoke-check stability in CI.
  - Mean time to diagnose orchestration failures.

## Open Questions for Kickoff 💬

- Which run mode should be the first `dg launch` parity pilot (fast ETL, full ETL, or `ferc_to_sqlite`)?
- How much backward compatibility is required for existing CLI users and automations?
- What is the acceptable deprecation window for script-based launch paths?
- Who wants to steward the parameter/config docs and keep them in good shape over time?

## Changelog

- 2026-03-07: Initial draft created with detailed Phase 1 and conceptual Phase 2/3.
- 2026-03-08: Reordered Phase 1 to prioritize CLI migration (`dg launch`) and deferred detailed planning for later phases.
- 2026-03-08: Clarified that P1-E1 and P1-E2 are interdependent tracks; softened tone for a more informal, cooperative style.
