# PUDL Dagster Housekeeping Plan 🚀

## Purpose

This is a working draft for modernizing PUDL's Dagster setup while keeping the ETL
reliable. It is meant to help with:

- Human planning and execution.
- Agent-assisted implementation.
- Straightforward translation into GitHub epics and tasks.

Associated GitHub tracking:

- Overall refactor epic: Issue #5066
- Epic 1: Issue #5120, PR #5071
- Epic 2: Issue #5118

Context shaping this plan:

- PUDL can run a full ETL in a couple of hours on a laptop.
- Source data changes infrequently (monthly at most, often yearly).
- Full outputs are rebuilt nightly and published as static Parquet in S3.
- So for now, we care most about simplification and consistency, not fancy orchestration features.

## Scope and Guardrails

- In scope:
  - Clean up the completed `dg` cutover and make project organization easier to maintain.
  - Dagster project ergonomics and modularity.
  - `dg`-native structure and workflows.
  - Incremental improvements that reduce maintenance burden.
- Out of scope (for this refactor):
  - Detailed planning for major future work beyond the epics listed here.
  - Major rewrites of asset logic
  - Large semantic changes to the asset graph or execution model
- Guardrails:
  - Keep the system running while we migrate.
  - Prefer additive changes and temporary compatibility shims when helpful.
  - Minimize disruption to active data/modeling work.

## Project Snapshot (2026-03-23)

- `pixi run dg check defs` passes.
- Current inventory from `pixi run dg list defs --json`:
  - 749 assets
  - 362 asset checks
  - 4 jobs: `ferc_to_sqlite`, `ferceqr`, `pudl`, `pudl_with_ferc_to_sqlite`
  - 11 resources: `datastore`, `zenodo_dois`, `pudl_io_manager`,
    `ferc1_dbf_sqlite_io_manager`, `ferc1_xbrl_sqlite_io_manager`,
    `ferc714_xbrl_sqlite_io_manager`, `etl_settings`, `runtime_settings`,
    `parquet_io_manager`, `geoparquet_io_manager`, `ferceqr_extract_settings`
  - 1 sensor: `ferceqr_sensor`
  - 0 schedules
- Epic 1, the execution-path migration, is now landed on this branch.
  GitHub tracking: Issue #5120, PR #5071.
  - The legacy `pudl_etl` and `ferc_to_sqlite` CLI modules have been deleted.
  - Dagster config profiles now live in
    `src/pudl/package_data/settings/` as `dg_fast.yml`, `dg_full.yml`,
    `dg_pytest.yml`, and `dg_nightly.yml`.
  - Local development, integration-test prebuilds, and nightly runs are now
    standardized around pixi tasks that delegate to `dg launch`.
  - `docker/gcp_pudl_etl.sh` now runs the nightly build as
    `pixi run pudl-with-ferc-to-sqlite-nightly` followed by split nightly
    pytest stages.
  - The integration harness uses Dagster-native prebuilds and preserves
    `DAGSTER_HOME` for `--live-pudl-output` runs so provenance-aware tests can
    see the existing nightly materializations.
- Dagster-native typed configuration is part of the baseline now:
  - Core ETL resources use `ConfigurableResource` patterns.
  - The FERC and mixed-format IO managers use `ConfigurableIOManager` patterns.
  - FERC SQLite provenance compatibility checks are part of the execution
    contract for live-output and downstream asset reads.
- Structural cleanup is still pending:
  - The `src/pudl/defs/` package still exists primarily as a compatibility shim
    for the `dg` project layout.
  - Definitions are still assembled primarily in `src/pudl/etl/__init__.py`.
- Observed metadata gaps in assets/checks remain a good target for follow-up:
  - 9 / 749 assets have tags.
  - 0 / 749 assets have owners.
  - 0 / 749 assets have kinds.
  - 622 / 749 assets have descriptions.
  - 18 / 362 asset checks have descriptions.

## Guiding Principles 🤝

- Keep migration incremental, reversible, and measurable.
- Prefer changes that let local development and nightly builds run the same way.
- Move toward current Dagster best practices without forcing a one-shot redesign.
- Favor convention over bespoke wiring when `dg` supports a standard pattern.
- Capture key decisions in plain language inside each epic or issue.

## Current Status

The first epic is complete, and the character of the remaining work has
changed.

- Epic 1 is complete on this branch.
- The original launch-path migration is no longer the main risk area.
- The remaining work is now cleanup and structural follow-through:
  1. remove the temporary compatibility layers we kept to make the cutover safe.
  2. make `src/pudl/defs/` a real source of truth rather than a shim.
  3. break apart centralized definitions assembly into smaller domain modules.
  4. establish a minimum metadata baseline for graph navigation and ownership.

One important direction change from the earliest version of this plan:

- In practice, the stable user-facing interface ended up being pixi tasks that wrap
  canonical `dg launch` commands, rather than asking humans and automation to call
  raw `dg launch` everywhere.
- The full-build nightly path now standardizes on
  `pudl_with_ferc_to_sqlite` plus split nightly pytest stages.
- We intentionally deferred broad `defs` reorganization until after execution-path
  stabilization, and that still looks like the right sequencing.

## Remaining Exit Criteria

- No temporary compatibility shims remain from the CLI-to-`dg` migration.
- The current pixi/`dg` launch contract is documented and preserved through the
  remaining refactors.
- `src/pudl/defs/` no longer exists only as a compatibility placeholder.
- Definition assembly is easier to navigate, with smaller domain-oriented modules.
- A minimal metadata contract exists for owners/tags/kinds/check descriptions, with
  some enforcement on newly touched definitions.

## Remaining Epic Sequencing

1. Epic 2: remove interim compatibility layers left behind by the cutover and absorb
  the low-risk `defs` source-of-truth cleanup.
2. Epic 3: decompose centralized ETL definition assembly.
3. Epic 4: establish a metadata and ownership baseline.

## Main Caveats to Manage Going Forward

- Caveat 1: Preserve the current pixi/`dg` contract.
  - Risk: structural cleanup accidentally changes how local, pytest, or nightly runs
    are invoked.
  - Mitigation: treat current pixi tasks and config files as the external contract and
    keep smoke tests around them.
- Caveat 2: Avoid graph drift during refactors.
  - Risk: moving definitions between modules changes asset keys, group names,
    resources, or job membership unintentionally.
  - Mitigation: compare `dg list defs --json` inventories before and after each step.
- Caveat 3: Preserve provenance-aware test behavior.
  - Risk: cleanup around IO managers or tests breaks the Dagster provenance contract
    for live-output runs.
  - Mitigation: keep targeted integration coverage for FERC SQLite provenance and
    nightly-style test execution.
- Caveat 4: Keep env/config semantics stable across contexts.
  - Risk: refactors blur the current separation between fast/full/pytest/nightly
    Dagster config profiles.
  - Mitigation: retain those config profiles as explicit artifacts and document any
    consolidation carefully.

## Epic 2: Clean Up Interim Compatibility Layers

GitHub tracking: Issue #5118.

The work done during Epic 1 intentionally preserved some compatibility shims. Now that
the execution-path migration is complete on this branch, Epic 2 should
eliminate this technical debt.

This epic now also includes the relatively small `defs` source-of-truth cleanup.
That work looks like straightforward module movement and registry cleanup rather than a
large or high-risk refactor, so it fits better here than as a standalone epic.

The FERC SQLite provenance module itself and the typed resource definitions do not
currently look like primary debt hotspots. The main cleanup need is to simplify the
wrapper, override, and fixture layers around them while preserving the current
provenance contract.

For Epic 2, we are explicitly taking the Dagster-first path. The goal is not just to
remove compatibility shims, but to refactor the surrounding tests and utilities so
they also use canonical Dagster patterns and abstractions rather than ad hoc
construction paths.

### Task 1: Collapse FERC IO manager wrapper classes

`FercDbfSQLiteDagsterIOManager` wraps `FercDBFSQLiteIOManager` and
`FercXbrlSQLiteDagsterIOManager` wraps `FercXBRLSQLiteIOManager`. The inner classes exist
to support non-Dagster construction paths in debug helpers and legacy tests. Once those
paths are removed, both pairs of classes should be collapsed into single `ConfigurableIOManager`
subclasses.

Direction:

- Delete the inner classes entirely and move the real logic onto the Dagster-facing
  classes.
- Stop supporting non-Dagster construction paths that require nested resource
  dependencies inside wrapper classes.

### Task 2: Refactor `extract/ferc1.py` debug helpers

`extract_dbf()` and `extract_xbrl()` in `pudl/extract/ferc1.py` use `model_copy(update=...)`
to inject a `DatasetsSettings` object into the IO manager at construction time, bypassing
Dagster's resource wiring. These helpers should be rewritten or removed so that debug
and notebook-oriented access follows Dagster-first execution patterns instead of
patching resource state by hand.

Direction:

- Prefer Dagster-native execution helpers such as `materialize_to_memory` or other
  explicit Definitions-based execution paths.
- If a helper survives, it should construct or consume canonical Dagster resources
  rather than mutating IO manager instances with `model_copy(update=...)`.

### Task 3: Refactor `TestFerc1ExtractDebugFunctions` integration tests

`test/integration/etl_test.py` tests the debug helpers directly. Once Task 2 is resolved,
these tests should be replaced by smoke tests that exercise the canonical FERC extraction
assets via `materialize_to_memory` or `dg launch`, rather than testing non-Dagster
construction paths.

### Task 4: Refactor `_engine_from_io_manager` in `test/conftest.py`

The `_engine_from_io_manager` helper and the `ferc1_engine_dbf`, `ferc1_engine_xbrl`,
`ferc714_engine_xbrl` fixtures use `model_copy(update={"dataset_settings": ...})` to inject
settings. After the wrapper layer is collapsed in Task 1, this pattern can be simplified or
replaced with a fixture that reads the engine from the built SQLite file path directly.

Direction:

- Replace these fixture helpers with Dagster-first patterns.
- Prefer a canonical fixture helper that initializes configured `Definitions` and the
  relevant resources once, and only fall back to direct file-path access where that is
  clearly simpler and still aligned with the production contract.

### Task 5: Simplify `build_defs()` resource override plumbing

`src/pudl/etl/__init__.py` currently contains a special-case resource override block
that rebuilds the FERC SQLite IO managers whenever `etl_settings` is overridden. The
code comment already marks this as a temporary workaround. Once the wrapper IO managers
are gone, this override logic should be removed in favor of generic resource merging.

Direction:

- Remove the special cases after Task 1 and rely on normal Dagster resource override
  behavior.
- If any extra setup is still needed for tests, keep it in test-only utilities rather
  than in production definitions assembly.

### Task 6: Simplify integration-test fixture layering in `test/conftest.py`

`test/conftest.py` absorbed a large amount of orchestration logic during the refactor:
`dg launch` prebuilds, `DAGSTER_HOME` management, live-output safeguards, config-file
resolution, and ad hoc `build_defs()` construction for asset value loading. Much of
this is valid, but the file is now a hotspot and likely harder to reason about than it
needs to be.

Direction:

- Introduce a single canonical Dagster-first test-session builder or equivalent helper
  that returns configured `Definitions`, `DagsterInstance`, and path context.
- Refactor fixtures to compose from that builder rather than reassembling Dagster
  pieces in several places.

### Task 7: Make `src/pudl/defs/` the real definitions registry

`src/pudl/defs/__init__.py` is still a placeholder, even though the `dg` project layout
is now part of the stable interface. This work looks small and low-risk: mostly moving
the existing registry wiring into the package that is already supposed to own it, while
keeping `dg list defs --json` unchanged.

Direction:

- Move the actual definitions assembly into `src/pudl/defs/` and keep only thin
  compatibility re-exports from `src/pudl/etl/__init__.py` during the transition.
- Make `src/pudl/defs/__init__.py` the canonical registry entry point.

### Task 8: Trim temporary compatibility imports once `defs` is canonical

After Task 7, remove or minimize compatibility indirection so there is one obvious
place to look for the canonical Dagster registry and its supporting module structure.
This should include verifying that inventory and execution behavior are unchanged after
the move.

Direction:

- Prefer one canonical import path and one canonical registry entry point.
- Remove transitional compatibility imports once tests and utilities have moved to the
  Dagster-first path.

### Epic 2 Verification

- `pixi run dg check defs`
- `pixi run dg list defs --json` inventory unchanged except for documented metadata-only
  deltas.
- Targeted integration tests still pass for live-output and prebuild paths.
- FERC SQLite provenance compatibility checks still protect downstream reads.

---

## Epic 3: Decompose Monolithic ETL Definition Assembly

### Epic 3 Objective

Break apart centralized module registration logic into cohesive domain modules once launch pathways are standardized.

### Epic 3 Suggested Tasks

- Task 1: Extract raw/core/out grouping config into dedicated modules.
  - Deliverable: small, focused grouping modules with clear ownership.
- Task 2: Introduce compositional assembly layer that imports domain registries.
  - Deliverable: easier-to-read top-level definitions wiring.
- Task 3: Preserve current job semantics (`ferc_to_sqlite`, `pudl`,
  `pudl_with_ferc_to_sqlite`, `ferceqr`) while relocating wiring.
  - Deliverable: no unintended job selection regressions.
- Task 4: Add regression test or snapshot test for selected asset keys/groups.
  - Deliverable: early warning on accidental graph drift.

### Epic 3 Verification

- `pixi run dg check defs`
- Targeted tests for asset/job registration shape.

## Epic 4: Metadata and Ownership Baseline

### Epic 4 Objective

Create and enforce a minimum metadata contract so the graph is navigable and maintainable.

### Epic 4 Suggested Tasks

- Task 1: Define metadata policy for `owners`, `kinds`, and `tags`.
  - Deliverable: short policy doc with examples and naming conventions.
- Task 2: Apply metadata to a pilot subset (for example 1-2 domains).
  - Deliverable: visible improvements in `dg list defs --json`.
- Task 3: Add automated check(s) for metadata completeness on changed definitions.
  - Deliverable: CI fails on missing required metadata fields for newly touched assets.
- Task 4: Backfill check descriptions for highest-value checks first.
  - Deliverable: top-priority checks have meaningful descriptions.

### Epic 4 Verification

- Programmatic query of `dg list defs --json` to report metadata completeness.
- CI check runs on pull requests touching Dagster definitions.

## Changelog

- 2026-03-07: Initial draft created with a detailed launch-path migration plan and later structural cleanup work.
- 2026-03-08: Reordered the early work to prioritize the `dg launch` migration and deferred broader structural planning.
- 2026-03-08: Clarified that the launch-command and config/resource tracks were interdependent and softened tone for a more informal, cooperative style.
- 2026-03-20: Added a concrete remaining-task list for the launch-path migration plus follow-up cleanup items based on the post-resource-migration state of the repository.
- 2026-03-23: Updated the plan after landing Epic 1 on this branch; refreshed the baseline snapshot, removed completed launch-path work, and refocused the plan on Epic 2 through Epic 4.
