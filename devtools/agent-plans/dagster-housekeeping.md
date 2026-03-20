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

---

## P1-E3 Current State (as of 2026-03-20)

### What is already done

- `pixi run ferc` and `pixi run pudl` pixi tasks use `dg launch` exclusively.
- `dg_full.yml`, `dg_fast.yml`, and `dg_pytest.yml` config files exist in
  `src/pudl/package_data/settings/` and work correctly with `dg launch`.
- Integration test `conftest.py` already runs the ETL via `dg launch --job pudl`
  (in `_pudl_etl()`), not via the legacy CLIs.
- `DatasetSettingsResource`, `FercToSqliteSettingsResource`, and `DatastoreResource`
  have been migrated to typed `ConfigurableResource` classes that accept
  `etl_settings_path`.
- FERC IO managers (`FercDbfSQLiteDagsterIOManager`, `FercXbrlSQLiteDagsterIOManager`)
  and `PudlMixedFormatIOManager` have been migrated to `ConfigurableIOManager`.

### What still needs to be done for P1-E3

The following tasks must be completed before `pudl_etl` and `ferc_to_sqlite` can be
removed as project console scripts.

#### P1-E3-T1: Fix broken run config in `ferc_to_sqlite/cli.py` (immediate bug)

`pudl/ferc_to_sqlite/cli.py` still passes `etl_settings.ferc_to_sqlite_settings.model_dump()`
as the resource run config. After the `FercToSqliteSettingsResource` migration, the
resource now expects `{"etl_settings_path": "..."}` — the `model_dump()` dict will
fail at runtime. The CLI is therefore currently broken.

Options:

- Fix it to pass `etl_settings_path` (minimal patch to unblock any lingering users).
- Skip the fix and go straight to T3 (retire the CLI entirely), since no automated
    system calls it anymore.

The same issue exists in `pudl/etl/cli.py` for `dataset_settings`, which still passes
`etl_settings.datasets.model_dump()`.

**Recommendation:** skip the fix and retire both CLIs directly (T3/T4).

#### P1-E3-T2: Decide whether nightly needs a separate `dg_nightly.yml`

The `gcp_pudl_etl.sh` nightly script currently passes `--workers 8` to `ferc_to_sqlite`
and `--loglevel DEBUG` to both CLIs. The `dg_full.yml` config has `xbrl_num_workers: null`
(saturates CPUs) and `log_level: INFO`.

Options for the nightly script:

- Use `dg_full.yml` as-is (null workers = use all CPUs, INFO logging, matches laptop behavior).
- Create `dg_nightly.yml` that sets `xbrl_num_workers: 8` and `log_level: DEBUG`.

**Recommendation:** create `dg_nightly.yml` with explicit worker count and DEBUG logging
so nightly behavior is explicit and independent of the default profile.

#### P1-E3-T3: Replace `ferc_to_sqlite` and `pudl_etl` calls in `gcp_pudl_etl.sh`

The `run_pudl_etl()` function in `docker/gcp_pudl_etl.sh` currently runs:

```bash
ferc_to_sqlite --loglevel DEBUG --workers 8 "$PUDL_SETTINGS_YML"
pudl_etl --loglevel DEBUG "$PUDL_SETTINGS_YML"
```

These should be replaced with:

```bash
# Clean stale FERC SQLite DBs (mirrors what pixi run ferc does)
rm -f $PUDL_OUTPUT/ferc*.sqlite $PUDL_OUTPUT/ferc*.duckdb \
    $PUDL_OUTPUT/ferc*_xbrl_datapackage.json $PUDL_OUTPUT/ferc*_xbrl_taxonomy_metadata.json
rm -f $PUDL_OUTPUT/pudl.sqlite
alembic upgrade head
dg launch --job pudl --config-file $DG_NIGHTLY_CONFIG
```

Note: the `pudl` job (`all() - ferceqr`) already includes the `raw_ferc_to_sqlite`
asset group, so a single `dg launch --job pudl` replaces both the `ferc_to_sqlite` and
`pudl_etl` invocations.

The `$PUDL_SETTINGS_YML` env var referenced in the script can be removed from the Docker
environment once both CLIs are gone.

#### P1-E3-T4: Remove `pudl_etl` and `ferc_to_sqlite` console script entry points

In `pyproject.toml`, remove:

- `ferc_to_sqlite = "pudl.ferc_to_sqlite.cli:main"`
- `pudl_etl = "pudl.etl.cli:pudl_etl"`

#### P1-E3-T5: Delete `pudl/etl/cli.py`

Remove the module containing `pudl_etl_job_factory`, the `pudl_etl` click command,
and all `build_reconstructable_job` / `execute_job` logic. This also removes the dead
`publish_destinations` post-run upload logic (which defaults to `[]` in all settings
files and is already superseded by the `save_outputs_to_gcs()` function in
`gcp_pudl_etl.sh`).

#### P1-E3-T6: Delete `pudl/ferc_to_sqlite/cli.py`

Remove the module containing `ferc_to_sqlite_job_factory` and the `main` click command.

#### P1-E3-T7: Remove legacy op graph and resources from `ferc_to_sqlite/__init__.py`

The `@graph ferc_to_sqlite()`, `FERC1_DBF_OP` (and sibling ops), and `default_resources_defs`
are only consumed by `ferc_to_sqlite/cli.py`. Once that module is deleted they become dead
code and should be removed.

#### P1-E3-T8: Remove `get_dagster_execution_config` from `pudl/helpers.py`

This helper is only called by the two CLI modules and by `pudl/etl/__init__.py` to build
the default job config (where it can be inlined or replaced with a YAML config reference
after the CLI is gone). Remove or relocate once the CLIs are deleted.

#### P1-E3-T9: Update runbook and developer documentation

- Update `docs/dev/run_the_etl.rst` (and any other developer docs) to replace all
  references to `pudl_etl` and `ferc_to_sqlite` CLI commands with `dg launch` equivalents.
- Add a brief migration note to `docs/release_notes.rst`.

### P1-E3 Verification

- `pixi run dg check defs` passes.
- `pixi run ferc` and `pixi run pudl` succeed end-to-end.
- `pytest-integration` CI job passes (already uses `dg launch`).
- `gcp_pudl_etl.sh` dry-run or staging run succeeds with only `dg launch`.
- Confirm `pudl_etl` and `ferc_to_sqlite` are no longer importable or executable.

---

## P1-E3 Follow-up PR: Clean Up Interim Compatibility Layers

The work done in the resource/IO manager migration (P1-E2) intentionally preserved some
compatibility shims. Once P1-E3 is merged, a follow-up PR should eliminate this technical
debt.

### FU-1: Collapse FERC IO manager wrapper classes

`FercDbfSQLiteDagsterIOManager` wraps `FercDBFSQLiteIOManager` and
`FercXbrlSQLiteDagsterIOManager` wraps `FercXBRLSQLiteIOManager`. The inner classes exist
to support non-Dagster construction paths in debug helpers and legacy tests. Once those
paths are removed, both pairs of classes should be collapsed into single `ConfigurableIOManager`
subclasses.

### FU-2: Refactor `extract/ferc1.py` debug helpers

`extract_dbf()` and `extract_xbrl()` in `pudl/extract/ferc1.py` use `model_copy(update=...)`
to inject a `DatasetsSettings` object into the IO manager at construction time, bypassing
Dagster's resource wiring. These helpers should either be deleted (if testing via `dg launch`
is sufficient) or rewritten to use Dagster-native test execution patterns.

### FU-3: Refactor `TestFerc1ExtractDebugFunctions` integration tests

`test/integration/etl_test.py` tests the debug helpers directly. Once FU-2 is resolved,
these tests should be replaced by smoke tests that exercise the canonical FERC extraction
assets via `materialize_to_memory` or `dg launch`, rather than testing non-Dagster
construction paths.

### FU-4: Refactor `_engine_from_io_manager` in `test/conftest.py`

The `_engine_from_io_manager` helper and the `ferc1_engine_dbf`, `ferc1_engine_xbrl`,
`ferc714_engine_xbrl` fixtures use `model_copy(update={"dataset_settings": ...})` to inject
settings. After the wrapper layer is collapsed (FU-1), this pattern can be simplified or
replaced with a fixture that reads the engine from the built SQLite file path directly.

### FU-5: Remove `publish_destinations` from `EtlSettings`

`publish_destinations: list[str] = []` in `pudl/settings.py` is dead code: it defaults to
`[]` in all packaged settings files, and the nightly deployment script handles publishing
separately. Remove the field and any associated logic once the `pudl_etl` CLI is deleted.

---

## P1-E3 Sequencing

```
T1 (optional bug-fix) → T2 (nightly config) → T3 (update gcp script) → T4/T5/T6/T7/T8/T9 (removal)
FU-1 → FU-2 → FU-3 → FU-4 → FU-5  (follow-up PR after T9)
```

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

## Changelog

- 2026-03-07: Initial draft created with detailed Phase 1 and conceptual Phase 2/3.
- 2026-03-08: Reordered Phase 1 to prioritize CLI migration (`dg launch`) and deferred detailed planning for later phases.
- 2026-03-08: Clarified that P1-E1 and P1-E2 are interdependent tracks; softened tone for a more informal, cooperative style.
- 2026-03-20: Added P1-E3 current-state analysis and concrete remaining task list (T1–T9) plus follow-up PR items (FU-1–FU-5) based on post-P1-E2 state of the repository.
