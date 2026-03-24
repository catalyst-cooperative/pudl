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

Relevant code paths:

- `src/pudl/io_managers.py`
- `src/pudl/etl/__init__.py`
- `src/pudl/extract/ferc1.py`
- `test/conftest.py`
- `test/unit/io_managers_test.py`

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

Relevant code paths:

- `src/pudl/extract/ferc1.py`
- `src/pudl/io_managers.py`
- `src/pudl/etl/ferc_to_sqlite_assets.py`
- `test/integration/etl_test.py`
- `test/conftest.py`

### Task 3: Refactor `TestFerc1ExtractDebugFunctions` integration tests

`test/integration/etl_test.py` tests the debug helpers directly. Once Task 2 is resolved,
these tests should be replaced by smoke tests that exercise the canonical FERC extraction
assets via `materialize_to_memory` or `dg launch`, rather than testing non-Dagster
construction paths.

Relevant code paths:

- `test/integration/etl_test.py`
- `src/pudl/extract/ferc1.py`
- `src/pudl/etl/ferc_to_sqlite_assets.py`
- `test/conftest.py`

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

Relevant code paths:

- `test/conftest.py`
- `src/pudl/io_managers.py`
- `src/pudl/resources.py`
- `src/pudl/workspace/setup.py`

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

Relevant code paths:

- `src/pudl/etl/__init__.py`
- `src/pudl/io_managers.py`
- `test/conftest.py`
- `src/pudl/defs/__init__.py`

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

Relevant code paths:

- `test/conftest.py`
- `test/unit/conftest.py`
- `src/pudl/etl/__init__.py`
- `src/pudl/package_data/settings/dg_pytest.yml`
- `src/pudl/package_data/settings/dg_full.yml`
- `src/pudl/ferc_sqlite_provenance.py`

### Task 7: Make `src/pudl/defs/` the real definitions registry

`src/pudl/defs/__init__.py` is still a placeholder, even though the `dg` project layout
is now part of the stable interface. This work looks small and low-risk: mostly moving
the existing registry wiring into the package that is already supposed to own it, while
keeping `dg list defs --json` unchanged.

Direction:

- Move the actual definitions assembly into `src/pudl/defs/` and keep only thin
  compatibility re-exports from `src/pudl/etl/__init__.py` during the transition.
- Make `src/pudl/defs/__init__.py` the canonical registry entry point.

Relevant code paths:

- `src/pudl/defs/__init__.py`
- `src/pudl/etl/__init__.py`
- `src/pudl/definitions.py`
- `test/conftest.py`

### Task 8: Trim temporary compatibility imports once `defs` is canonical

After Task 7, remove or minimize compatibility indirection so there is one obvious
place to look for the canonical Dagster registry and its supporting module structure.
This should include verifying that inventory and execution behavior are unchanged after
the move.

Direction:

- Prefer one canonical import path and one canonical registry entry point.
- Remove transitional compatibility imports once tests and utilities have moved to the
  Dagster-first path.

Relevant code paths:

- `src/pudl/defs/__init__.py`
- `src/pudl/etl/__init__.py`
- `src/pudl/definitions.py`
- `test/conftest.py`
- `docs/dev/run_the_etl.rst`

### Epic 2 Verification

- `pixi run dg check defs`
- `pixi run dg list defs --json` inventory unchanged except for documented metadata-only
  deltas.
- Targeted integration tests still pass for live-output and prebuild paths.
- FERC SQLite provenance compatibility checks still protect downstream reads.

---

## Epic 3: Decompose Monolithic ETL Definition Assembly

### Epic 3 Objective

Break apart centralized module registration logic into cohesive domain modules once
launch pathways are standardized.

### Why metadata work matters now

Right now a large amount of Dagster registry wiring lives in one place. That works, but
it makes the code harder to scan, increases merge conflicts, and makes it harder to see
which assets, jobs, resources, and checks belong together. Splitting definitions is
preferable because it:

- makes ownership and intent easier to understand locally.
- reduces the amount of central boilerplate a contributor has to edit for small changes.
- makes it easier to test or inventory one slice of the graph without understanding the
  entire ETL registry at once.
- gives us a clearer place to attach metadata, documentation, and future automation at
  the domain level.
- aligns better with how Dagster projects are usually maintained once they outgrow a
  single registry module.

### How Dagster projects are typically organized

There is not one mandatory Dagster layout, but mature projects usually stop relying on
one giant definitions module. A common pattern is:

- use Python modules and packages to group together related assets, checks, resources,
  and jobs.
- keep the directory layout aligned with a primary mental model such as business domain,
  source system, or pipeline stage.
- use Dagster metadata for the secondary organizational dimensions:
  group names, asset key structure, tags, owners, and descriptions.

In other words, the filesystem usually represents the main way humans reason about the
project, while the Dagster graph itself carries the other dimensions needed for UI
navigation and filtering.

### Organizational options for PUDL

PUDL has at least three real axes of organization:

- ETL layer: `raw`, `core`, `out`
- dataset or source family: `eia923`, `ferc1`, `phmsagas`, etc.
- source format or ingestion mechanism: `xlsx`, `dbf`, `xbrl`, `csv`, `json`, etc.

There are several plausible ways to organize the repo more explicitly along Dagster
lines:

#### Option 1: Layer-first

Examples:

- `defs/raw/...`
- `defs/core/...`
- `defs/out/...`

Pros:

- matches the visual shape of the PUDL graph.
- easy to explain to new contributors in terms of pipeline stages.
- aligns naturally with existing asset group language.

Cons:

- spreads one dataset across many folders.
- makes it harder to work on a single source family end-to-end.
- tends to recreate a lot of cross-layer imports for one domain.

#### Option 2: Dataset-first

Examples:

- `defs/eia923/raw.py`, `defs/eia923/core.py`, `defs/eia923/out.py`
- `defs/ferc1/raw.py`, `defs/ferc1/core.py`, `defs/ferc1/out.py`

Pros:

- keeps most related logic for one source family together.
- maps well to how contributors usually think about PUDL work and issue ownership.
- makes it easier to add or change one dataset without touching the rest of the tree.

Cons:

- can make cross-dataset shared infrastructure less obvious.
- requires discipline so common helpers do not get copied into each dataset package.

#### Option 3: Format-first

Examples:

- `defs/xbrl/...`
- `defs/dbf/...`
- `defs/csv/...`

Pros:

- useful when extraction technology is the dominant source of complexity.
- can work well for raw ingestion assets and shared parsers.

Cons:

- format usually stops being the main concern after extraction.
- awkward for downstream `core` and `out` assets, where business meaning matters more
  than file format.
- likely too low-level to serve as the primary top-level organization for all of PUDL.

#### Option 4: Hybrid

Examples:

- dataset-first at the top level
- layer-specific modules inside each dataset package
- format-specific submodules only where they actually matter, mostly in raw ingestion

Pros:

- fits the real shape of PUDL better than a single-axis layout.
- keeps end-to-end dataset work together while still exposing raw/core/out structure.
- allows formats like XBRL and DBF to be explicit where they are genuinely important.

Cons:

- requires a little more upfront design discipline.
- can drift into inconsistency if the conventions are not documented.

### What likely makes the most sense for PUDL

At the moment, a hybrid layout looks like the best fit:

- organize primarily by dataset or source family.
- keep layer distinctions inside those dataset packages.
- reserve format-specific modules for raw ingestion and other places where format is a
  first-order concern.

That approach matches how people usually reason about PUDL changes: most work starts
with a dataset family like FERC 1 or EIA 923, then touches its raw/core/out assets, and
only sometimes needs to care about whether the source came from XBRL, DBF, CSV, or
XLSX.

### How to decide the final structure

The key question is not "which axis exists?" All three axes exist. The question is
which axis should be primary in the filesystem and which should be represented through
Dagster metadata and naming.

For PUDL, the primary organizational dimension should be the one that best:

- matches how contributors pick up and scope work.
- minimizes edits to unrelated files when one dataset changes.
- keeps shared infrastructure visible without dominating the whole tree.
- preserves intuitive navigation in both the repo and the Dagster UI.

That decision should be validated by a small pilot refactor rather than a one-shot
reorganization of the whole registry.

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

Create and enforce a minimum metadata contract so the graph is navigable,
operationally legible, and easier to connect to PUDL's existing metadata systems.

### Why this is worth doing

PUDL is now large enough that asset names alone are no longer a sufficient discovery
mechanism. As the graph approaches one thousand assets, contributors need the Dagster
UI and `dg list defs --json` output to answer questions like:

- What is this asset?
- Who owns or understands it?
- Which source family or ETL layer does it belong to?
- Does it depend on XBRL, DBF, CSV, or some other ingestion path?
- Is it likely to be expensive in CPU, memory, or I/O?
- Which upstream data sources and transformation paths contributed to it?

Today, much of that information either does not exist in the Dagster graph or lives in
parallel PUDL metadata structures that are not explicitly projected onto the assets.
Epic 4 is about making the graph itself more informative without turning Dagster into a
second bespoke metadata system.

### The main kinds of metadata we care about

Epic 4 should distinguish several different metadata categories, because they serve
different purposes:

- Descriptive metadata:
  asset and asset-check descriptions that explain what something represents and why it
  exists.
- Organizational metadata:
  owners, tags, kinds, group names, and asset key structure used for filtering,
  navigation, and responsibility.
- Operational metadata:
  annotations that help classify assets by execution profile, such as high-memory,
  high-CPU, I/O-heavy, or tool-specific behavior.
- Runtime metadata:
  materialization-time facts like row counts, source versions, file paths, and date
  ranges that help with observability and debugging.
- Provenance metadata:
  information about upstream source families, derived or imputed outputs, and other
  transformation context that is not obvious from graph edges alone.
- Semantic/schema metadata:
  the table, field, and source metadata already modeled in `pudl.metadata`, which is
  used for documentation and machine-queryable metadata exports.

Not all of these belong in the same place. Some should live statically on Dagster
definitions, some should be emitted at materialization time, and some should remain
canonical in `pudl.metadata` and only be projected into Dagster.

### What problems this should help PUDL solve

PUDL has several concrete problems that better Dagster metadata would help address.

#### Problem 1: Resource usage is uneven across the graph

Some assets are lightweight and parallel-safe, while others are memory-hungry,
compute-heavy, or dominated by I/O. Today that knowledge is implicit and mostly lives
in contributors' heads.

Relevant metadata direction:

- Add operational tags or kinds describing execution profile.
- Optionally annotate major engine/tooling choices such as pandas, polars, and DuckDB.

This metadata will not by itself enforce resource limits, but it creates the stable
classification layer needed for future concurrency controls, execution pools, and
targeted scheduling decisions.

#### Problem 2: Different assets rely on different raw formats and internal tooling

PUDL spans DBF, XBRL, CSV, XLSX, JSON, and other ingestion paths. Internally it also
uses different processing tools depending on the workload.

Relevant metadata direction:

- Tag assets by source format where that distinction is meaningful.
- Tag or classify assets by primary implementation/tooling when useful.

This makes it easier to understand where specialized infrastructure is concentrated and
to search the graph by execution or ingestion technology rather than only by dataset
name.

#### Problem 3: Rich data descriptions already exist, but not on the graph

PUDL already maintains detailed table, field, and source descriptions in
`pudl.metadata`. Those descriptions drive docs generation and machine-queryable metadata
exports, but they are not systematically associated with the corresponding Dagster
assets.

Relevant metadata direction:

- Project existing PUDL resource descriptions onto Dagster asset descriptions wherever
  the mapping is clear.
- Use Dagster descriptions and related annotations to make the graph a better internal
  documentation surface for contributors.

The key point is to reuse existing metadata rather than manually rewriting it into a
second parallel system.

#### Problem 4: Provenance is broader than graph edges alone

Many PUDL assets combine multiple upstream source families, and some outputs include
estimated, allocated, or imputed values. Basic graph lineage is necessary, but it is
not always sufficient to explain the full provenance story.

Relevant metadata direction:

- Keep graph dependencies explicit in the asset graph.
- Add structured asset-level provenance annotations for major source families and other
  important transformation facts.
- Emit runtime metadata for selected outputs when source-version or transformation
  details are especially important to inspect after a run.

This does not require solving column-level provenance immediately. Asset-level
provenance is a useful and much more tractable first step.

#### Problem 5: Existing metadata fields are somewhat bespoke and uneven

PUDL's current metadata models, especially around data sources, contain fields that are
useful but not always sharply defined for orchestration, discovery, or ownership.

Relevant metadata direction:

- Define which metadata is canonical in `pudl.metadata`.
- Define which subset of that metadata should be projected into Dagster.
- Avoid keeping the same fact in two systems unless one is clearly generated from the
  other.

This is as much a modeling problem as a Dagster problem.

#### Problem 6: Discovery is getting harder as the graph grows

At the current scale, people need to be able to find assets by several dimensions, not
just by memorizing asset names.

Relevant metadata direction:

- Establish a small but durable baseline of descriptions, owners, tags, and kinds.
- Use tags for secondary dimensions such as ETL layer, dataset family, raw format,
  engine, or operational class.
- Backfill high-value asset-check descriptions so checks are understandable in the UI.

This is likely the highest immediate return-on-investment area for Epic 4.

### Design principles for Epic 4

- Do not duplicate the same fact in two places unless one copy is explicitly generated
  from the other.
- Keep semantic data definitions canonical in `pudl.metadata` where possible.
- Use Dagster metadata for graph navigation, ownership, operational classification, and
  selective provenance annotations.
- Distinguish static definition metadata from runtime materialization metadata.
- Start with a minimal, enforceable baseline rather than trying to encode every useful
  fact about every asset all at once.

### A practical baseline for PUDL

For the first pass, the target should be modest and enforceable.

Recommended baseline for most assets:

- description
- owner
- at least one dataset/source-family tag
- at least one ETL-layer tag or equivalent grouping signal

Recommended additional metadata where useful:

- raw-format tags such as `xbrl`, `dbf`, `csv`, `xlsx`, `json`
- engine/tooling tags such as `pandas`, `polars`, `duckdb`
- operational-class tags such as `high_memory`, `high_cpu`, `io_heavy`
- kinds where a stable vocabulary adds value for filtering or display

Recommended baseline for asset checks:

- description for high-value or frequently encountered checks first

Recommended runtime metadata for selected assets:

- row counts
- date coverage
- source-version or provenance details where inspection after a run is useful

### Suggested implementation shape

The most sustainable shape is not to hand-author all Dagster metadata independently.
Instead:

- keep deep table, field, and source semantics in `pudl.metadata`
- derive Dagster descriptions and some tags from those canonical metadata models where
  possible
- keep orchestration-specific metadata, such as operational class and ownership, close
  to the Dagster definitions

That split preserves one semantic source of truth while still letting the graph become
much more useful.

### Epic 4 Suggested Tasks

- Task 1: Define the metadata contract and vocabulary.
  - Deliverable: a short policy doc or inline design note that defines:
    required fields, recommended fields, tag vocabularies, ownership conventions, and
    the split between canonical `pudl.metadata` facts and Dagster-projected metadata.
- Task 2: Define a projection strategy from `pudl.metadata` into Dagster.
  - Deliverable: a clear mapping for which table/source descriptions and related fields
    should automatically populate Dagster asset descriptions or tags.
- Task 3: Apply the baseline to a pilot subset.
  - Deliverable: one or two domains with visibly improved descriptions, owners, tags,
    and check descriptions in `dg list defs --json` and the UI.
- Task 4: Introduce operational metadata for selected assets.
  - Deliverable: a small, useful taxonomy for execution profile and tooling metadata,
    applied first to assets where scheduling or contributor understanding most benefits.
- Task 5: Add automated checks for metadata completeness on changed definitions.
  - Deliverable: CI or local validation that fails when newly touched assets miss the
    minimum required metadata contract.
- Task 6: Backfill high-value check descriptions and provenance annotations.
  - Deliverable: the most important or most visible checks have clear descriptions, and
    selected assets expose better source-family or transformation provenance metadata.

### Questions Epic 4 should answer explicitly

- Which facts are canonical in `pudl.metadata`, and which belong only in Dagster?
- Which tags are intended for human discovery versus future orchestration logic?
- Should ownership be assigned by dataset family, ETL layer, or some other boundary?
- Which provenance facts are worth surfacing at asset level in the first pass?
- Which metadata fields should be required universally, and which should only apply to
  subsets of assets?

### Epic 4 Verification

- Programmatic query of `pixi run dg list defs --json` reports improved completeness for
  descriptions, owners, tags, kinds, and check descriptions.
- Pilot domains are materially easier to find and understand in the graph and CLI
  inventory.
- CI or local validation runs on pull requests touching Dagster definitions.
- The metadata contract does not require duplicating canonical table/source semantics in
  both Dagster definitions and `pudl.metadata` by hand.

## Changelog

- 2026-03-07: Initial draft created with a detailed launch-path migration plan and later structural cleanup work.
- 2026-03-08: Reordered the early work to prioritize the `dg launch` migration and deferred broader structural planning.
- 2026-03-08: Clarified that the launch-command and config/resource tracks were interdependent and softened tone for a more informal, cooperative style.
- 2026-03-20: Added a concrete remaining-task list for the launch-path migration plus follow-up cleanup items based on the post-resource-migration state of the repository.
- 2026-03-23: Updated the plan after landing Epic 1 on this branch; refreshed the baseline snapshot, removed completed launch-path work, and refocused the plan on Epic 2 through Epic 4.
- 2026-03-23: Expanded Epic 4 with background on metadata types, PUDL-specific problems the metadata should solve, and a staged plan for projecting canonical PUDL metadata into Dagster.
