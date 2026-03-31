# AGENTS.md

## Project overview

PUDL ingests raw public energy data (EIA, FERC, EPA, and others) and transforms it into
clean, analysis-ready tables. The pipeline is orchestrated using Dagster assets and jobs.

## About this file

`AGENTS.md` is the canonical instruction file for this repository. `CLAUDE.md` in
the same directory is a symlink that points to `AGENTS.md` — they are always
identical, not independent files to be kept in sync.

### Working across multiple worktrees

When working in multiple git worktrees simultaneously, any `AGENTS.md` injected
into your context at session start reflects the **primary working directory** only.
If you encounter an `AGENTS.md` at a different path during the same session, do not
assume it is the same file — it may be a different version of this document, or
belong to an entirely different repository.

To avoid this confusion:

- Always use full absolute paths when referencing or comparing `AGENTS.md` files
  across worktrees — this makes the distinction visible immediately.
- When you need to know which `AGENTS.md` governs a particular worktree, read it
  directly from that worktree's directory rather than relying on the
  context-loaded version.

## Repository structure

Key directories under `src/pudl/`:

- `extract/` — one module per data source; reads raw inputs via the datastore and
  produces lightly-typed DataFrames
- `transform/` — one module per data source; cleans, normalizes, and validates data
- `dagster/` — all Dagster orchestration code; the canonical home for everything
  Dagster-specific. Sub-structure:
  - `dagster/asset_checks.py` — asset-check definitions, factories, and helpers
  - `dagster/assets/` — oddball asset definitions that don't fit the per-source layout;
    `assets/core/` for processed assets, `assets/raw/` for raw-extraction assets
  - `dagster/build.py` — assembles the `dagster.Definitions` object
  - `dagster/config.py` — reusable Dagster run-config fragments and helpers
  - `dagster/io_managers.py` — IO managers for SQLite, Parquet, and FERC SQLite reads
  - `dagster/jobs.py` — named Dagster jobs (`pudl`, `ferc_to_sqlite`, `ferceqr`)
  - `dagster/partitions.py` — shared partition definitions
  - `dagster/provenance.py` — FERC SQLite fingerprinting and compatibility checks
  - `dagster/resources.py` — `ConfigurableResource` definitions and default resource map
  - `dagster/sensors.py` — sensor-based automation (e.g. the FERC EQR sensor)
- `deploy/` — post-ETL deployment logic: publishing outputs to GCS/S3, updating
  `nightly`/`stable` git branches, triggering Zenodo releases, applying GCS holds,
  and redeploying the PUDL Viewer Cloud Run service. `ferceqr.py` contains Dagster
  assets specific to the FERC EQR batch pipeline; `pudl.py` covers full PUDL builds.
- `scripts/` — all CLI entry points as thin wrappers; one module per script, each
  exposing a `main` Click command. Registered in `[project.scripts]` in `pyproject.toml`
- `metadata/` — table and column metadata (`classes.py`, `fields.py`, `resources.py`);
  "Resources" are tables, "Fields" are columns
- `glue/` — entity resolution tables that link IDs across data sources
- `analysis/` — higher-level analytical assets built on top of the core ETL outputs
- `helpers.py` — shared utility functions; check here before writing new helpers
- `settings.py` — Pydantic settings models for all datasets and ETL configuration
- `validate.py` — data validation helpers that need to be accessible outside Dagster
  (foreign key checks, continuity checks)
- `definitions.py` — stable `dg`-compatible entry point; re-exports `defs` from
  `pudl.dagster`

Other important directories:

- `dbt/` — dbt models used for data validation only (not transformation)
- `test/unit/` — fast unit tests; run these during development
- `test/integration/` — slow integration tests; do not run interactively
- `docs/` — Sphinx documentation source (reStructuredText)
- `src/pudl/package_data/settings/` — packaged Dagster run config YAML files
  (`dg_fast.yml`, `dg_full.yml`, `dg_pytest.yml`, `dg_nightly.yml`)

## Development environment

### Inputs and outputs

Raw inputs and pipeline outputs live **outside the repository**, in directories with
sufficient disk space. Their locations are set by two environment variables:

- `$PUDL_INPUT` — root of the raw input datastore. Raw data files are downloaded here
  by the `pudl_datastore` CLI and read by the pipeline at runtime. Do not write to
  this directory manually.
- `$PUDL_OUTPUT` — root of all pipeline outputs. Contents include:
  - Apache Parquet files (`$PUDL_OUTPUT/parquet/`) — the primary analytical outputs
  - SQLite databases (`$PUDL_OUTPUT/*.sqlite`) — used for FERC raw data and some outputs
  - DuckDB databases (`$PUDL_OUTPUT/*.duckdb`) — currently only for FERC XBRL data
  - JSON datapackage descriptors (`$PUDL_OUTPUT/*_datapackage.json`) — frictionless
    datapackage metadata describing the schema and structure of the tabular outputs

Never assume these directories are inside the repository. Never hardcode paths to them.

### Python environment

PUDL uses `pixi` for dependency and task management. Always use `pixi run <command>` to
ensure commands run in the correct environment.

**Never** use `uv`, `pip`, `conda`, `venv`, or any other tool to install packages or
create Python environments. Do not run `uv run`, `uv pip install`, `python -m venv`, or
anything similar. Pixi is the only permitted environment manager.

Project tasks and environments are defined in `pyproject.toml` under `[tool.pixi]`.
Git pre-commit hooks are defined in `.pre-commit-config.yaml`.

### Adding dependencies

Adding a **runtime dependency** requires updating `pyproject.toml` in two places:

1. `[tool.pixi.dependencies]` — the full conda-forge listing (dev + test + runtime).
2. `[tool.pixi.package.run-dependencies]` — the minimal runtime set used when PUDL is
   installed as a conda package (e.g. by `pudl-archiver`). Keep this in sync with the
   expansive list above.

Dev-only or test-only dependencies belong only in `[tool.pixi.dependencies]` (or
`[tool.pixi.feature.dev.dependencies]`). After editing, run `pixi install` to update
the lockfile.

If a new pre-commit hook is needed, add it to `.pre-commit-config.yaml` and run
`pixi run pre-commit-install` to update the git hooks.

## Common commands

```bash
# New worktree initialization
pixi install
pixi run pre-commit-install

# Linting and formatting
pixi run pre-commit run --all-files             # run all hooks on all files
pixi run pre-commit run ruff-check --all-files  # lint without fixing
pixi run pre-commit run ruff-format --all-files # fix formatting

# Type checking (faster than mypy)
pixi run ty check src/pudl/path/to/file.py

# Unit tests (fast; run before every commit)
pixi run pytest-unit

# dbt data validation tests
pixi run dbt_helper validate                                                  # all dbt tests
pixi run dbt_helper validate --asset-select "key:out_eia__yearly_generators"  # one asset

# Running the ETL
pixi run ferc-to-sqlite               # FERC SQLite only
pixi run pudl-with-ferc-to-sqlite     # full ETL (local dev)
pixi run dg launch --job pudl_with_ferc_to_sqlite --config src/pudl/package_data/settings/dg_fast.yml

# Dagster UI and CLI
pixi run dg dev                   # start webserver and daemons
pixi run dg check defs --verbose  # sanity-check that all defs load

# Documentation build
pixi run docs-check  # faster, disables intersphinx, no rendered output
pixi run docs-build  # slower, requires network, produces HTML in docs/_build/html
```

## Preferred CLI tools

### rg (ripgrep)

Use instead of `grep` for all codebase searches. Faster, respects `.gitignore`, and
produces cleaner output.

```bash
rg "class FercDbfExtractor" src/          # basic search
rg -t py "FercSQLiteProvenance" src/      # restrict to Python files
rg -C 3 "assert_ferc_sqlite_compatible" src/  # show 3 lines of context
rg '"plant_id_eia"' src/pudl/metadata/    # find a field/column definition in metadata
```

### ty

Use to find Python type errors before committing. `ty` uses its own error codes
(e.g. `missing-argument`, `unresolved-attribute`) which differ from mypy's
(e.g. `call-arg`, `attr-defined`).

For suppression syntax, see **Type error suppression** in the Code Style section.

### jq

Parses JSON files. Takes the filename as a positional argument.

```bash
jq '.nodes | keys' dbt/target/manifest.json                                           # all dbt node names
jq '[.nodes[] | select(.resource_type == "model") | .name]' dbt/target/manifest.json  # model names only
jq '.nodes["model.pudl.fuel_ferc1"]' dbt/target/manifest.json                         # inspect a specific dbt node
jq '[.resources[].name]' "$PUDL_OUTPUT/ferc1_xbrl_datapackage.json"                   # table names in a datapackage
jq '.resources[] | select(.name == "identification_001_duration") | .schema.fields[].name' "$PUDL_OUTPUT/ferc1_xbrl_datapackage.json"  # column names for a table
```

### dbt_helper

PUDL's wrapper around `dbt build` that annotates test failures with the actual query
results, making failures much easier to diagnose. Use instead of raw `dbt build`.
Accepts Dagster asset selection syntax via `--asset-select`.

```bash
pixi run dbt_helper validate                                              # run all dbt tests
pixi run dbt_helper validate --asset-select "key:out_eia__yearly_generators"   # one asset
pixi run dbt_helper validate --asset-select "+key:out_eia__yearly_generators"  # asset + upstream
pixi run dbt_helper validate --asset-select "key:out_eia__yearly_generators" --exclude "*check_row_counts*"
```

**`dbt_helper validate` runs against the Parquet files in `$PUDL_OUTPUT`.** If those
files were not produced by materializing the corresponding asset using the full ETL
settings on the current branch, the validation results will not be reliable. Only
validate tables you are actively working on, and always materialize them first on the
current branch:

```bash
pixi run dg launch --assets "out_eia__yearly_generators" --config src/pudl/package_data/settings/dg_full.yml
pixi run dbt_helper validate --asset-select "key:out_eia__yearly_generators"
```

### dg

Use `pixi run dg` to ensure `dg` is always run in the correct environment.

### ruff

Can be run directly on specific files or via pre-commit on all files.

```bash
pixi run ruff check src/pudl/path/to/file.py    # check a specific file
pixi run ruff format src/pudl/path/to/file.py   # format a specific file
pixi run pre-commit run ruff-check --all-files  # check everything before committing
pixi run pre-commit run ruff-format --all-files # format everything before committing
```

## Available skills

Skills are defined in `skills-lock.json`. If not already installed, run
`pixi run install-skills`.

- **`dagster-expert`** — Dagster and `dg` CLI reference. Use when adding or modifying
  assets, resources, IO managers, jobs, sensors, or any other Dagster construct.
- **`dignified-python`** — production Python coding standards (3.10-3.13). Use when
  writing, reviewing, or refactoring Python code.

## Dagster architecture

**Settings flow**: Always pass config via `dg launch --config dg_xxx.yml`. Never
hand-assemble `run_config` dicts. The YAML path is read by `PudlEtlSettingsResource`,
which loads `EtlSettings` and injects it into all assets and IO managers.

**FERC SQLite provenance**: each FERC SQLite materialization records a fingerprint
(Zenodo DOI, years, ETL settings hash) in Dagster asset metadata. Downstream IO managers
call `assert_ferc_sqlite_compatible()` before reading and raise a descriptive
`RuntimeError` if the stored fingerprint does not match the current run.

## Testing

Always include `--no-cov` when running pytest directly (not via a pixi task) to skip
coverage collection and avoid spurious failures.

### Unit tests

Unit tests live under `test/unit/`, take up to 2 minutes, and run automatically via the
pre-commit hook on every commit.

```bash
pixi run pytest-unit                                                           # all unit tests
pixi run pytest --no-cov test/unit/path/to/test_file.py                        # single file
pixi run pytest --no-cov test/unit/extract/excel_test.py::TestGenericExtractor # single class
```

### Integration tests

Integration tests live under `test/integration/` and take up to 60 minutes. They use a
`prebuilt_outputs` fixture that runs the full ETL via `dg launch` as a subprocess with
`dg_pytest.yml` as the default config. Do not run them interactively during development.

```bash
pixi run pytest-integration                                    # full integration suite
pixi run pytest-ci                                             # docs + unit + integration + dbt + coverage
pixi run pytest --no-cov --live-pudl-output test/integration   # using existing local outputs
```

### Custom pytest flags

- `--live-pudl-output` — skip the prebuild and use existing local PUDL outputs. Useful
  when your change doesn't affect the ETL itself (e.g. analysis code or data validation).
  **Cannot be combined with unit tests** in the same session; the two suites require
  incompatible `$PUDL_OUTPUT` environment variable handling.
- `--temp-pudl-input` — download a fresh copy of raw inputs for this run only, instead
  of reusing the local datastore cache. Use when testing datastore functionality.
- `--dg-config PATH` — override the default `dg_pytest.yml` with a custom Dagster config.

### Fixture constraints

Do not run tests that depend on `prebuilt_outputs`, `pudl_engine`, `ferc1_xbrl_engine`,
or `ferc1_dbf_engine` fixtures during development — these require a full integration ETL
build. Exception: with `--live-pudl-output`, these fixtures use existing outputs and run
quickly.

### Test style

Use pytest-mock (`mocker`). Avoid `unittest` and `monkeypatch`.

## Code style

**Acronyms in compound class names**: In compound class names that contain multiple
acronyms, capitalize acronyms as words: e.g. `FercDbf`, `FercXbrl`, `SQLite` (SQLite
is a special case — all SQL letters are capitalized because the L participates in both
the acronym and the word "Lite").

**Line length**: limit lines to 88 characters. Do not artificially restrict to 80.

**Type annotations**: add annotations wherever they aid readability or IDE
inference — function signatures are the highest-value target. Annotations on
internal variables are optional; add them only when the type is non-obvious.
Enforcement is currently light and experimental, so err on the side of annotating
rather than skipping.

**Type error suppression**: prefer `# type: ignore[specific-code]` when a single
code suppresses the warning across all type checkers being used. When `ty` and mypy
use different codes for the same error and there is no shared code, fall back to a
bare `# type: ignore` — but you must also add `# noqa: PGH003` to silence ruff's
rule that forbids bare ignores:

```python
result = some_dynamic_call()  # type: ignore  # noqa: PGH003
```

The `# noqa: PGH003` is not optional: without it, ruff will reject the bare ignore.
Reserve this pattern for cases where no single error code works across checkers.
If the suppression is non-obvious, add a prose comment explaining why:

```python
# EIA raw data uses mixed string/int years; we coerce downstream.
year = row["year"]  # type: ignore  # noqa: PGH003
```

**Logging**: never use `print()` outside of CLI interfaces. Use Python's `logging`
module via the `pudl.logging_helpers` module. Obtain a logger at module level with
`logger = pudl.logging_helpers.get_logger(__name__)`. Never use old-style `%`
formatting or `str.format()` for log messages that include variable values. Instead
always use f-strings for log messages that need to include variable values:

```python
from pudl import logging_helpers
logger = logging_helpers.get_logger(__name__)

logger.info(f"Processing {len(df)} rows for year {year}.")
logger.warning(f"Skipping {plant_id=} — missing required field.")
```

## Architecture and key patterns

- Data validation belongs in dbt, not in Dagster asset checks.
- Sanity checks on data assumptions embedded within the `pudl.transform` modules
  should raise `AssertionError` loudly when assumptions are violated, rather than
  silently passing and causing downstream issues.
- Use existing utility functions in `pudl.helpers` before writing new ones.
- Raw data access must use the datastore pattern, not direct file I/O.
- Use nullable pandas dtypes (`pd.Int64Dtype()`, `pd.StringDtype()`) to avoid
  generic `object` dtypes and mixed NULL values.
- Parquet outputs use snappy compression and pyarrow dtypes.
- For large datasets (>1GB), use polars or DuckDB to read data instead of pandas.

### API compatibility and refactoring scope

PUDL is an **application**, not a library. No external consumers depend on its internal
APIs. You do not need to maintain backwards-compatibility shims, deprecation warnings, or
gradual cut-overs when refactoring. A change is complete when every call-site and
reference inside the repository has been updated and the tests pass.

**Excluded from scope**: the `devtools/` and `notebooks/` directories are informal,
untested, and already broken. Do not spend time updating them, and do not let broken
references there block a refactor.

## Metadata system

Metadata describing tables, columns, and data sources lives in `pudl.metadata`.
"Resources" are tables; "Fields" are columns. Metadata classes in
`pudl.metadata.classes` use Pydantic and mirror the frictionless datapackage standard.

## PUDL developer reference docs

The following files under `docs/dev/` and `docs/methodology/` cover PUDL-specific
concepts that are not in the dagster-expert or dignified-python skills. **Read the
relevant file before working in that area** rather than guessing at conventions.

| File | When to read it |
| ---- | --------------- |
| `docs/dev/naming_conventions.rst` | Before naming any asset, table, column, variable, or code identifier — covers layer prefixes (`raw_`/`core_`/`out_`), table-type suffixes (`_assn`, `_ent`, `_scd`, etc.), and column-name patterns for IDs, codes, units, and flags |
| `docs/dev/metadata.rst` | Before adding or modifying a table, column, or data source — explains how Resources (tables) and Fields (columns) are defined, validated with Pandera, and wired into the frictionless datapackage |
| `docs/dev/data_guidelines.rst` | Before designing a new transformation — establishes what changes to raw data are acceptable, tidy-data requirements, unit conventions, and time-series completeness expectations |
| `docs/dev/existing_data_updates.rst` | When integrating a new year or version of an existing data source — step-by-step workflow covering file maps, extraction, transformation, schema updates, ID mapping, and validation |
| `docs/dev/datastore.rst` | When working with raw input data, the `pudl_datastore` CLI, or Zenodo DOI references in `zenodo_dois.yml` |
| `docs/dev/clone_ferc1.rst` | Before touching FERC extraction or the `ferc_to_sqlite` job — explains the DBF→SQLite and XBRL→SQLite conversion pipeline and the raw FERC asset group |
| `docs/dev/pudl_id_mapping.rst` | When working with cross-dataset entity resolution (`plant_id_pudl`, `utility_id_pudl`) or the manual ID mapping spreadsheet |
| `docs/methodology/entity_resolution.rst` | When working with `pudl.glue` or entity/SCD tables — explains how PUDL reconciles inconsistent plant and utility identities across EIA and FERC reporting |

## Documentation and release notes

Docs are built with Sphinx from reStructuredText source in `docs/`. Significant
user-visible or developer-visible changes must be summarized in `docs/release_notes.rst`
with references to the PR and any related issue numbers. Add release notes after
the feature on a branch is complete and prior to marking a PR as ready for review.

**When writing release notes, commit messages, or any other summary of changes, always
compare the current branch against the branch it will merge into** (typically `main`,
but check the PR base branch) using `git diff <upstream>...HEAD` or
`git log <upstream>..HEAD`. Do not rely on memory or recent file reads alone — the diff
is the authoritative record of what actually changed.

### Generated documentation files

Several pages are assembled at build time from Jinja templates rather than edited
directly:

- Data source pages are generated from metadata in `src/pudl/metadata/`.
- The PUDL data dictionary is generated from the full `PUDL_PACKAGE` metadata object.
- The Jinja templates for both live under `docs/templates/`.

These generated files appear under `docs/` during the build and are cleaned up
automatically when it completes. Do not edit them — edit the templates or the metadata
instead. When searching for documentation source files, ignore `docs/_build/` entirely.

If `pixi run docs-build` or `pixi run docs-check` fails with an error in a generated
file (e.g. an RST formatting error with a line number), re-run with
`PUDL_DOCS_KEEP_GENERATED_FILES=1` to prevent automatic cleanup and inspect the file:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-check
```

After debugging, remove the generated files with `pixi run docs-clean`.

## Contribution workflow

- Create a new branch and a matching worktree with the same name for each feature.
- Initialize the new worktree with `pixi install` and `pixi run pre-commit-install`.
- Run `pixi run pre-commit-run` and fix all issues before committing.
- Do not add spurious or generated files to source control.
- Newly added functionality should include unit tests.
- Write informative commit messages that summarize the changes and their motivation.
  Use plaintext no more than 80 characters wide, with a short summary line (max 50
  chars). For significant changes, follow the first summary with a blank line and
  a more detailed description.
- Do not skip pre-commit hooks (`--no-verify`); fix the underlying issue instead.
- Include both the issue number and PR number in release notes entries.

**When relocating code, documentation, or any other text between modules, move it
verbatim — never rewrite or reinterpret it in the same step.** If the moved content also
needs edits, make those as a separate step with explicit user approval. When moving
entire files, always use `git mv` to preserve history. When moving blocks of code
between files or within the same file, first commit the verbatim move without changes,
then make any necessary edits in a subsequent commit.

### PR checklist

Before marking a PR as ready for review:

- [ ] All unit tests pass: `pixi run pytest-unit`
- [ ] Pre-commit hooks pass on all files: `pixi run pre-commit run --all-files`
- [ ] If public behavior changed: release notes entry added to `docs/release_notes.rst`
      with the issue number and PR number
- [ ] The PR description includes a summary of the change, the motivation, and any
      relevant context
- [ ] No generated files, credentials, or unrelated changes included in the diff

## Sandbox safe execution

**zsh autocorrect**: the `.pixi/` directory in the project root causes zsh to suggest
correcting `pixi` → `.pixi`. This is always wrong. Disable autocorrect before running
any pixi commands:

```bash
unsetopt CORRECT CORRECT_ALL
pixi run ...
```

**Frozen execution**: use `pixi run --frozen` to prevent pixi from updating the
environment when running commands, avoiding unexpected dependency resolution:

```bash
pixi run --frozen pytest --no-cov test/unit
pixi run --frozen dg check defs --verbose
```

Prefer already-installed binaries (`dg`, `rg`, `ruff`, `ty`, `jq`) before invoking
commands that may trigger package resolution or updates.

When running `pixi run docs-build` in a sandbox environment, set
`PUDL_DOCS_DISABLE_INTERSPHINX=1` to avoid the need for external network connectivity.
