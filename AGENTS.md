# AGENTS.md

## Project overview

PUDL ingests raw public energy data (EIA, FERC, EPA, and others) and transforms it into
clean, analysis-ready tables. The pipeline is orchestrated using Dagster assets and jobs.

Raw inputs are managed through the datastore and are rooted at `$PUDL_INPUT`. Pipeline
outputs are written under `$PUDL_OUTPUT/` and include:

- Apache Parquet files (`$PUDL_OUTPUT/parquet/`) — the primary analytical outputs
- SQLite databases (`$PUDL_OUTPUT/*.sqlite`) — used for FERC raw data and some outputs
- DuckDB databases (`$PUDL_OUTPUT/*.duckdb`) — currently only for FERC XBRL data
- JSON datapackage descriptors (`$PUDL_OUTPUT/*_datapackage.json`) — frictionless
  datapackage metadata describing the schema and structure of the tabular outputs

## Repository structure

Key directories under `src/pudl/`:

- `extract/` — one module per data source; reads raw inputs via the datastore and
  produces lightly-typed DataFrames
- `transform/` — one module per data source; cleans, normalizes, and validates data
- `etl/` — Dagster asset and job definitions, organized by data source; top-level
  `defs` object and all jobs live in `pudl.etl`
- `metadata/` — table and column metadata (`classes.py`, `fields.py`, `resources.py`);
  "Resources" are tables, "Fields" are columns
- `glue/` — entity resolution tables that link IDs across data sources
- `analysis/` — higher-level analytical assets built on top of the core ETL outputs
- `helpers.py` — shared utility functions; check here before writing new helpers
- `io_managers.py` — Dagster IO managers for SQLite, Parquet, and FERC SQLite reads
- `settings.py` — Pydantic settings models for all datasets and ETL configuration
- `resources.py` — Dagster resources (`etl_settings`, `datastore`, `zenodo_dois`, etc.)
- `ferc_sqlite_provenance.py` — fingerprinting and compatibility checks for FERC SQLite
  databases across separate job runs

Other important directories:

- `dbt/` — dbt models used for data validation only (not transformation)
- `test/unit/` — fast unit tests; run these during development
- `test/integration/` — slow integration tests; do not run interactively
- `docs/` — Sphinx documentation source (reStructuredText)
- `src/pudl/package_data/settings/` — packaged Dagster run config YAML files
  (`dg_fast.yml`, `dg_full.yml`, `dg_pytest.yml`, `dg_nightly.yml`)

## Development environment

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
pixi run pre-commit run --all-files            # run all hooks on all files
pixi run pre-commit run ruff-check --all-files # lint without fixing
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
pixi run pytest-unit                                                            # all unit tests
pixi run pytest --no-cov test/unit/path/to/test_file.py                        # single file
pixi run pytest --no-cov test/unit/extract/excel_test.py::TestGenericExtractor # single class
```

### Integration tests

Integration tests live under `test/integration/` and take up to 60 minutes. They use a
`prebuilt_outputs` fixture that runs the full ETL via `dg launch` as a subprocess with
`dg_pytest.yml` as the default config. Do not run them interactively during development.

```bash
pixi run pytest-integration                                     # full integration suite
pixi run pytest-ci                                              # docs + unit + integration + dbt + coverage
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

## Metadata system

Metadata describing tables, columns, and data sources lives in `pudl.metadata`.
"Resources" are tables; "Fields" are columns. Metadata classes in
`pudl.metadata.classes` use Pydantic and mirror the frictionless datapackage standard.

## Documentation and release notes

Docs are built with Sphinx from reStructuredText source in `docs/`. Significant
user-visible or developer-visible changes must be summarized in `docs/release_notes.rst`
with references to the PR and any related issue numbers. Add release notes after
the feature on a branch is complete and prior to marking a PR as ready for review.

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
- Run pre-commit and fix all issues before committing.
- Do not add spurious or generated files to source control.
- Do not skip pre-commit hooks (`--no-verify`); fix the underlying issue instead.
- Include both the issue number and PR number in release notes entries.

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
