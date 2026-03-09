---
name: pytest
description: Run pytest unit and integration tests for PUDL. Use for test selection, debugging failures, and choosing fast vs CI-equivalent test runs.
argument-hint: "[unit|integration|ci|targeted] [path_or_nodeid]"
---

# Pytest Workflow

Use this skill when you need to run or reason about software tests in PUDL.

## Trigger phrases

- "run pytest"
- "run unit tests"
- "run integration tests"
- "run CI-equivalent tests"
- "run this specific test"
- "debug this failing pytest"
- "rerun test with --live-dbs"
- "use fast ETL settings for tests"
- "what test command should I run"

## Prerequisites

- Run commands in the pixi environment (`pixi run ...`) unless already in
	`pixi shell`.
- Integration tests can be slow and may trigger ETL work.

## Common commands

```bash
# CI-equivalent workflow (docs + unit + integration + coverage report)
pixi run pytest-ci

# Unit tests + doctests (fast)
pixi run pytest --no-cov --doctest-modules src/pudl test/unit

# Integration tests with fast ETL settings
pixi run pytest --no-cov --etl-settings src/pudl/package_data/settings/etl_fast.yml test/integration

# Integration tests against existing local DBs
pixi run pytest --live-dbs --no-cov test/integration

# Single test file or nodeid
pixi run pytest --no-cov test/unit/path/to/test_file.py
pixi run pytest --no-cov test/unit/path/to/test_file.py::TestClass::test_case
```

## PUDL-specific pytest flags

- `--live-dbs`: reuse existing local PUDL/FERC DBs.
- `--tmp-data`: download fresh input data for only this run.
- `--etl-settings <path>`: choose a non-default ETL settings file.

## Test authoring conventions

- Use `pytest` style tests, not `unittest` style classes unless required.
- Prefer `pytest-mock` over `monkeypatch`.
- Use `pytest.mark.parametrize` for repeated scenarios.

## References

- `docs/dev/testing.rst`
- [Testing Instructions](../../instructions/testing.instructions.md)
