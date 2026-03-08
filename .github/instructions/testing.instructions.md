---
description: "Use when writing or updating pytest tests, debugging test failures, or deciding which PUDL test scope to run."
name: "PUDL Testing Instructions"
applyTo: "test/**/*.py"
---

# PUDL testing instructions

- Run tests in the pixi environment.
- Prefer `pixi run pytest ...` unless already in `pixi shell`.
- For targeted local runs, disable coverage to avoid noisy warnings and failures from
  partial test selection.

## Common commands

```bash
# Run all CI-equivalent checks (slow): docs + unit + integration + coverage report
pixi run pytest-ci

# Fast local unit and doctest feedback
pixi run pytest --no-cov --doctest-modules src/pudl test/unit

# Integration tests with fast ETL settings (slow)
pixi run pytest --no-cov --etl-settings src/pudl/package_data/settings/etl_fast.yml test/integration

# Run a specific file or test
pixi run pytest --no-cov test/unit/path/to/test_file.py
pixi run pytest --no-cov test/unit/path/to/test_file.py::TestClass::test_name
```

## Test writing conventions

- Use `pytest`, not `unittest` test classes.
- Prefer `pytest-mock` over `monkeypatch` when mocking is needed.
- Prefer `pytest.mark.parametrize` over repetitive hand-written test variants.
- Keep unit tests independent of external network and large data dependencies.

## Integration test caveats

- Integration tests can trigger ETL work and may take 45+ minutes.
- If testing behavior against existing outputs that are already built, use
  `--live-dbs` where appropriate.
- If testing datastore behavior, `--tmp-data` can isolate from local cache.

See also: `docs/dev/testing.rst`
