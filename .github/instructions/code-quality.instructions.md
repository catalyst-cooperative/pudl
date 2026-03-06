---
description: "Use when linting, formatting, or preparing commits with pre-commit hooks (ruff, doc8, typos, actionlint, shellcheck, etc.)."
name: "PUDL Code Quality Instructions"
applyTo: "src/**/*.py, test/**/*.py, docs/**/*.rst, .github/**/*.yml, .github/**/*.yaml"
---

# Code quality instructions

- Run all checks inside the pixi environment.
- Prefer repo-configured pre-commit hooks over ad-hoc local tool options.

## Ruff linting and formatting

```bash
# Lint + autofix using pre-commit hook configuration
pixi run pre-commit run ruff-check --all-files

# Format Python files with ruff formatter
pixi run pre-commit run ruff-format --all-files
```

## Run all pre-commit hooks

```bash
# Includes non-ruff hooks configured in .pre-commit-config.yaml
pixi run pre-commit-run
```

## Install git hooks locally

```bash
pixi run pre-commit-install
```

## Notes

- Hook configuration is canonical in `.pre-commit-config.yaml`.
- Non-ruff hooks include checks such as doc8, typos, actionlint, shellcheck,
  test naming, merge conflict markers, and YAML/TOML validity.
- Local hooks also include sorting row counts and running unit tests at commit time.

See also: `docs/dev/dev_setup.rst` (linting section)
