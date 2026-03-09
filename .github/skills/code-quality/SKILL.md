---
name: code-quality
description: Run PUDL linting and formatting workflows with ruff and pre-commit hooks, including docs and config validation hooks.
argument-hint: "[ruff|pre-commit|all] [path]"
---

# Code Quality Workflow

Use this skill for formatting, linting, and pre-commit quality checks.

## Trigger phrases

- "run ruff"
- "lint this file"
- "format this Python code"
- "run pre-commit"
- "fix pre-commit failures"
- "run all code quality checks"
- "check docs lint"
- "run typos and shellcheck"
- "prepare this branch for commit"

## Ruff workflow

```bash
# Lint and autofix with repository hook settings
pixi run pre-commit run ruff-check --all-files

# Format Python with ruff formatter
pixi run pre-commit run ruff-format --all-files
```

## Full pre-commit workflow

```bash
# Run all pre-commit hooks configured in .pre-commit-config.yaml
pixi run pre-commit-run

# One-time setup in a new clone
pixi run pre-commit-install
```

## What this includes

- Ruff linting and formatting
- YAML/TOML and merge-conflict checks
- docs linting (`doc8`)
- typo checks (`typos`)
- shell and workflow linting (`shellcheck`, `actionlint`)
- local project hooks (e.g. row-count sorting, notebook output clearing, unit-tests)

## Suggested order before commit

1. `pixi run pre-commit run ruff-check --all-files`
2. `pixi run pre-commit run ruff-format --all-files`
3. `pixi run pre-commit-run`

## References

- `.pre-commit-config.yaml`
- `docs/dev/dev_setup.rst`
- [Code Quality Instructions](../../instructions/code-quality.instructions.md)
