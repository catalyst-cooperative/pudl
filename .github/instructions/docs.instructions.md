---
description: "Use when editing PUDL documentation in docs/ or README.rst, including release notes and developer documentation pages."
name: "PUDL Documentation Instructions"
applyTo: "docs/**/*.rst, README.rst"
---

# Documentation instructions

- Documentation is built with Sphinx from reStructuredText sources.
- Keep docs changes consistent with actual commands and current project tooling.
- Prefer linking to canonical docs pages instead of duplicating long procedural text.

## Build docs locally

```bash
pixi run docs-build
```

## Programmatically generated docs

- Some docs pages are generated from code or metadata (e.g. `docs/dev/metadata.rst`).
- When editing these, update the source code or metadata and regenerate the docs.
- The PUDL Data Dictionary is generated from `src/pudl/metadata/resources/**/*.py` and
  the field-level descriptions.
- Documentation Jinja templates are under `docs/templates` and are used to construct
  the documentation pages under `docs/data_sources` as well as the PUDL data dictionary.
- If a warning or error is generated when building the HTML docs from the generated
  RST source files, you can force the generated files to be retained for inspection by
  setting keep_generated_files to True in `docs/conf.py`.

## Release notes

- Significant user-visible or developer-visible changes should be noted in
  `docs/release_notes.rst`.

## Developer docs hotspots

When behavior changes in these areas, update docs under `docs/dev/`:

- testing workflows
- dbt data validation workflows
- Dagster development workflows
- metadata editing process
