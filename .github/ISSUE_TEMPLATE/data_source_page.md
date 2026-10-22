---
name: New Data Source Page
about: Check-list for adding a new Data Source page to the PUDL data documentation
title: Add Data Source documentation for [source]
labels: documentation
assignees: ""
---

## Data Source Page Checklist

These are the necessary steps to adding a new Data Source page:

- [ ] Make a copy of `docs/templates/generic_child_template.rst.jinja` & complete sections as desired
- [ ] Make a new directory `docs/data_sources/[source]` and add any downloads (reporting instructions, etc)
- [ ] Add to `docs/conf.py:INCLUDED_SOURCES`
- [ ] Add to `pyproject.toml:[tool.pixi.tasks.docs-clean]`
- [ ] Add to `docs/index.rst`
- [ ] Add to `docs/data_sources/index.rst`
- [ ] Review related metadata in `src/pudl/metadata/sources.py`
- [ ] Build the docs using `pixi run docs-build`
