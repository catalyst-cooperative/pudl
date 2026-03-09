# Docs Generation Traceability Matrix

## Use this when

- You need to trace a generated docs artifact back to source code.
- You are debugging why generated `.rst`/`.csv` outputs changed.
- You need to update templates or metadata classes safely.

## Build hook to output mapping

| `docs/conf.py` build hook | Primary outputs | Input metadata model(s) | Template(s) |
| --- | --- | --- | --- |
| `data_dictionary_metadata_to_rst()` | `docs/data_dictionaries/pudl_db.rst` | `pudl.metadata.classes.Package`, `RESOURCE_METADATA` | `docs/templates/package.rst.jinja`, `docs/templates/resource.rst.jinja` |
| `data_sources_metadata_to_rst()` | `docs/data_sources/<source_id>.rst` for `INCLUDED_SOURCES` | `pudl.metadata.classes.DataSource`, `PUDL_PACKAGE`, `Resource` subsets | `docs/templates/data_source_parent.rst.jinja`, `docs/templates/*_child.rst.jinja` |
| `static_dfs_to_rst()` | `docs/data_dictionaries/codes_and_labels.rst` and `docs/data_dictionaries/code_csvs/*.csv` | `pudl.metadata.classes.CodeMetadata`, `CODE_METADATA` | `docs/templates/codemetadata.rst.jinja` |

## Template mapping quick reference

- `resource.rst.jinja` -> `pudl.metadata.classes.Resource`
- `package.rst.jinja` -> `pudl.metadata.classes.Package`
- `data_source_parent.rst.jinja` -> `pudl.metadata.classes.DataSource`
- `*_child.rst.jinja` -> source-specific context keyed by source ID (`eia923`, `ferc1`, `vcerare`, etc.)
- `codemetadata.rst.jinja` -> code label CSVs rendered into `codes_and_labels.rst`

## Generated file lifecycle

- Keep generated files:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
```

- Retained generated files are written under:
  - `docs/data_sources/**/*.rst`
  - `docs/data_dictionaries/**/*.rst`
  - `docs/data_dictionaries/**/*.csv`

- Default behavior removes generated files in `build-finished` hooks in `docs/conf.py`.

## Canonical sources

- `docs/conf.py`
- `docs/templates/`
- `src/pudl/metadata/classes.py`
- `src/pudl/metadata/resources/`
- `src/pudl/metadata/sources.py`
- `src/pudl/metadata/codes.py`
