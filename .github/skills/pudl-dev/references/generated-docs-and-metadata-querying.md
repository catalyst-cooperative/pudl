# Generated Docs and Metadata Querying

## Use this when

- Rich generated documentation pages are unavailable in the repo checkout.
- You need data dictionary or data-source details without rendered HTML.
- You want machine-queryable metadata for table/field discovery.

## Strategy

Prefer querying canonical metadata directly from Python and Frictionless JSON, then
fall back to generated docs artifacts when available.

## Sphinx custom build entrypoints

Custom metadata-driven doc generation happens in `docs/conf.py` under
`-- Custom build operations --`.

- `data_dictionary_metadata_to_rst()`
- `data_sources_metadata_to_rst()`
- `static_dfs_to_rst()`

These functions are connected in `setup(app)` using `builder-inited` hooks.

## Export full metadata as Frictionless JSON

```python
from pudl.metadata.classes import PUDL_PACKAGE

print(PUDL_PACKAGE.to_frictionless().to_json())
```

For practical querying, write to a file:

```python
from pathlib import Path
from pudl.metadata.classes import PUDL_PACKAGE

out = Path("/tmp/pudl_datapackage.json")
out.write_text(PUDL_PACKAGE.to_frictionless().to_json())
print(out)
```

## Query with jq

```bash
# Number of resources (tables)
jq '.resources | length' /tmp/pudl_datapackage.json

# First resource name
jq -r '.resources[0].name' /tmp/pudl_datapackage.json

# Field names for one resource
jq -r '.resources[] | select(.name=="out_eia923__generation") | .schema.fields[].name' /tmp/pudl_datapackage.json

# Field description for one column in one table
jq -r '.resources[] | select(.name=="out_eia923__generation") | .schema.fields[] | select(.name=="plant_id_eia") | .description' /tmp/pudl_datapackage.json
```

## Query with DuckDB

```python
import duckdb

con = duckdb.connect()
q = """
SELECT json_extract_string(r.value, '$.name') AS resource_name
FROM read_json('/tmp/pudl_datapackage.json') t,
     json_each(t.resources) r
ORDER BY resource_name
LIMIT 10
"""
print(con.execute(q).fetchall())
```

## Programmatic metadata APIs (no generated docs required)

```python
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import DataSource, Field

resource = PUDL_PACKAGE.get_resource("out_eia923__generation")
print(resource.description)
print([f.name for f in resource.schema.fields][:10])

field = Field.from_id("plant_id_eia")
print(field.description)

source = DataSource.from_id("eia923")
print(source.title)
print(source.description)
```

## Generated-doc templates and build artifacts

- Jinja templates for data-source and data-dictionary generation: `docs/templates/`
- To rebuild docs and retain generated intermediates:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
```

- Retained generated files appear under:
  - `docs/data_sources/**/*.rst`
  - `docs/data_dictionaries/**/*.rst`
  - `docs/data_dictionaries/**/*.csv`

- If available locally, inspect built pages:
  - `docs/_build/html/data_dictionaries/pudl_db.html`
  - `docs/_build/html/data_sources/*.html`

## Template mappings

- `resource.rst.jinja` -> `pudl.metadata.classes.Resource`
- `package.rst.jinja` -> `pudl.metadata.classes.Package`
- `data_source_parent.rst.jinja` -> `pudl.metadata.classes.DataSource`
- `ID_child.rst.jinja` -> source-specific docs context (e.g. `eia923`, `ferc1`, `vcerare`)
- `codemetadata.rst.jinja` -> generated CSVs in
  `docs/data_dictionaries/code_csvs/*.csv`, rendered into
  `docs/data_dictionaries/codes_and_labels.rst`

## Frictionless schema reference

- Vendored offline schema for sandboxed/no-network use:
  - `.github/skills/pudl-dev/references/datapackage.schema.json`
- Upstream canonical schema URL:
  - `https://datapackage.org/profiles/2.0/datapackage.json`

Optional local schema validation snippet:

```python
import json
from pathlib import Path

import jsonschema
from pudl.metadata.classes import PUDL_PACKAGE

schema = json.loads(
    Path('.github/skills/pudl-dev/references/datapackage.schema.json').read_text()
)
datapackage = json.loads(PUDL_PACKAGE.to_frictionless().to_json())
jsonschema.validate(instance=datapackage, schema=schema)
print('datapackage validates against vendored schema')
```

## Canonical sources

- `src/pudl/metadata/classes.py`
- `src/pudl/metadata/resources/*.py`
- `src/pudl/metadata/fields.py`
- `src/pudl/metadata/sources.py`
- `docs/conf.py`
- `docs/templates/`
- `.github/skills/pudl-dev/references/datapackage.schema.json`
- `.github/skills/pudl/SKILL.md`
- `docs/data_dictionaries/index.rst`
- `docs/data_sources/index.rst`
- `docs/dev/build_docs.rst`
