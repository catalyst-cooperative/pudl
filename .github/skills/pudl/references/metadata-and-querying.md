# Metadata and Querying

## Use this when

- You need table, field, or source definitions programmatically.
- Generated docs are missing from a local checkout.
- You need machine-queryable metadata for discovery or validation.

## Preferred metadata strategy

Use these sources in order:

1. In-repo metadata APIs (`Package`, `Resource`, `Field`, `DataSource`).
2. Metadata embedded in Parquet schema.
3. Generated docs and rendered HTML for prose context.

## Programmatic metadata APIs

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

## Export Frictionless datapackage JSON

```python
from pathlib import Path
from pudl.metadata.classes import PUDL_PACKAGE

out = Path("/tmp/pudl_datapackage.json")
out.write_text(PUDL_PACKAGE.to_frictionless().to_json())
print(out)
```

## Query metadata with jq

```bash
jq '.resources | length' /tmp/pudl_datapackage.json
jq -r '.resources[] | select(.name=="out_eia923__generation") | .schema.fields[].name' /tmp/pudl_datapackage.json
jq -r '.resources[] | select(.name=="out_eia923__generation") | .schema.fields[] | select(.name=="plant_id_eia") | .description' /tmp/pudl_datapackage.json
```

## Query metadata with DuckDB

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

## Parquet-embedded metadata

```python
import pyarrow.parquet as pq
from pudl.workspace.setup import PudlPaths

table_name = "out_eia923__generation"
path = PudlPaths().parquet_path(table_name)
schema = pq.read_schema(path)

table_meta = schema.metadata
plant_field_meta = schema.field("plant_id_eia").metadata
```

## If generated docs are missing

- Metadata-driven docs are generated via `docs/conf.py` custom build operations:
  - `data_dictionary_metadata_to_rst()`
  - `data_sources_metadata_to_rst()`
  - `static_dfs_to_rst()`
- Rebuild and retain generated intermediates:

```bash
PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
```

- Retained files appear under:
  - `docs/data_sources/**/*.rst`
  - `docs/data_dictionaries/**/*.rst`
  - `docs/data_dictionaries/**/*.csv`

## Templates and schema

- Docs templates: `docs/templates/`
- Vendored datapackage schema: `.github/skills/pudl-dev/references/datapackage.schema.json`
- Upstream schema URL: `https://datapackage.org/profiles/2.0/datapackage.json`

## Canonical sources

- `src/pudl/metadata/classes.py`
- `src/pudl/metadata/resources/*.py`
- `src/pudl/metadata/fields.py`
- `src/pudl/metadata/sources.py`
- `docs/conf.py`
- `docs/templates/`
- `docs/data_dictionaries/index.rst`
- `docs/data_sources/index.rst`
- `docs/dev/build_docs.rst`
