---
name: pudl-data-access
description: Access PUDL table data plus table/column/source metadata in Jupyter or Marimo notebooks for debugging and visualization. Use when users ask what a table contains, how to read it, or how columns are defined.
argument-hint: "[table_name] [local|s3] [data|metadata|both]"
---

# PUDL Data Access

The Public Utility Data Liberation Project (PUDL) processes many different public
data sets related to energy systems, and generates tabular outputs. These outputs are
primarily distributed as Apache Parquet files.

This skill is optimized for notebook workflows (Jupyter and Marimo) focused on:

- debugging data quality issues,
- validating transformations,
- creating exploratory plots and visualizations,
- understanding table and column meaning from metadata.

## Trigger phrases

- "show me this PUDL table in a notebook"
- "how do I read table X from Parquet"
- "load this table from local PUDL output"
- "read this PUDL table from S3"
- "what columns are in this table"
- "what does this column mean"
- "show table metadata for X"
- "which data source does this table come from"
- "find field descriptions for this table"
- "I need notebook code to explore this PUDL data"

## Preferred metadata strategy

Use these metadata sources in order:

1. In-repo metadata APIs (`Package`, `Resource`, `Field`, `DataSource`) for structured,
   programmatic metadata.
2. Metadata embedded in Parquet schemas (table + field descriptions).
3. Rendered docs for human-readable context and example snippets.

Use the rendered docs to explain context to humans, but use the metadata classes as the
canonical machine-readable source.

## Where to find PUDL data

- Local outputs: `$PUDL_OUTPUT/parquet/<table_name>.parquet`
- Nightly S3 outputs: `s3://pudl.catalyst.coop/nightly/<table_name>.parquet`
- Stable S3 outputs: `s3://pudl.catalyst.coop/stable/<table_name>.parquet`

If both are available and the user did not request cloud data specifically, prefer local
Parquet for speed and reproducibility.

For analyst-friendly tables, prefer `out_*` tables.

## Notebook quick start (data)

### Local Parquet via PUDL helper (preferred in repo)

```python
from pudl.helpers import get_parquet_table, get_parquet_table_polars

table_name = "out_eia923__generation"

# Pandas (or GeoPandas for geometry tables), schema-aware
df = get_parquet_table(table_name)

# Optional projection/filter for performance
df_small = get_parquet_table(
    table_name,
    columns=["plant_id_eia", "report_date", "net_generation_mwh"],
)

# Polars LazyFrame
lf = get_parquet_table_polars(table_name)
```

### Local Parquet via explicit path

```python
import pandas as pd
from pudl.workspace.setup import PudlPaths

table_name = "out_eia923__generation"
path = PudlPaths().parquet_path(table_name)
df = pd.read_parquet(path)
```

### Remote S3 Parquet

```python
import pandas as pd

table_name = "out_eia923__generation"
df = pd.read_parquet(f"s3://pudl.catalyst.coop/nightly/{table_name}.parquet")
```

## Notebook quick start (metadata)

### Table metadata (description, schema, keys, sources)

```python
from pudl.metadata import PUDL_PACKAGE

table_name = "out_eia923__generation"
resource = PUDL_PACKAGE.get_resource(table_name)

table_description = resource.description
primary_key = resource.schema.primary_key
field_names = [f.name for f in resource.schema.fields]
source_ids = [s.name for s in resource.sources]
```

### Column metadata (field-level)

```python
from pudl.metadata.classes import Field

field = Field.from_id("plant_id_eia")
field_info = field.model_dump()

# Commonly useful fields:
field_name = field.name
field_type = field.type
field_description = field.description
field_constraints = field.constraints.model_dump()
```

### Data-source metadata (e.g. EIA-923, FERC Form 1)

```python
from pudl.metadata.classes import DataSource

source = DataSource.from_id("eia923")

source_title = source.title
source_description = source.description
source_partitions = source.working_partitions
related_tables = source.get_resource_ids()
```

### Frictionless datapackage JSON metadata

```python
from pudl.metadata.classes import Package

table_name = "out_eia923__generation"
pkg = Package.from_resource_ids(resource_ids=(table_name,))
datapackage_json = pkg.to_frictionless().to_json()
```

## Metadata embedded in Parquet

PUDL writes Parquet with metadata from `Resource.to_pyarrow()`, including:

- table-level `description` (and often `primary_key`),
- field-level `description` metadata.

```python
import pyarrow.parquet as pq
from pudl.workspace.setup import PudlPaths

table_name = "out_eia923__generation"
path = PudlPaths().parquet_path(table_name)
schema = pq.read_schema(path)

table_meta = schema.metadata  # bytes->bytes mapping
plant_field_meta = schema.field("plant_id_eia").metadata
```

## Human-readable docs and snippet sources

Use these when you need prose context or ready-made table examples:

- `docs/data_access.rst`
- `docs/_build/html/data_dictionaries/pudl_db.html`
- `docs/_build/html/data_sources/*.html`

The data dictionary page includes per-table snippet tabs (pandas, polars, DuckDB, R)
and table descriptions.

## Source-of-truth metadata files in the repo

- Table metadata: `src/pudl/metadata/resources/*.py`
- Field metadata: `src/pudl/metadata/fields.py`
- Data source metadata: `src/pudl/metadata/sources.py`
- Metadata models and conversion methods: `src/pudl/metadata/classes.py`

## Rendering metadata to RST (advanced)

When you need generated documentation fragments directly from metadata classes:

```python
from pathlib import Path
from pudl.metadata.classes import Resource, Package

Resource.from_id("out_eia923__generation").to_rst(
    docs_dir=Path("docs"),
    path="/tmp/out_eia923__generation.rst",
)

Package.from_resource_ids(resource_ids=("out_eia923__generation",)).to_rst(
    docs_dir=Path("docs"),
    path="/tmp/package_subset.rst",
)
```

`DataSource.to_rst()` is available too, but requires source/extra resource lists and is
primarily intended for full docs generation.

## Practical debugging workflow

1. Load a narrow slice of data first (selected columns and optional filters).
2. Pull table metadata from `PUDL_PACKAGE.get_resource(table_name)`.
3. Pull relevant field metadata via `Field.from_id(...)` for suspicious columns.
4. Check Parquet schema metadata to verify what was written with the artifact.
5. Cross-check table prose and canned snippets in `docs/_build/html/data_dictionaries/pudl_db.html`.
6. If needed, inspect source metadata files under `src/pudl/metadata/`.
