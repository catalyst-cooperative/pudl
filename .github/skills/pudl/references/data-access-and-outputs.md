# Data Access and Outputs

## Use this when

- Reading PUDL tables in notebooks (Jupyter or Marimo).
- Choosing between local outputs and cloud outputs.
- Writing reproducible data exploration snippets for users.

## Access priorities

- Prefer `out_*` tables for analyst-facing workflows.
- Prefer local Parquet in active development for speed and reproducibility.
- Use S3 `nightly`/`stable` for shareable cloud examples.

## Canonical locations

- Local Parquet: `$PUDL_OUTPUT/parquet/<table_name>.parquet`
- S3 nightly: `s3://pudl.catalyst.coop/nightly/<table_name>.parquet`
- S3 stable: `s3://pudl.catalyst.coop/stable/<table_name>.parquet`

## Notebook quick start

### Local Parquet via helper API (preferred in-repo)

```python
from pudl.helpers import get_parquet_table, get_parquet_table_polars

table_name = "out_eia923__generation"

# pandas (or geopandas for geometry tables)
df = get_parquet_table(table_name)

# Optional projection for speed
df_small = get_parquet_table(
    table_name,
    columns=["plant_id_eia", "report_date", "net_generation_mwh"],
)

# polars lazyframe
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

## Related outputs

- Fully processed outputs are distributed as SQLite and Parquet.
- Some high-volume hourly outputs are Parquet-only.
- Raw converted FERC databases are distributed as SQLite.

## Canonical sources

- `docs/data_access.rst`
- `docs/index.rst`
- `README.rst`
