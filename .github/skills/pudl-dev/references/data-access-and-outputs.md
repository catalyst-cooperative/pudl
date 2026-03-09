# Data Access and Outputs

## Use this when

- Explaining how users should access PUDL data.
- Writing/debugging notebook examples against PUDL outputs.
- Choosing local vs cloud data access for development.

## Access priorities

- Prefer `out_*` tables for end-user analysis.
- Prefer local Parquet for fast iteration in development.
- Use S3 `nightly`/`stable` for reproducible cloud access.

## Common access paths

- Local: `$PUDL_OUTPUT/parquet/<table>.parquet`
- S3 nightly: `s3://pudl.catalyst.coop/nightly/<table>.parquet`
- S3 stable: `s3://pudl.catalyst.coop/stable/<table>.parquet`

## Related outputs

- Fully processed outputs: SQLite + Parquet.
- Some high-volume hourly outputs are Parquet-only.
- Raw converted FERC databases are distributed as SQLite.

## Canonical sources

- `docs/data_access.rst`
- `docs/index.rst`
- `README.rst`
- `.github/skills/pudl/SKILL.md`
