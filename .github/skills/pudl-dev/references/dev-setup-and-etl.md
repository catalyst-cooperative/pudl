# Dev Setup and ETL

## Use this when

- Setting up a local environment.
- Running ETL assets or rebuilding outputs.
- Debugging data availability or datastore issues.

## Core commands

```bash
# Enter the project environment
pixi shell

# Example targeted pytest run (without coverage for local iteration)
pixi run pytest --no-cov test/unit
```

## ETL orientation

- PUDL processing is orchestrated with Dagster assets.
- Raw inputs are managed via datastore under `$PUDL_INPUT`.
- Primary outputs are Parquet files under `$PUDL_OUTPUT/parquet/`.
- Core and Output layers are the main developer-facing data products.

## Common pitfalls

- Running commands outside the pixi environment.
- Confusing raw/intermediate assets with distributed outputs.
- Forgetting that some validations assume outputs already exist.

## Canonical sources

- `docs/dev/dev_setup.rst`
- `docs/dev/run_the_etl.rst`
- `docs/dev/datastore.rst`
- `docs/dev/clone_ferc1.rst`
- `docs/index.rst`
