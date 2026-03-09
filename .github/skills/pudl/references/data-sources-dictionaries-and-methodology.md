# Data Sources, Dictionaries, and Methodology

## Use this when

- Mapping a table back to source datasets.
- Interpreting output columns with source caveats.
- Understanding methodological assumptions behind derived outputs.

## Data-source and dictionary workflow

1. Identify the source(s) behind the table (for example `eia923`, `ferc1`).
2. Check source docs for publication/partitioning caveats.
3. Check data dictionaries for table and field semantics.
4. Verify metadata definitions in `src/pudl/metadata/` if needed.

## Methodology workflow

1. Determine whether the value is raw, normalized, or derived.
2. Check methodology docs for assumptions and model behavior.
3. Trace source -> core -> output when debugging anomalies.

## High-value docs to consult

- `docs/data_sources/index.rst`
- `docs/data_sources/other_data.rst`
- `docs/data_sources/future_data.rst`
- `docs/data_dictionaries/index.rst`
- `docs/data_dictionaries/pudl_db.rst`
- `docs/data_dictionaries/ferc1_db.rst`
- `docs/data_dictionaries/codes_and_labels.rst`
- `docs/data_dictionaries/usage_warnings.rst`
- `docs/methodology/index.rst`
- `docs/methodology/timeseries_imputation.rst`
- `docs/methodology/sec10k_modeling.rst`

## Canonical sources

- `src/pudl/metadata/sources.py`
- `src/pudl/metadata/resources/*.py`
- `src/pudl/metadata/fields.py`
