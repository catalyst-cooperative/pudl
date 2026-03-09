# Methodology and Analytics

## Use this when

- Interpreting derived outputs and analytical tables.
- Debugging transformations that rely on imputation/modeling.
- Explaining why computed values differ from raw reports.

## Principles

- Methodology docs describe cross-table logic and assumptions.
- Derived output behavior often reflects these assumptions, not just ETL mechanics.
- When debugging, compare implementation behavior against documented methodology.

## High-value checks

- Confirm whether a value is raw, normalized, or derived.
- Check known caveats before treating an output as ground truth.
- Trace source-to-core-to-output transformations when investigating anomalies.

## Canonical sources

- `docs/methodology/index.rst`
- `docs/methodology/timeseries_imputation.rst`
- `docs/methodology/sec10k_modeling.rst`
- `docs/index.rst`
