# pudl.validate

PUDL data validation tooling.

This subpackage provides a home for all PUDL data validation logic, organized by
validation framework and approach:

* [`pudl.validate.dbt`](dbt/index.md#module-pudl.validate.dbt) – wrappers around dbt invocations for custom behavior
* [`pudl.validate.integrity`](integrity/index.md#module-pudl.validate.integrity) – database integrity checks (foreign keys, etc.)
* [`pudl.validate.quality`](quality/index.md#module-pudl.validate.quality) – bespoke data quality checking utilities

Submodules are exported here so callers can use the namespace-qualified idiom:

```default
from pudl.validate import quality as pv
pv.no_null_rows(df)
```

## Submodules

* [pudl.validate.dbt](dbt/index.md)
* [pudl.validate.integrity](integrity/index.md)
* [pudl.validate.quality](quality/index.md)
