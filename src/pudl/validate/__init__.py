"""PUDL data validation tooling.

This subpackage provides a home for all PUDL data validation logic, organized by
validation framework and approach:

* :mod:`pudl.validate.dbt` -- wrappers around dbt invocations for custom behavior
* :mod:`pudl.validate.integrity` -- database integrity checks (foreign keys, etc.)
* :mod:`pudl.validate.quality` -- bespoke data quality checking utilities

Submodules are exported here so callers can use the namespace-qualified idiom::

    from pudl.validate import quality as pv
    pv.no_null_rows(df)
"""

from pudl.validate import dbt, integrity, quality

__all__ = ["dbt", "integrity", "quality"]
