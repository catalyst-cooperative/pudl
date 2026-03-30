"""PUDL data validation tooling.

This subpackage provides a home for all PUDL data validation logic, organized by
validation framework and approach:

* :mod:`pudl.validate.dbt` -- wrappers around dbt invocations for custom behavior
* :mod:`pudl.validate.integrity` -- database integrity checks (foreign keys, etc.)
* :mod:`pudl.validate.quality` -- bespoke data quality checking utilities
"""
