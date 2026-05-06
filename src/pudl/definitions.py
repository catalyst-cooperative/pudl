"""Dagster code location module for dg-compatible loading.

This module provides a single ``defs`` object so ``dg`` commands can load the
existing PUDL Definitions without requiring a broader project refactor.

See https://docs.dagster.io/getting-started/concepts#code-location for more context.
"""

import dagster as dg

import pudl.etl

defs: dg.Definitions = pudl.etl.defs
