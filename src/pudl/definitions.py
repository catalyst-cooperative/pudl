"""Dagster code location module for dg-compatible loading.

This module provides a single ``defs`` object so ``dg`` commands can load the
existing PUDL Definitions without requiring a broader project refactor.
"""

import dagster as dg

import pudl.etl
import pudl.ferc_to_sqlite

defs: dg.Definitions = dg.Definitions.merge(
    pudl.etl.defs,
    pudl.ferc_to_sqlite.defs,
)
