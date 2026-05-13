"""Stable Dagster code location module for dg-compatible loading.

This module stays lightweight on purpose. The canonical Dagster assembly lives in
``pudl.dagster.build`` and this module remains the stable top-level entrypoint
configured for ``dg``.
"""

from pudl.dagster.build import build_defs

defs = build_defs()

__all__ = ["defs"]
