"""Stable Dagster code location module for dg-compatible loading.

This module stays lightweight on purpose. The canonical registry assembly lives in
``pudl.defs`` and this module remains the stable top-level entrypoint configured for
``dg``.
"""

from pudl.defs import defs

__all__ = ["defs"]
