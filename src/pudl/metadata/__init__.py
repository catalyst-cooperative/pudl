"""Metadata constants and methods."""

from functools import lru_cache
from typing import Tuple

from .classes import Package, Resource
from .resources import RESOURCE_METADATA

__all__ = ["Package", "Resource", "RESOURCE_METADATA"]


@lru_cache
def build_package(
    resource_ids: Tuple[str] = tuple(sorted(RESOURCE_METADATA))
) -> Package:
    """
    Build and cache a PUDL metadata pacakge. Include all resources by default.

    Args:
        resource_ids: The resource IDs that should be included in the package.
            They should be sorted to avoid unnecessary caching of equivalent
            packages.

    """
    return Package.from_resource_ids(resource_ids)
