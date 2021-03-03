"""Metadata constants and methods."""

from . import resources
from .classes import Package, Resource

PACKAGE = Package(
    name="pudl",
    resources=[Resource.dict_from_id(name) for name in resources.RESOURCES]
)

RESOURCES = PACKAGE.resources
