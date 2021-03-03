"""Metadata constants and methods."""

import pydantic

from . import resources
from .classes import Resource

RESOURCES = {}
errors = []
for name in resources.RESOURCES:
    try:
        RESOURCES[name] = Resource.from_id(name)
    except pydantic.ValidationError as error:
        errors.append("\n" + f"[{name}] {error}")
if errors:
    raise ValueError("".join(errors))
