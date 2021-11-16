"""A subpackage to define and organize PUDL database tables by data group."""

import importlib
import pkgutil
from typing import Dict, List

from pudl.metadata.helpers import build_foreign_keys

RESOURCE_METADATA = {}
for module_info in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{__name__}.{module_info.name}")
    resources = module.RESOURCE_METADATA
    for key in resources:
        if "group" not in resources[key]:
            resources[key].update({'group': module_info.name})
    RESOURCE_METADATA.update(resources)

FOREIGN_KEYS: Dict[str, List[dict]] = build_foreign_keys(RESOURCE_METADATA)
"""
Generated foreign key constraints by resource name.

See :func:`pudl.metadata.helpers.build_foreign_keys`.
"""
