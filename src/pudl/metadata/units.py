"""PUDL unit registry built on top of Pint's default registry.

Extends Pint with energy-industry units (MMBtu, Mcf, MMcf, TBtu), reactive
power (VAr), and a currency dimension (USD). All PUDL
code that needs to parse or validate unit strings should import
PUDL_UNIT_REGISTRY from here rather than constructing a bare UnitRegistry.

Unit strings used in field metadata follow Pint expression syntax with a
slash-with-spaces convention for compound units, e.g. ``MMBtu / MWh``,
``USD / MWh``, ``short_ton / hour``.
"""

import pint

# Pint definition strings for every custom unit and dimension.  The ordering
# matters: dimensions must be defined before units that reference them.
PUDL_UNIT_DEFINITIONS: list[str] = [
    # Energy-industry volume and heat units
    "MMBtu = 1e6 * BTU = MMBTU",
    "Mcf = 1000 * cubic_foot",
    "MMcf = 1e6 * cubic_foot",
    "TBtu = 1e12 * BTU",
    # Reactive power — dimensionally equal to watt in SI, but conventionally
    # labeled VAr in power systems to distinguish from real power.
    "VAr = watt = volt_ampere_reactive",
    "MVAr = 1e6 * VAr",
    # Currency — a custom dimension so that compound units like USD / MWh are
    # parseable. Exchange-rate conversions are outside Pint's scope.
    "USD = [currency]",
]

PUDL_UNIT_REGISTRY: pint.UnitRegistry = pint.UnitRegistry()
for _defn in PUDL_UNIT_DEFINITIONS:
    PUDL_UNIT_REGISTRY.define(_defn)


def unit_registry_to_frictionless() -> dict:
    """Return a JSON-serializable dict describing PUDL's custom unit definitions.

    The returned dict is suitable for embedding as ``unit_registry`` in a
    Frictionless datapackage descriptor.  Consumers can reconstruct the registry
    by calling ``pint.UnitRegistry().define(d)`` for each definition string.
    """
    return {"format": "pint", "definitions": PUDL_UNIT_DEFINITIONS}
