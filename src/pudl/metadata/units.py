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

PUDL_UNIT_REGISTRY: pint.UnitRegistry = pint.UnitRegistry()

# Energy-industry volume and heat units
PUDL_UNIT_REGISTRY.define("MMBtu = 1e6 * BTU = MMBTU")
PUDL_UNIT_REGISTRY.define("Mcf = 1000 * cubic_foot")
PUDL_UNIT_REGISTRY.define("MMcf = 1e6 * cubic_foot")
PUDL_UNIT_REGISTRY.define("TBtu = 1e12 * BTU")

# Reactive power — dimensionally equal to watt in SI, but conventionally
# labeled VAr in power systems to distinguish from real power.
PUDL_UNIT_REGISTRY.define("VAr = watt = volt_ampere_reactive")

# Currency — a custom dimension so that compound units like USD / MWh are
# parseable. Exchange-rate conversions are outside Pint's scope.
PUDL_UNIT_REGISTRY.define("[currency] = [currency]")
PUDL_UNIT_REGISTRY.define("USD = [currency]")
