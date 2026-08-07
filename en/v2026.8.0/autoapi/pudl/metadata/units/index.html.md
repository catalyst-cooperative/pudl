# pudl.metadata.units

PUDL unit registry built on top of Pint’s default registry.

Extends Pint with energy-industry units (MMBtu, Mcf, MMcf, TBtu), reactive
power (VAr), and a currency dimension (USD). All PUDL
code that needs to parse or validate unit strings should import
PUDL_UNIT_REGISTRY from here rather than constructing a bare UnitRegistry.

Unit strings used in field metadata follow Pint expression syntax with a
slash-with-spaces convention for compound units, e.g. `MMBtu / MWh`,
`USD / MWh`, `short_ton / hour`.

## Attributes

| [`PUDL_UNIT_DEFINITIONS`](#pudl.metadata.units.PUDL_UNIT_DEFINITIONS)   |    |
|-------------------------------------------------------------------------|----|
| [`PUDL_UNIT_REGISTRY`](#pudl.metadata.units.PUDL_UNIT_REGISTRY)         |    |

## Functions

| [`unit_registry_to_frictionless`](#pudl.metadata.units.unit_registry_to_frictionless)(→ dict)   | Return a JSON-serializable dict describing PUDL's custom unit definitions.   |
|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|

## Module Contents

### pudl.metadata.units.PUDL_UNIT_DEFINITIONS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['MMBtu = 1e6 \* BTU = MMBTU', 'Mcf = 1000 \* cubic_foot', 'MMcf = 1e6 \* cubic_foot', 'TBtu = 1e12...*

### pudl.metadata.units.PUDL_UNIT_REGISTRY *: pint.UnitRegistry*

### pudl.metadata.units.unit_registry_to_frictionless() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Return a JSON-serializable dict describing PUDL’s custom unit definitions.

The returned dict is suitable for embedding as `unit_registry` in a
Frictionless datapackage descriptor.  Consumers can reconstruct the registry
by calling `pint.UnitRegistry().define(unit_def)` for each definition string.
