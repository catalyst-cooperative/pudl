# pudl.metadata.resources.allocate_gen_fuel

Resource metadata for the allocate_gen_fuel tables.

## Attributes

| [`AGG_FREQS`](#pudl.metadata.resources.allocate_gen_fuel.AGG_FREQS)                                     |    |
|---------------------------------------------------------------------------------------------------------|----|
| [`USAGE_WARNING_DRAWBACK`](#pudl.metadata.resources.allocate_gen_fuel.USAGE_WARNING_DRAWBACK)           |    |
| [`UPSTREAM_ALLOCATION_CONTEXT`](#pudl.metadata.resources.allocate_gen_fuel.UPSTREAM_ALLOCATION_CONTEXT) |    |
| [`KNOWN_DRAWBACKS_DESCRIPTION`](#pudl.metadata.resources.allocate_gen_fuel.KNOWN_DRAWBACKS_DESCRIPTION) |    |
| [`RESOURCE_METADATA`](#pudl.metadata.resources.allocate_gen_fuel.RESOURCE_METADATA)                     |    |

## Module Contents

### pudl.metadata.resources.allocate_gen_fuel.AGG_FREQS *= ['yearly', 'monthly']*

### pudl.metadata.resources.allocate_gen_fuel.USAGE_WARNING_DRAWBACK

### pudl.metadata.resources.allocate_gen_fuel.UPSTREAM_ALLOCATION_CONTEXT *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""The net generation and fuel consumption allocation
method PUDL employs begins with the following context of the originally reported
EIA-860 and EIA-923 data:

* The :ref:`core_eia923__monthly_generation_fuel` table is the authoritative source of
  information about how much generation and fuel consumption is attributable to an
  entire plant. This table has the most complete data coverage, but it is not the most
  granular data reported.
* The :ref:`core_eia923__monthly_generation` table contains the most granular net
  generation data. It is reported at the ``plant_id_eia``, ``generator_id`` and
  ``report_date`` level. This table includes only ~40% of the total MWhs reported in
  the :ref:`core_eia923__monthly_generation_fuel` table.
* The :ref:`core_eia923__monthly_boiler_fuel` table contains the most granular fuel
  consumption data.  It is reported at the boiler/prime mover/energy source level. This
  table includes only ~40% of the total MMBTUs reported in the
  :ref:`core_eia923__monthly_generation_fuel` table.
* The :ref:`core_eia860__scd_generators` table provides an exhaustive list of all
  generators whose generation is being reported in the
  :ref:`core_eia923__monthly_generation_fuel` table.

"""
```

</details>

### pudl.metadata.resources.allocate_gen_fuel.KNOWN_DRAWBACKS_DESCRIPTION *= "This process does not distinguish between primary and secondary energy_sources for generators....*

### pudl.metadata.resources.allocate_gen_fuel.RESOURCE_METADATA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*
