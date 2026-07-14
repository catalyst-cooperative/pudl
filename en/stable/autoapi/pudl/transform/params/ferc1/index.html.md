# pudl.transform.params.ferc1

FERC 1 specific transformation parameters.

These constants are used to construct `pydantic` models, which are validated and
used to control the various data transformations. The definitions of those models can be
found in [`pudl.transform.classes`](../../classes/index.md#module-pudl.transform.classes) and [`pudl.transform.ferc1`](../../ferc1/index.md#module-pudl.transform.ferc1)

## Attributes

| [`PERPOUND_TO_PERSHORTTON`](#pudl.transform.params.ferc1.PERPOUND_TO_PERSHORTTON)                 | Parameters for converting from inverse pounds to inverse short tons.                 |
|---------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [`CENTS_TO_DOLLARS`](#pudl.transform.params.ferc1.CENTS_TO_DOLLARS)                               | Parameters for converting from cents to dollars.                                     |
| [`CENTS_PERMMBTU_TO_USD_PERMMBTU`](#pudl.transform.params.ferc1.CENTS_PERMMBTU_TO_USD_PERMMBTU)   | Parameters for converting from cents per mmbtu to dollars per mmbtu.                 |
| [`PERCF_TO_PERMCF`](#pudl.transform.params.ferc1.PERCF_TO_PERMCF)                                 | Parameters for converting from inverse cubic feet to inverse 1000s of cubic feet.    |
| [`PERGALLON_TO_PERBARREL`](#pudl.transform.params.ferc1.PERGALLON_TO_PERBARREL)                   | Parameters for converting from inverse gallons to inverse barrels.                   |
| [`PERKW_TO_PERMW`](#pudl.transform.params.ferc1.PERKW_TO_PERMW)                                   | Parameters for converting column units from per kW to per MW.                        |
| [`PERKWH_TO_PERMWH`](#pudl.transform.params.ferc1.PERKWH_TO_PERMWH)                               | Parameters for converting column units from per kWh to per MWh.                      |
| [`KW_TO_MW`](#pudl.transform.params.ferc1.KW_TO_MW)                                               | Parameters for converting column units from kW to MW.                                |
| [`KWH_TO_MWH`](#pudl.transform.params.ferc1.KWH_TO_MWH)                                           | Parameters for converting column units from kWh to MWh.                              |
| [`BTU_TO_MMBTU`](#pudl.transform.params.ferc1.BTU_TO_MMBTU)                                       | Parameters for converting column units from BTU to MMBTU.                            |
| [`PERBTU_TO_PERMMBTU`](#pudl.transform.params.ferc1.PERBTU_TO_PERMMBTU)                           | Parameters for converting column units from BTU to MMBTU.                            |
| [`BTU_PERKWH_TO_MMBTU_PERMWH`](#pudl.transform.params.ferc1.BTU_PERKWH_TO_MMBTU_PERMWH)           | Parameters for converting column units from BTU/kWh to MMBTU/MWh.                    |
| [`VALID_PLANT_YEARS`](#pudl.transform.params.ferc1.VALID_PLANT_YEARS)                             | Valid range of years for power plant construction.                                   |
| [`VALID_COAL_MMBTU_PER_TON`](#pudl.transform.params.ferc1.VALID_COAL_MMBTU_PER_TON)               | Valid range for coal heat content, taken from the EIA-923 instructions.              |
| [`VALID_COAL_USD_PER_MMBTU`](#pudl.transform.params.ferc1.VALID_COAL_USD_PER_MMBTU)               | Historical coal price range from the EIA-923 Fuel Receipts and Costs table.          |
| [`VALID_GAS_MMBTU_PER_MCF`](#pudl.transform.params.ferc1.VALID_GAS_MMBTU_PER_MCF)                 | Valid range for gaseous fuel heat content, taken from the EIA-923 instructions.      |
| [`VALID_GAS_USD_PER_MMBTU`](#pudl.transform.params.ferc1.VALID_GAS_USD_PER_MMBTU)                 | Historical natural gas price range from the EIA-923 Fuel Receipts and Costs table.   |
| [`VALID_OIL_MMBTU_PER_BBL`](#pudl.transform.params.ferc1.VALID_OIL_MMBTU_PER_BBL)                 | Valid range for petroleum fuels heat content, taken from the EIA-923 instructions.   |
| [`VALID_OIL_USD_PER_MMBTU`](#pudl.transform.params.ferc1.VALID_OIL_USD_PER_MMBTU)                 | Historical petroleum price range from the EIA-923 Fuel Receipts and Costs table.     |
| [`COAL_COST_PER_MMBTU_CORRECTIONS`](#pudl.transform.params.ferc1.COAL_COST_PER_MMBTU_CORRECTIONS) |                                                                                      |
| [`GAS_COST_PER_MMBTU_CORRECTIONS`](#pudl.transform.params.ferc1.GAS_COST_PER_MMBTU_CORRECTIONS)   |                                                                                      |
| [`OIL_COST_PER_MMBTU_CORRECTIONS`](#pudl.transform.params.ferc1.OIL_COST_PER_MMBTU_CORRECTIONS)   |                                                                                      |
| [`FUEL_COST_PER_MMBTU_CORRECTIONS`](#pudl.transform.params.ferc1.FUEL_COST_PER_MMBTU_CORRECTIONS) |                                                                                      |
| [`COAL_MMBTU_PER_UNIT_CORRECTIONS`](#pudl.transform.params.ferc1.COAL_MMBTU_PER_UNIT_CORRECTIONS) |                                                                                      |
| [`GAS_MMBTU_PER_UNIT_CORRECTIONS`](#pudl.transform.params.ferc1.GAS_MMBTU_PER_UNIT_CORRECTIONS)   |                                                                                      |
| [`OIL_MMBTU_PER_UNIT_CORRECTIONS`](#pudl.transform.params.ferc1.OIL_MMBTU_PER_UNIT_CORRECTIONS)   |                                                                                      |
| [`FUEL_MMBTU_PER_UNIT_CORRECTIONS`](#pudl.transform.params.ferc1.FUEL_MMBTU_PER_UNIT_CORRECTIONS) |                                                                                      |
| [`FERC1_STRING_NORM`](#pudl.transform.params.ferc1.FERC1_STRING_NORM)                             |                                                                                      |
| [`INVALID_PLANT_NAMES`](#pudl.transform.params.ferc1.INVALID_PLANT_NAMES)                         | Invalid plant names which appear in multiple plant tables.                           |
| [`PLANT_FUNCTION_CATEGORIES`](#pudl.transform.params.ferc1.PLANT_FUNCTION_CATEGORIES)             |                                                                                      |
| [`UTILITY_TYPE_CATEGORIES`](#pudl.transform.params.ferc1.UTILITY_TYPE_CATEGORIES)                 |                                                                                      |
| [`PLANT_STATUS`](#pudl.transform.params.ferc1.PLANT_STATUS)                                       |                                                                                      |
| [`FUEL_CATEGORIES`](#pudl.transform.params.ferc1.FUEL_CATEGORIES)                                 | A mapping a canonical fuel name to a set of strings which are used to represent that |
| [`FUEL_UNIT_CATEGORIES`](#pudl.transform.params.ferc1.FUEL_UNIT_CATEGORIES)                       | A mapping of canonical fuel units (keys) to sets of strings representing those fuel  |
| [`PLANT_TYPE_CATEGORIES`](#pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES)                     | A mapping from canonical plant kinds (keys) to the associated freeform strings       |
| [`PLANT_TYPE_CATEGORIES_HYDRO`](#pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES_HYDRO)         | A mapping from canonical plant kinds (keys) to the associated freeform strings       |
| [`CONSTRUCTION_TYPE_CATEGORIES`](#pudl.transform.params.ferc1.CONSTRUCTION_TYPE_CATEGORIES)       | A dictionary of construction types (keys) and lists of construction type strings     |
| [`TRANSFORM_PARAMS`](#pudl.transform.params.ferc1.TRANSFORM_PARAMS)                               | The full set of parameters used to transform the FERC Form 1 data.                   |

## Module Contents

### pudl.transform.params.ferc1.PERPOUND_TO_PERSHORTTON

Parameters for converting from inverse pounds to inverse short tons.

### pudl.transform.params.ferc1.CENTS_TO_DOLLARS

Parameters for converting from cents to dollars.

### pudl.transform.params.ferc1.CENTS_PERMMBTU_TO_USD_PERMMBTU

Parameters for converting from cents per mmbtu to dollars per mmbtu.

### pudl.transform.params.ferc1.PERCF_TO_PERMCF

Parameters for converting from inverse cubic feet to inverse 1000s of cubic feet.

### pudl.transform.params.ferc1.PERGALLON_TO_PERBARREL

Parameters for converting from inverse gallons to inverse barrels.

### pudl.transform.params.ferc1.PERKW_TO_PERMW

Parameters for converting column units from per kW to per MW.

### pudl.transform.params.ferc1.PERKWH_TO_PERMWH

Parameters for converting column units from per kWh to per MWh.

### pudl.transform.params.ferc1.KW_TO_MW

Parameters for converting column units from kW to MW.

### pudl.transform.params.ferc1.KWH_TO_MWH

Parameters for converting column units from kWh to MWh.

### pudl.transform.params.ferc1.BTU_TO_MMBTU

Parameters for converting column units from BTU to MMBTU.

### pudl.transform.params.ferc1.PERBTU_TO_PERMMBTU

Parameters for converting column units from BTU to MMBTU.

### pudl.transform.params.ferc1.BTU_PERKWH_TO_MMBTU_PERMWH

Parameters for converting column units from BTU/kWh to MMBTU/MWh.

### pudl.transform.params.ferc1.VALID_PLANT_YEARS

Valid range of years for power plant construction.

### pudl.transform.params.ferc1.VALID_COAL_MMBTU_PER_TON

Valid range for coal heat content, taken from the EIA-923 instructions.

Lower bound is for waste coal. Upper bound is for bituminous coal.
[https://www.eia.gov/survey/form/eia_923/instructions.pdf](https://www.eia.gov/survey/form/eia_923/instructions.pdf)

### pudl.transform.params.ferc1.VALID_COAL_USD_PER_MMBTU

Historical coal price range from the EIA-923 Fuel Receipts and Costs table.

### pudl.transform.params.ferc1.VALID_GAS_MMBTU_PER_MCF

Valid range for gaseous fuel heat content, taken from the EIA-923 instructions.

Lower bound is for landfill gas. Upper bound is for “other gas”.  Blast furnace gas
(which has very low heat content) is effectively excluded.
[https://www.eia.gov/survey/form/eia_923/instructions.pdf](https://www.eia.gov/survey/form/eia_923/instructions.pdf)

### pudl.transform.params.ferc1.VALID_GAS_USD_PER_MMBTU

Historical natural gas price range from the EIA-923 Fuel Receipts and Costs table.

### pudl.transform.params.ferc1.VALID_OIL_MMBTU_PER_BBL

Valid range for petroleum fuels heat content, taken from the EIA-923 instructions.

Lower bound is for waste oil. Upper bound is for residual fuel oil.
[https://www.eia.gov/survey/form/eia_923/instructions.pdf](https://www.eia.gov/survey/form/eia_923/instructions.pdf)

### pudl.transform.params.ferc1.VALID_OIL_USD_PER_MMBTU

Historical petroleum price range from the EIA-923 Fuel Receipts and Costs table.

### pudl.transform.params.ferc1.COAL_COST_PER_MMBTU_CORRECTIONS

### pudl.transform.params.ferc1.GAS_COST_PER_MMBTU_CORRECTIONS

### pudl.transform.params.ferc1.OIL_COST_PER_MMBTU_CORRECTIONS

### pudl.transform.params.ferc1.FUEL_COST_PER_MMBTU_CORRECTIONS

### pudl.transform.params.ferc1.COAL_MMBTU_PER_UNIT_CORRECTIONS

### pudl.transform.params.ferc1.GAS_MMBTU_PER_UNIT_CORRECTIONS

### pudl.transform.params.ferc1.OIL_MMBTU_PER_UNIT_CORRECTIONS

### pudl.transform.params.ferc1.FUEL_MMBTU_PER_UNIT_CORRECTIONS

### pudl.transform.params.ferc1.FERC1_STRING_NORM

### pudl.transform.params.ferc1.INVALID_PLANT_NAMES

Invalid plant names which appear in multiple plant tables.

### pudl.transform.params.ferc1.PLANT_FUNCTION_CATEGORIES

### pudl.transform.params.ferc1.UTILITY_TYPE_CATEGORIES

### pudl.transform.params.ferc1.PLANT_STATUS

### pudl.transform.params.ferc1.FUEL_CATEGORIES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A mapping a canonical fuel name to a set of strings which are used to represent that
fuel in the FERC Form 1 Reporting.

Case is ignored, as all fuel strings are converted to lower case in the data set.

### pudl.transform.params.ferc1.FUEL_UNIT_CATEGORIES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A mapping of canonical fuel units (keys) to sets of strings representing those fuel
units (values)

### pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A mapping from canonical plant kinds (keys) to the associated freeform strings
(values) identified as being associated with that kind of plant in the FERC Form 1 raw
data.

There are many strings that weren’t categorized, Solar and Solar Project were not
classified as these do not indicate if they are solar thermal or photovoltaic. Variants
on Steam (e.g. “steam 72” and “steam and gas”) were classified based on additional
research of the plants on the Internet.

### pudl.transform.params.ferc1.PLANT_TYPE_CATEGORIES_HYDRO *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A mapping from canonical plant kinds (keys) to the associated freeform strings
(values) identified as being associated with that kind of plant in the FERC Form 1 Hydro
Plants data.

These are seperated out from the rest of the plant types due to the difference in
languaged used to refer to hydro vs. other types of plants. For example: “conventional”
in the context of a hydro plant means that it is conventional hydro-electric. In the
context of the steam table, however, it’s unclear what conventional means.

### pudl.transform.params.ferc1.CONSTRUCTION_TYPE_CATEGORIES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

A dictionary of construction types (keys) and lists of construction type strings
associated with each type (values) from FERC Form 1.

There are many strings that weren’t categorized, including crosses between conventional
and outdoor, PV, wind, combined cycle, and internal combustion. The lists are broken out
into the two types specified in Form 1: conventional and outdoor. These lists are
inclusive so that variants of conventional (e.g. “conventional full”) and outdoor (e.g.
“outdoor full” and “outdoor hrsg”) are included.

### pudl.transform.params.ferc1.TRANSFORM_PARAMS

The full set of parameters used to transform the FERC Form 1 data.

Each item in the dictionary can be used to instantiate a
[`pudl.transform.ferc1.Ferc1TableTransformParams`](../../ferc1/index.md#pudl.transform.ferc1.Ferc1TableTransformParams) object appropriate for
transforming the table identified by that item’s key.
