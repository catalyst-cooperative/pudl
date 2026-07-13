# pudl.metadata.labels

Descriptive labels for coded field values.

## Attributes

| [`ESTIMATED_OR_ACTUAL`](#pudl.metadata.labels.ESTIMATED_OR_ACTUAL)               | Descriptive labels for EIA estimated or actual codes.                  |
|----------------------------------------------------------------------------------|------------------------------------------------------------------------|
| [`POWER_PURCHASE_TYPES_FERC1`](#pudl.metadata.labels.POWER_PURCHASE_TYPES_FERC1) | Descriptive labels for FERC 1 power purchase type codes.               |
| [`COALMINE_TYPES_EIA`](#pudl.metadata.labels.COALMINE_TYPES_EIA)                 | Descriptive labels for coal mine type codes used in EIA 923 reporting. |
| [`CENSUS_REGIONS`](#pudl.metadata.labels.CENSUS_REGIONS)                         | Descriptive labels for Census Region codes.                            |
| [`RTO_ISO`](#pudl.metadata.labels.RTO_ISO)                                       | Descriptive labels for RTO/ISO short codes.                            |
| [`FUEL_UNITS_EIA`](#pudl.metadata.labels.FUEL_UNITS_EIA)                         | Descriptive labels for the units of measure EIA uses for fuels.        |

## Module Contents

### pudl.metadata.labels.ESTIMATED_OR_ACTUAL *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for EIA estimated or actual codes.

### pudl.metadata.labels.POWER_PURCHASE_TYPES_FERC1 *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for FERC 1 power purchase type codes.

### pudl.metadata.labels.COALMINE_TYPES_EIA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for coal mine type codes used in EIA 923 reporting.

These codes and descriptions come from Page 7 of the EIA 923.

### pudl.metadata.labels.CENSUS_REGIONS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for Census Region codes.

Not currently being used.

### pudl.metadata.labels.RTO_ISO *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for RTO/ISO short codes.

Not currently being used.

### pudl.metadata.labels.FUEL_UNITS_EIA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Descriptive labels for the units of measure EIA uses for fuels.

The physical units fuel consumption is reported in.  All consumption is reported in
either short tons for solids, thousands of cubic feet for gases, and barrels for
liquids.
