# pudl.output.eia860

Denormalized versions of the EIA 860 tables.

## Functions

| [`out_eia860__yearly_ownership`](#pudl.output.eia860.out_eia860__yearly_ownership)(→ pandas.DataFrame)                      | A denormalized version of the EIA 860 ownership table.                  |
|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`out_eia860__yearly_emissions_control_equipment`](#pudl.output.eia860.out_eia860__yearly_emissions_control_equipment)(...) | A denormalized version of the EIA 860 emission control equipment table. |

## Module Contents

### pudl.output.eia860.out_eia860_\_yearly_ownership(\_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_scd_ownership: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_assn_eia_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

A denormalized version of the EIA 860 ownership table.

* **Parameters:**
  * **\_out_eia_\_plants_utilities** – Denormalized table containing plant and utility
    names and IDs.
  * **core_eia860_\_scd_ownership** – EIA 860 ownership table.
  * **core_pudl_\_assn_eia_pudl_utilities** – Table of associations between EIA utility IDs and
    PUDL Utility IDs.
* **Returns:**
  A denormalized version of the EIA 860 ownership table.

### pudl.output.eia860.out_eia860_\_yearly_emissions_control_equipment(core_eia860_\_scd_emissions_control_equipment: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia_\_plants_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

A denormalized version of the EIA 860 emission control equipment table.

* **Parameters:**
  * **core_eia860_\_scd_emissions_control_equipment** – EIA 860 emissions control equipment table.
  * **\_out_eia_\_plants_utilities** – Denormalized table containing plant and utility
    names and IDs.
* **Returns:**
  A denormalized version of the EIA 860 emissions control equipment table.
