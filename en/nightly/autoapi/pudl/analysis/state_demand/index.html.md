# pudl.analysis.state_demand

Estimate historical hourly state-level electricity demand.

Using hourly electricity demand reported at the balancing authority and utility level in
the FERC 714, and service territories for utilities and balancing authorities inferred
from the counties served by each utility, and the utilities that make up each balancing
authority in the EIA 861, estimate the total hourly electricity demand for each US
state.

This analysis uses the total electricity sales by state reported in the EIA 861 as a
scaling factor to ensure that the magnitude of electricity sales is roughly correct, and
obtains the shape of the demand curve from the hourly planning area demand reported in
the FERC 714.

The compilation of historical service territories based on the EIA 861 data is somewhat
manual and could certainly be improved, but overall the results seem reasonable.
Additional predictive spatial variables will be required to obtain more granular
electricity demand estimates (e.g. at the county level).

## Attributes

| [`STATES`](#pudl.analysis.state_demand.STATES)   |    |
|--------------------------------------------------|----|

## Functions

| [`lookup_state`](#pudl.analysis.state_demand.lookup_state)(→ dict)                                                          | Lookup US state by state identifier.             |
|-----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|
| [`county_assignments_ferc714`](#pudl.analysis.state_demand.county_assignments_ferc714)(→ pandas.DataFrame)                  | Load FERC 714 county assignments.                |
| [`census_counties`](#pudl.analysis.state_demand.census_counties)(→ pandas.DataFrame)                                        | Load county attributes.                          |
| [`total_state_sales_eia861`](#pudl.analysis.state_demand.total_state_sales_eia861)(→ pandas.DataFrame)                      | Read and format EIA 861 sales by state and year. |
| [`out_ferc714__hourly_estimated_state_demand`](#pudl.analysis.state_demand.out_ferc714__hourly_estimated_state_demand)(...) | Estimate hourly electricity demand by state.     |

## Module Contents

### pudl.analysis.state_demand.STATES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]*

### pudl.analysis.state_demand.lookup_state(state: [str](https://docs.python.org/3/library/stdtypes.html#str) | [int](https://docs.python.org/3/library/functions.html#int)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Lookup US state by state identifier.

* **Parameters:**
  **state** – State name, two-letter abbreviation, or FIPS code.
  String matching is case-insensitive.
* **Returns:**
  State identifiers.

### Examples

```pycon
>>> lookup_state('alabama')
{'name': 'Alabama', 'code': 'AL', 'fips': '01'}
>>> lookup_state('AL')
{'name': 'Alabama', 'code': 'AL', 'fips': '01'}
>>> lookup_state(1)
{'name': 'Alabama', 'code': 'AL', 'fips': '01'}
```

### pudl.analysis.state_demand.county_assignments_ferc714(out_ferc714_\_respondents_with_fips) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load FERC 714 county assignments.

* **Parameters:**
  **out_ferc714_\_respondents_with_fips** – From pudl.output.ferc714, FERC 714 respondents
  with county FIPS IDs.
* **Returns:**
  Dataframe with columns
  respondent_id_ferc714, report year (int), and county_id_fips.

### pudl.analysis.state_demand.census_counties(out_censusdp1tract_\_counties: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load county attributes.

* **Parameters:**
  **out_censusdp1tract_\_counties** – The county layer of the Census DP1 geodatabase.
* **Returns:**
  Dataframe with columns county_id_fips and population.

### pudl.analysis.state_demand.total_state_sales_eia861(core_eia861_\_yearly_sales) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read and format EIA 861 sales by state and year.

* **Parameters:**
  **core_eia861_\_yearly_sales** – Electricity sales data from EIA 861.
* **Returns:**
  Dataframe with columns state_id_fips, year, demand_mwh.

### pudl.analysis.state_demand.out_ferc714_\_hourly_estimated_state_demand(context, out_ferc714_\_hourly_planning_area_demand: polars.LazyFrame, out_censusdp1tract_\_counties: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), out_ferc714_\_respondents_with_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_sales: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None) = None) → polars.LazyFrame

Estimate hourly electricity demand by state.

* **Parameters:**
  * **out_ferc714_\_hourly_planning_area_demand** – Hourly demand timeseries, with imputed demand.
  * **out_censusdp1tract_\_counties** – The county layer of the Census DP1 shapefile.
  * **out_ferc714_\_respondents_with_fips** – Annual respondents with the county FIPS IDs
    for their service territories.
  * **core_eia861_\_yearly_sales** – EIA 861 sales data. If provided, the predicted hourly
    demand is scaled to match these totals.
* **Returns:**
  LazyFrame with columns `state_id_fips`, `datetime_utc`, `demand_mwh`, and
  (if `state_totals` was provided) `scaled_demand_mwh`.
