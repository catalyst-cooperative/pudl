# pudl.analysis.service_territory

Compile historical utility and balancing area territories.

Use the mapping of utilities to counties, and balancing areas to utilities, available
within the EIA 861, in conjunction with the US Census geometries for counties, to infer
the historical spatial extent of utility and balancing area territories. Output the
resulting geometries for use in other applications.

## Attributes

| [`logger`](#pudl.analysis.service_territory.logger)                                                   |    |
|-------------------------------------------------------------------------------------------------------|----|
| [`MAP_CRS`](#pudl.analysis.service_territory.MAP_CRS)                                                 |    |
| [`CALC_CRS`](#pudl.analysis.service_territory.CALC_CRS)                                               |    |
| [`service_territory_eia861_assets`](#pudl.analysis.service_territory.service_territory_eia861_assets) |    |

## Functions

| [`utility_ids_all_eia`](#pudl.analysis.service_territory.utility_ids_all_eia)(→ pandas.DataFrame)                 | Compile IDs and Names of all known EIA Utilities.                                |
|-------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`get_territory_fips`](#pudl.analysis.service_territory.get_territory_fips)(→ pandas.DataFrame)                   | Compile county FIPS codes associated with an entity's service territory.         |
| [`add_geometries`](#pudl.analysis.service_territory.add_geometries)(→ geopandas.GeoDataFrame)                     | Merge census geometries into dataframe on county_id_fips, optionally dissolving. |
| [`get_territory_geometries`](#pudl.analysis.service_territory.get_territory_geometries)(→ geopandas.GeoDataFrame) | Compile service territory geometries based on county_id_fips.                    |
| [`_save_geoparquet`](#pudl.analysis.service_territory._save_geoparquet)(→ None)                                   | Save utility or balancing authority service territory geometries to GeoParquet.  |
| [`compile_geoms`](#pudl.analysis.service_territory.compile_geoms)(→ pandas.DataFrame)                             | Compile all available utility or balancing authority geometries.                 |
| [`service_territory_asset_factory`](#pudl.analysis.service_territory.service_territory_asset_factory)(...)        | Build asset definitions for balancing authority and utility territories.         |
| [`plot_historical_territory`](#pudl.analysis.service_territory.plot_historical_territory)(→ None)                 | Plot all the historical geometries defined for the specified entity.             |
| [`plot_all_territories`](#pudl.analysis.service_territory.plot_all_territories)(gdf, report_date[, ...])          | Plot all of the planning areas of a given type for a given report date.          |

## Module Contents

### pudl.analysis.service_territory.logger

### pudl.analysis.service_territory.MAP_CRS *= 'EPSG:3857'*

### pudl.analysis.service_territory.CALC_CRS *= 'ESRI:102003'*

### pudl.analysis.service_territory.utility_ids_all_eia(out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile IDs and Names of all known EIA Utilities.

Grab all EIA utility names and IDs from both the EIA 861 Service Territory table and
the EIA 860 Utility entity table. This is a temporary function that’s only needed
because we haven’t integrated the EIA 861 information into the entity harvesting
process and PUDL database yet.

* **Parameters:**
  * **out_eia_\_yearly_utilities** – De-normalized EIA 860 utility attributes table.
  * **core_eia861_\_yearly_service_territory** – Normalized EIA 861 Service Territory table.
* **Returns:**
  A DataFrame having 2 columns `utility_id_eia` and `utility_name_eia`.

### pudl.analysis.service_territory.get_territory_fips(ids: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[int](https://docs.python.org/3/library/functions.html#int)], assn: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), assn_col: [str](https://docs.python.org/3/library/stdtypes.html#str), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), limit_by_state: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile county FIPS codes associated with an entity’s service territory.

For each entity identified by ids, look up the set of counties associated
with that entity on an annual basis. Optionally limit the set of counties
to those within states where the selected entities reported activity elsewhere
within the EIA 861 data.

* **Parameters:**
  * **ids** – A collection of EIA utility or balancing authority IDs.
  * **assn** – Association table, relating `report_date`,
  * **state** – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **other** (*and utility_id_eia to each*) – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **the** (*as well as*) – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **assn_col** – Label of the dataframe column in `assn` that contains
    the ID of the entities of interest. Should probably be either
    `balancing_authority_id_eia` or `utility_id_eia`.
  * **core_eia861_\_yearly_service_territory** – The EIA 861 Service Territory table.
  * **limit_by_state** – Whether to require that the counties associated
    with the balancing authority are inside a state that has also been
    seen in association with the balancing authority and the utility
    whose service territory contains the county.
* **Returns:**
  A table associating the entity IDs with a collection of
  counties annually, identifying counties both by name and county_id_fips
  (both state and state_id_fips are included for clarity).

### pudl.analysis.service_territory.add_geometries(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), census_gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), dissolve: [bool](https://docs.python.org/3/library/functions.html#bool) = False, dissolve_by: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Merge census geometries into dataframe on county_id_fips, optionally dissolving.

Merge the US Census county-level geospatial information into the DataFrame df
based on the the column county_id_fips (in df), which corresponds to the column
GEOID10 in census_gdf. Also bring in the population and area of the counties,
summing as necessary in the case of dissolved geometries.

* **Parameters:**
  * **df** – A DataFrame containing a county_id_fips column.
  * **census_gdf** – A GeoDataFrame based on the US Census
    demographic profile (DP1) data at county resolution, with the original
    column names as published by US Census.
  * **dissolve** – If True, dissolve individual county geometries into larger
    service territories.
  * **dissolve_by** – The columns to group by in the dissolve. For example,
    dissolve_by=[“report_date”, “utility_id_eia”] might provide annual utility
    service territories, while [“report_date”, “balancing_authority_id_eia”]
    would provide annual balancing authority territories.
* **Returns:**
  A GeoDataFrame with the merged census geometries.

### pudl.analysis.service_territory.get_territory_geometries(ids: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[int](https://docs.python.org/3/library/functions.html#int)], assn: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), assn_col: [str](https://docs.python.org/3/library/stdtypes.html#str), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), census_gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), limit_by_state: [bool](https://docs.python.org/3/library/functions.html#bool) = True, dissolve: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Compile service territory geometries based on county_id_fips.

Calls `get_territory_fips` to generate the list of counties associated with
each entity identified by `ids`, and then merges in the corresponding county
geometries from the US Census DP1 data passed in via `census_gdf`.

Optionally dissolve all of the county level geometries into a single geometry for
each combination of entity and year.

#### NOTE
Dissolving geometires is a costly operation, and may take half an hour or more
if you are processing all entities for all years. Dissolving also means that all
the per-county information will be lost, rendering the output inappropriate for
use in many analyses. Dissolving is mostly useful for generating visualizations.

* **Parameters:**
  * **ids** – A collection of EIA balancing authority IDs.
  * **assn** – Association table, relating `report_date`,
  * **state** – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **other** (*and utility_id_eia to each*) – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **the** (*as well as*) – column indicated by `assn_col` – if it’s not `utility_id_eia`.
  * **assn_col** – Label of the dataframe column in `assn` that contains
    the ID of the entities of interest. Should probably be either
    `balancing_authority_id_eia` or `utility_id_eia`.
  * **core_eia861_\_yearly_service_territory** – The EIA 861 Service Territory table.
  * **census_gdf** – The US Census DP1 county-level geometries.
  * **limit_by_state** – Whether to require that the counties associated
    with the balancing authority are inside a state that has also been
    seen in association with the balancing authority and the utility
    whose service territory contains the county.
  * **dissolve** – If False, each record in the compiled territory will correspond
    to a single county, with a county-level geometry, and there will be many
    records enumerating all the counties associated with a given
    balancing_authority_id_eia in each year. If dissolve=True, all of the
    county-level geometries for each utility in each year will be merged
    together (“dissolved”) resulting in a single geometry and record for each
    balancing_authority-year.
* **Returns:**
  A GeoDataFrame with service territory geometries for each entity.

### pudl.analysis.service_territory.\_save_geoparquet(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), entity_type: Literal['utility', 'balancing_authority'], dissolve: [bool](https://docs.python.org/3/library/functions.html#bool), limit_by_state: [bool](https://docs.python.org/3/library/functions.html#bool), output_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Save utility or balancing authority service territory geometries to GeoParquet.

In order to prevent the geometry data from exceeding the 2GB maximum size of an
Arrow object, we need to keep the row groups small. Sort the dataframe by the
primary key columns to minimize the number of values in any row group.  Output
filename is constructed based on input arguments.

* **Parameters:**
  * **gdf** – GeoDataframe containing utility or balancing authority geometries.
  * **entity_type** – string indicating whether we’re outputting utility or balancing
    authority geometries.
  * **dissolve** – Whether the individual county geometries making up the service
    territories have been merged together. Used to construct filename.
  * **limit_by_state** – Whether service territories have been limited to include only
    counties in states where the utilities reported sales. Used to construct
    filename.
  * **output_dir** – Path to the directory where the GeoParquet file will be written.

### pudl.analysis.service_territory.compile_geoms(core_eia861_\_yearly_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_assn_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_assn_utility: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), census_counties: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), entity_type: Literal['balancing_authority', 'utility'], save_format: Literal['geoparquet', 'geodataframe', 'dataframe'], output_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) | [None](https://docs.python.org/3/library/constants.html#None) = None, dissolve: [bool](https://docs.python.org/3/library/functions.html#bool) = False, limit_by_state: [bool](https://docs.python.org/3/library/functions.html#bool) = True, years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)] = []) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile all available utility or balancing authority geometries.

Returns a geoparquet file, geopandas GeoDataFrame or a pandas DataFrame with the
geometry column removed depending on the value of the save_format parameter. By
default, this returns only counties with observed EIA 861 data for a utility or
balancing authority, with geometries available at the county level.

### pudl.analysis.service_territory.service_territory_asset_factory(entity_type: Literal['balancing_authority', 'utility'], io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)]

Build asset definitions for balancing authority and utility territories.

### pudl.analysis.service_territory.service_territory_eia861_assets

### pudl.analysis.service_territory.plot_historical_territory(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), id_col: [str](https://docs.python.org/3/library/stdtypes.html#str), id_val: [str](https://docs.python.org/3/library/stdtypes.html#str) | [int](https://docs.python.org/3/library/functions.html#int)) → [None](https://docs.python.org/3/library/constants.html#None)

Plot all the historical geometries defined for the specified entity.

This is useful for exploring how a particular entity’s service territory has evolved
over time, or for identifying individual missing or inaccurate territories.

* **Parameters:**
  * **gdf** – A geodataframe containing geometries pertaining electricity planning areas.
    Can be broken down by county FIPS code, or have a single record containing a
    geometry for each combination of report_date and the column being used to
    select planning areas (see below).
  * **id_col** – The label of a column in gdf that identifies the planning area to be
    visualized, like `utility_id_eia`, `balancing_authority_id_eia`, or
    `balancing_authority_code_eia`.
  * **id_val** – The ID of the entity whose territory should be plotted.

### pudl.analysis.service_territory.plot_all_territories(gdf: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame), report_date: [str](https://docs.python.org/3/library/stdtypes.html#str), respondent_type: [str](https://docs.python.org/3/library/stdtypes.html#str) | [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = ('balancing_authority', 'utility'), color: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'black', alpha: [float](https://docs.python.org/3/library/functions.html#float) = 0.25)

Plot all of the planning areas of a given type for a given report date.

* **Parameters:**
  * **gdf** – GeoDataFrame containing planning area geometries, organized by
    `respondent_id_ferc714` and `report_date`.
  * **report_date** – A string representing a datetime that indicates what year’s
    planning areas should be displayed.
  * **respondent_type** – Type of respondent whose planning
    areas should be displayed. Either “utility” or
    “balancing_authority” or an iterable collection containing both.
  * **color** – Color to use for the planning areas.
  * **alpha** – Transparency to use for the planning areas.
* **Returns:**
  matplotlib.axes.Axes
