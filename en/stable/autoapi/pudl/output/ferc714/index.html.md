# pudl.output.ferc714

Functions & classes for compiling derived aspects of the FERC Form 714 data.

For a narrative overview of the timeseries imputation process, see the documentation
at [Timeseries Imputation](../../../../methodology/timeseries_imputation.md)

## Attributes

| [`logger`](#pudl.output.ferc714.logger)                                                                         |                                                                           |
|-----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| [`ASSOCIATIONS`](#pudl.output.ferc714.ASSOCIATIONS)                                                             | Adjustments to balancing authority-utility associations from EIA 861.     |
| [`UTILITIES`](#pudl.output.ferc714.UTILITIES)                                                                   | Balancing authorities to treat as utilities in associations from EIA 861. |
| [`imputed_hourly_planning_area_demand_assets`](#pudl.output.ferc714.imputed_hourly_planning_area_demand_assets) |                                                                           |

## Functions

| [`categorize_eia_code`](#pudl.output.ferc714.categorize_eia_code)(→ pandas.DataFrame)                                        | Categorize FERC 714 `eia_codes` as either balancing authority or utility IDs.   |
|------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`filled_core_eia861__yearly_balancing_authority`](#pudl.output.ferc714.filled_core_eia861__yearly_balancing_authority)(...) | Modified core_eia861_\_yearly_balancing_authority table.                        |
| [`filled_core_eia861__assn_balancing_authority`](#pudl.output.ferc714.filled_core_eia861__assn_balancing_authority)(...)     | Modified core_eia861_\_assn_balancing_authority table.                          |
| [`filled_service_territory_eia861`](#pudl.output.ferc714.filled_service_territory_eia861)(→ pandas.DataFrame)                | Modified core_eia861_\_yearly_service_territory table.                          |
| [`_out_ferc714__annualized_respondents`](#pudl.output.ferc714._out_ferc714__annualized_respondents)(→ pandas.DataFrame)      | Broadcast respondent data across all years with reported demand.                |
| [`_out_ferc714__categorized_respondents`](#pudl.output.ferc714._out_ferc714__categorized_respondents)(→ pandas.DataFrame)    | Annualized respondents with `respondent_type` assigned if possible.             |
| [`out_ferc714__respondents_with_fips`](#pudl.output.ferc714.out_ferc714__respondents_with_fips)(→ pandas.DataFrame)          | Annual respondents with the county FIPS IDs for their service territories.      |
| [`_out_ferc714__georeferenced_counties`](#pudl.output.ferc714._out_ferc714__georeferenced_counties)(...)                     | Annual respondents with all associated county-level geometries.                 |
| [`out_ferc714__georeferenced_respondents`](#pudl.output.ferc714.out_ferc714__georeferenced_respondents)(...)                 | Annual respondents with a single all-encompassing geometry for each year.       |
| [`out_ferc714__summarized_demand`](#pudl.output.ferc714.out_ferc714__summarized_demand)(→ pandas.DataFrame)                  | Compile annualized, categorized respondents and summarize values.               |

## Module Contents

### pudl.output.ferc714.logger

### pudl.output.ferc714.ASSOCIATIONS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Adjustments to balancing authority-utility associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* id (int): EIA balancing authority identifier (balancing_authority_id_eia).
* from (int): Reference year, to use as a template for target years.
* to (List[int]): Target years, in the closed interval format [minimum, maximum].
  Rows in core_eia861_\_yearly_balancing_authority are added (if missing) for every target year
  with the attributes from the reference year.
  Rows in core_eia861_\_assn_balancing_authority are added (or replaced, if existing)
  for every target year with the utility associations from the reference year.
  Rows in core_eia861_\_yearly_service_territory are added (if missing) for every target year
  with the nearest year’s associated utilities’ counties.
* exclude (Optional[List[str]]): Utilities to exclude, by state (two-letter code).
  Rows are excluded from core_eia861_\_assn_balancing_authority with target year and state.

### pudl.output.ferc714.UTILITIES *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]*

Balancing authorities to treat as utilities in associations from EIA 861.

The changes are applied locally to EIA 861 tables.

* id (int): EIA balancing authority (BA) identifier (balancing_authority_id_eia).
  Rows for id are removed from core_eia861_\_yearly_balancing_authority.
* reassign (Optional[bool]): Whether to reassign utilities to parent BAs.
  Rows for id as BA in core_eia861_\_assn_balancing_authority are removed.
  Utilities assigned to id for a given year are reassigned
  to the BAs for which id is an associated utility.
* replace (Optional[bool]): Whether to remove rows where id is a utility in
  core_eia861_\_assn_balancing_authority. Applies only if reassign=True.

### pudl.output.ferc714.categorize_eia_code(eia_codes: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], ba_ids: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], util_ids: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], priority: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'balancing_authority') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Categorize FERC 714 `eia_codes` as either balancing authority or utility IDs.

Most FERC 714 respondent IDs are associated with an `eia_code` which refers to
either a `balancing_authority_id_eia` or a `utility_id_eia` but no indication
as to which type of ID each one is. This is further complicated by the fact
that EIA uses the same numerical ID to refer to the same entity in most but not all
cases, when that entity acts as both a utility and as a balancing authority.

This function associates a `respondent_type` of `utility`,
`balancing_authority` or `pandas.NA` with each input `eia_code` using the
following rules:

* If a `eia_code` appears only in `util_ids` the `respondent_type` will be
  `utility`.
* If `eia_code` appears only in `ba_ids` the `respondent_type` will be
  assigned `balancing_authority`.
* If `eia_code` appears in neither set of IDs, `respondent_type` will be
  assigned `pandas.NA`.
* If `eia_code` appears in both sets of IDs, then whichever `respondent_type`
  has been selected with the `priority` flag will be assigned.

Note that the vast majority of `balancing_authority_id_eia` values also show up
as `utility_id_eia` values, but only a small subset of the `utility_id_eia`
values are associated with balancing authorities. If you use
`priority="utility"` you should probably also be specifically compiling the list
of Utility IDs because you know they should take precedence. If you use utility
priority with all utility IDs

* **Parameters:**
  * **eia_codes** – A collection of IDs which may be either
    associated with EIA balancing authorities or utilities, to be categorized.
  * **ba_ids_eia** – A collection of IDs which should be
    interpreted as belonging to EIA Balancing Authorities.
  * **util_ids_eia** – A collection of IDs which should be
    interpreted as belonging to EIA Utilities.
  * **priority** – Which respondent_type to give priority to if the eia_code shows
    up in both util_ids_eia and ba_ids_eia. Must be one of “utility” or
    “balancing_authority”. The default is “balancing_authority”.
* **Returns:**
  `eia_code` and `respondent_type`.
* **Return type:**
  A DataFrame containing 2 columns

### pudl.output.ferc714.filled_core_eia861_\_yearly_balancing_authority(core_eia861_\_yearly_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Modified core_eia861_\_yearly_balancing_authority table.

This function adds rows for each balancing authority-year pair missing from the
cleaned core_eia861_\_yearly_balancing_authority table, using a dictionary of manual fixes. It
uses the reference year as a template. The function also removes balancing
authorities that are manually categorized as utilities.

### pudl.output.ferc714.filled_core_eia861_\_assn_balancing_authority(core_eia861_\_assn_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Modified core_eia861_\_assn_balancing_authority table.

This function adds rows for each balancing authority-year pair missing from the
cleaned core_eia861_\_assn_balancing_authority table, using a dictionary of manual fixes.
It uses the reference year as a template. The function also reassigns balancing
authorities that are manually categorized as utilities to their parent balancing
authorities.

### pudl.output.ferc714.filled_service_territory_eia861(core_eia861_\_assn_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Modified core_eia861_\_yearly_service_territory table.

This function adds rows for each balancing authority-year pair missing from the
cleaned core_eia861_\_yearly_service_territory table, using a dictionary of manual fixes. It also
drops utility-state combinations which are missing counties across all years of
data, fills records missing counties with the nearest year of county data for the
same utility and state.

### pudl.output.ferc714.\_out_ferc714_\_annualized_respondents(context, core_ferc714_\_respondent_id: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Broadcast respondent data across all years with reported demand.

The FERC 714 Respondent IDs and names are reported in their own table, without any
reference to individual years, but much of the information we are associating with
them varies annually. This method creates an annualized version of the respondent
table, with each respondent having an entry corresponding to every year for which
FERC 714 has been processed. This means that many of the respondents will end up
having entries for years in which they reported no demand, and that’s fine.
They can be filtered later.

### pudl.output.ferc714.\_out_ferc714_\_categorized_respondents(context, core_ferc714_\_respondent_id: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_ferc714_\_annualized_respondents: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Annualized respondents with `respondent_type` assigned if possible.

Categorize each respondent as either a `utility` or a `balancing_authority`
using the parameters stored in the instance of the class. While categorization
can also be done without annualizing, this function annualizes as well, since we
are adding the `respondent_type` in order to be able to compile service
territories for the respondent, which vary annually.

### pudl.output.ferc714.out_ferc714_\_respondents_with_fips(context, \_out_ferc714_\_categorized_respondents: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_assn_balancing_authority: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_yearly_service_territory: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia861_\_assn_utility: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Annual respondents with the county FIPS IDs for their service territories.

Given the `respondent_type` associated with each respondent (either `utility` or
`balancing_authority`) compile a list of counties that are part of their service
territory on an annual basis, and merge those into the annualized respondent table.
This results in a very long dataframe, since there are thousands of counties and
many of them are served by more than one entity.

Currently respondents categorized as `utility` will include any county that
appears in the `core_eia861__yearly_service_territory` table in association with
that utility ID in each year, while for `balancing_authority` respondents, some
counties can be excluded based on state (if `limit_by_state==True`).

### pudl.output.ferc714.\_out_ferc714_\_georeferenced_counties(out_ferc714_\_respondents_with_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_censusdp1tract_\_counties: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Annual respondents with all associated county-level geometries.

Given the county FIPS codes associated with each respondent in each year, pull in
associated geometries from the US Census DP1 dataset, so we can do spatial analyses.
This keeps each county record independent – so there will be many records for each
respondent in each year. This is fast, and still good for mapping, and retains all
of the FIPS IDs so you can also still do ID based analyses.

### pudl.output.ferc714.out_ferc714_\_georeferenced_respondents(out_ferc714_\_respondents_with_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_ferc714_\_summarized_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_censusdp1tract_\_counties: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Annual respondents with a single all-encompassing geometry for each year.

Given the county FIPS codes associated with each responent in each year, compile a
geometry for the respondent’s entire service territory annually. This results in
just a single record per respondent per year, but is computationally expensive and
you lose the information about what all counties are associated with the respondent
in that year. But it’s useful for merging in other annual data like total demand, so
you can see which respondent-years have both reported demand and decent geometries,
calculate their areas to see if something changed from year to year, etc.

### pudl.output.ferc714.out_ferc714_\_summarized_demand(\_out_ferc714_\_annualized_respondents: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_ferc714_\_hourly_planning_area_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_ferc714_\_categorized_respondents: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_ferc714_\_georeferenced_counties: [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile annualized, categorized respondents and summarize values.

Calculated summary values include:
\* Total reported electricity demand per respondent (`demand_annual_mwh`)
\* Reported per-capita electrcity demand (`demand_annual_per_capita_mwh`)
\* Population density (`population_density_km2`)
\* Demand density (`demand_density_mwh_km2`)

These metrics are helpful identifying suspicious changes in the compiled annual
geometries for the planning areas.

### pudl.output.ferc714.imputed_hourly_planning_area_demand_assets
