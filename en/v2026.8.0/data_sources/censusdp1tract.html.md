# Census DP1 – Profile of General Demographic Characteristics

| Source URL                      | [https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html](https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html)                                      |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Description              | US Census Demographic Profile 1 (DP1) County and Tract GeoDatabase.                                                                                                                         |
| Download Size                   | 275 MB                                                                                                                                                                                      |
| Temporal Coverage               |                                                                                                                                                                                             |
| PUDL Code                       | `censusdp1tract`                                                                                                                                                                            |
| Unprocessed Source Data Archive | [10.5281/zenodo.4127048](https://doi.org/10.5281/zenodo.4127048)                                                                                                                            |
| Issues                          | [Open Census DP1 – Profile of General Demographic Characteristics issues](https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Acensusdp1tract) |

## PUDL Database Tables

We’ve segmented the processed data into the following normalized data tables.
Clicking on the links will show you a description of the table as well as
the names and descriptions of each of its fields.

| Data Dictionary                                                                              | Browse Online                                                                                                                                 |
|----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| [out_censusdp1tract_\_counties](../data_dictionaries/pudl_db.md#out-censusdp1tract-counties) | [https://data.catalyst.coop/preview/pudl/out_censusdp1tract_\_counties](https://data.catalyst.coop/preview/pudl/out_censusdp1tract__counties) |
| [out_censusdp1tract_\_states](../data_dictionaries/pudl_db.md#out-censusdp1tract-states)     | [https://data.catalyst.coop/preview/pudl/out_censusdp1tract_\_states](https://data.catalyst.coop/preview/pudl/out_censusdp1tract__states)     |
| [out_censusdp1tract_\_tracts](../data_dictionaries/pudl_db.md#out-censusdp1tract-tracts)     | [https://data.catalyst.coop/preview/pudl/out_censusdp1tract_\_tracts](https://data.catalyst.coop/preview/pudl/out_censusdp1tract__tracts)     |

## Background

In 2010 the US Census published its [Demographic Profile 1 (DP1)](https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html) data in a
convenient geospatial database. This database included select demographic information at
the state, county, and census tract level, along with the geometries of those geographic
areas. The single file format was easy for us to archive and convert to SQLite for
integration with other data in PUDL early on in the project.

It continues to be a convenient source of state and county boundaries for use in mapping
and visualizations, and we use county-level population to provide context for the
FERC-714 hourly electricity demand. However, it should not be considered canonical
information since it’s from 2010 and more recent analogous data is available from the
Census directly.

We convert the original GeoDatabase to SQLite using the [ogr2ogr](https://gdal.org/en/stable/programs/ogr2ogr.html) command-line tool that is part of
the [open source geospatial library GDAL](https://gdal.org/en/stable/index.html). The
resulting state, county, and tract level tables are then output to [GeoParquet](https://geoparquet.org/), which preserves the coordinate reference system and
provides performant access to the vectorized geometries.

We rename the handful of columns that do not contain demographic data to be more legible
and consistent with the rest of the PUDL database. The ~170 demographic data columns are
left named by their standard Census data series IDs, which begin with `dp`, followed
by 7-digits.

### Download additional documentation

### Data available through PUDL

While the Census continues to compile similar demographic summary data, it has not
published subsequent versions in the same single-file geodatabase format that was used
for the 2010 data. As a result we have not updated this dataset since its initial
integration into PUDL in 2020.

For more extensive and up-to-date US Census data, check out the list of available
[Census APIs](https://www.census.gov/data/developers/data-sets.html).

### Who submits this data?

This data is summarized from the 2010 decennial census.

### What does the original data look like?

The original data is published as an [ESRI GeoDatabase](https://en.wikipedia.org/wiki/Geodatabase_(Esri)).

## Notable Irregularities

* The Census DP1 data we are using dates from the 2010 census. Jurisdiction boundaries,
  names, and IDs associated with individual counties and census tracts do change over
  time, so this data contains some minor discrepancies with present-day names, IDs, and
  boundaries. The population estimates and other demographic information it contains is
  also out of date. If you need more accurate geometries and demographics, you should go
  directly to canonical US Census data sources.

## PUDL Data Transformations

To see the transformations applied to the data in each table, you can read the
docstrings for `pudl.transform.censusdp1tract` created for each table’s
respective transform function.
