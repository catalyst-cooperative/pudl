# pudl.extract.eiaaeo

Extract EIA AEO data from the bulk JSON.

## Attributes

| [`logger`](#pudl.extract.eiaaeo.logger)   |    |
|-------------------------------------------|----|

## Classes

| [`AEOCategory`](#pudl.extract.eiaaeo.AEOCategory)   | Describe how the AEO data is categorized.              |
|-----------------------------------------------------|--------------------------------------------------------|
| [`AEOSeries`](#pudl.extract.eiaaeo.AEOSeries)       | Describe actual AEO timeseries data.                   |
| [`AEOTable`](#pudl.extract.eiaaeo.AEOTable)         | Data schema for a raw AEO table.                       |
| [`AEOTaxonomy`](#pudl.extract.eiaaeo.AEOTaxonomy)   | Container for *all* the information in one AEO report. |

## Functions

| [`raw_eiaaeo`](#pudl.extract.eiaaeo.raw_eiaaeo)(context)                                              | Extract tables from EIA's Annual Energy Outlook.                     |
|-------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| [`raw_table_54_invariants`](#pudl.extract.eiaaeo.raw_table_54_invariants)(→ dagster.AssetCheckResult) | Check that the AEO Table 54 raw data conforms to *some* assumptions. |

## Module Contents

### pudl.extract.eiaaeo.logger

### *class* pudl.extract.eiaaeo.AEOCategory(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Describe how the AEO data is categorized.

Categories are the basic way in which metadata that is shared across
multiple data series is represented.

#### category_id *: [int](https://docs.python.org/3/library/functions.html#int)*

#### parent_category_id *: [int](https://docs.python.org/3/library/functions.html#int)*

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### notes *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### childseries *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### *class* pudl.extract.eiaaeo.AEOSeries(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Describe actual AEO timeseries data.

This includes the data itself as well as some timeseries-specific metadata
that may not be shared across multiple timeseries.

#### series_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### last_updated *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### units *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### data *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [float](https://docs.python.org/3/library/functions.html#float)]]*

### *class* pudl.extract.eiaaeo.AEOTable

Bases: `pandera.pandas.DataFrameModel`

Data schema for a raw AEO table.

#### projection_year *: [int](https://docs.python.org/3/library/functions.html#int)*

#### value *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### units *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### series_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### series_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### category_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### model_case_eiaaeo *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### report_year *: [int](https://docs.python.org/3/library/functions.html#int)*

### *class* pudl.extract.eiaaeo.AEOTaxonomy(records: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)])

Container for *all* the information in one AEO report.

AEO reports are composed of *categories*, which are metadata about multiple
data series, and *series*, which are the actual data + metadata associated
with one specific time series.

The categories and series form a DAG structure with 5 generations: root,
case, subject, leaf category, and data series.

The first generation is the root - there is one root node which is nameless
and which all other nodes descend from.

The second generation is the “cases.” Cases are different scenarios within
the AEO. These have names like “Reference case,” “High Economic Growth”,
“Low Oil and Gas Supply.” All direct children of the root node are cases.

The third generation is the “subjects.” These are high-level tags, with
names like “Energy Prices”, “Energy Consumption”, etc. These are largely
used for filtering in the AEO data UI, so we ignore these.

The fourth generation is the “leaf categories.” These are named things like
“Table 54.  Electric Power Projections by Electricity Market Module Region,
United States” and have a long list of “child series” which actually
contain the data. In other words, these leaf categories map the notion of
an AEO “table” to the actual data.

The fifth generation is the “data series.” These actually contain the data
points, and have no children. They have names like “Electricity : Electric
Power Sector : Cumulative Planned Additions : Coal” and “Coal Supply :
Delivered Prices : Electric Power.” As you can see the names imply a bunch
of different dimensions, which we don’t try to make sense of in the extract
step.

In the first four generations we see a strictly branching tree, but many
leaf categories can point at the same data series so the whole taxonomy is
a DAG. This is because of two reasons:

* the subject tag doesn’t affect data values, but because of the tree
  structure, each leaf category is repeated once for each subject, leading to
  multiple *duplicated* leaf categories pointing at the same data series.
* some data series are relevant to multiple different tables - so multiple
  different leaf categories point at the same data series. In this case we
  would expect the names of the leaf category to reflect their different
  identities.

Note, also, that there is no structural notion of a “Table” in the AEO
data. That information is carried purely by the names of the leaf
categories.

#### *class* EntityType(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

These are the three types of entities in AEO.

#### ROOT *= 1001*

#### CATEGORY *= 1002*

#### SERIES *= 1003*

#### *class* CheckSpec

Encapsulate shared checks for the taxonomy structure.

#### generation *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### typecheck *: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[int](https://docs.python.org/3/library/functions.html#int) | [str](https://docs.python.org/3/library/stdtypes.html#str)], [bool](https://docs.python.org/3/library/functions.html#bool)]*

#### in_degree *: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[int](https://docs.python.org/3/library/functions.html#int)], [bool](https://docs.python.org/3/library/functions.html#bool)]*

#### out_degree *: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[int](https://docs.python.org/3/library/functions.html#int)], [bool](https://docs.python.org/3/library/functions.html#bool)]*

#### graph

#### \_\_cases

#### \_\_sanitize_re

#### \_\_load_records(records: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [AEOCategory](#pudl.extract.eiaaeo.AEOCategory)], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [AEOSeries](#pudl.extract.eiaaeo.AEOSeries)]]

Read AEO JSON blob into memory.

A single JSON object can represent either a category or a series, so we
parse those into two separate mappings.

#### \_\_generate_graph(categories: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [AEOCategory](#pudl.extract.eiaaeo.AEOCategory)], series: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [AEOSeries](#pudl.extract.eiaaeo.AEOSeries)]) → [networkx.DiGraph](https://networkx.org/documentation/stable/reference/classes/digraph.html#networkx.DiGraph)

Stitch categories and series together into a DAG.

#### \_\_generation_invariants() → [list](https://docs.python.org/3/library/stdtypes.html#list)

Check that the graph behaves the way we expect.

We have a few generic checks for *all* generations - node type,
in-degree, and out-degree.

We also have bespoke checks for individual generations as needed.

Returns the list of generations for further manipulation.

#### \_\_sanitize(s: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

#### \_\_series_to_records(series_id: [str](https://docs.python.org/3/library/stdtypes.html#str), potential_parents: [set](https://docs.python.org/3/library/stdtypes.html#set)[[int](https://docs.python.org/3/library/functions.html#int)], report_year: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Turn a data series into records we can feed into a DataFrame.

This uses graph ancestor data to figure out what case this series
belongs to.

This series may be associated with multiple different tables in the
graph. In that case, we’ll need to filter down only to the leaf
categories that are relevant to the table we’re creating a DataFrame
for. We do that by passing in `potential_parents` as a parameter.

#### get_table(table_number: [int](https://docs.python.org/3/library/functions.html#int), report_year: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get a specific table number and report year as a DataFrame.

### pudl.extract.eiaaeo.raw_eiaaeo(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext))

Extract tables from EIA’s Annual Energy Outlook.

We first extract a taxonomy from the AEO JSON blob, which connects
individual data series to “categories”. Some categories are associated with
a specific table; others are associated with an AEO case or subject.

The AEO cases are different scenarios such as “High Economic Growth” or
“High Oil Price.” They include “Reference” and “2022 AEO reference case” as
well.

The AEO subjects are only used for filtering which tables are relevant to
which subjects, e.g. “Table 54 is relevant to Energy Prices.” So we ignore
those right now.

The series each have their own timeseries data, as well as some metadata
such as a series name and units. Many different dimensions can be inferred
from the series names, but the data is somewhat heterogeneous so we do not
try to infer those here and leave that to the transformation step.

### pudl.extract.eiaaeo.raw_table_54_invariants(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dagster.AssetCheckResult](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetCheckResult)

Check that the AEO Table 54 raw data conforms to *some* assumptions.
