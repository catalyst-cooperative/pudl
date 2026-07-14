# pudl.analysis.plant_parts_eia

Aggregate plant parts to make an EIA master plant-part table.

Practically speaking, a plant is a collection of generator(s). There are many attributes
of generators (i.e. prime mover, primary fuel source, technology type). We can use these
generator attributes to group generator records into larger aggregate records which we
call “plant-parts”. A plant part is a record which corresponds to a particular
collection of generators that all share an identical attribute. E.g. all of the
generators with unit_id=2, or all of the generators with coal as their primary fuel
source.

The EIA data about power plants (from EIA 923 and 860) is reported in tables with
records that correspond to mostly generators and plants. Other datasets (cough cough
FERC1) are less well organized and include plants, generators and other plant-parts all
in the same table without any clear labels. The master plant-part table is an attempt to
create records corresponding to many different plant-parts in order to connect specific
slices of EIA plants to other datasets.

Because generators are often owned by multiple utilities, another dimension of the
plant-part table involves generating two records for each owner: one for the portion of
the plant part they own and one for the plant part as a whole. The portion records are
labeled in the `ownership_record_type` column as “owned” and the total records are
labeled as “total”.

This module refers to “true granularities”. Many plant parts we cobble together here in
the master plant-part list refer to the same collection of infrastructure as other
plant-part list records. For example, if we have a “plant_prime_mover” plant part record
and a “plant_unit” plant part record which were both cobbled together from the same two
generators. We want to be able to reduce the plant-part list to only unique collections
of generators, so we label the first unique granularity as a true granularity and label
the subsequent records as false granularities with the `true_gran` column. In order to
choose which plant-part to keep in these instances, we assigned a hierarchy of plant
parts, the order of the keys in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS) and label whichever plant-part
comes first as the unique granularity.

**Recipe Book for the plant-part list**

[`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS) is the main recipe book for how each of the plant-parts need to
be compiled. These plant-parts represent ways to group generators based on widely
reported values in EIA. All of these are logical ways to group collections of generators
- in most cases - but some groupings of generators are more prevalent or relevant than
others for certain types of plants.

The canonical example here is the `plant_unit`. A unit is a collection of generators
that operate together - most notably the combined-cycle natural gas plants.
Combined-cycle units generally consist of a number of gas turbines which feed excess
steam to a number of steam turbines.

```pycon
>>> df_gens = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1],
...     'generator_id': ['a', 'b', 'c'],
...     'unit_id_pudl': [1, 1, 1],
...     'prime_mover_code': ['CT', 'CT', 'CA'],
...     'capacity_mw': [50, 50, 100],
... })
>>> df_gens
    plant_id_eia    generator_id    unit_id_pudl    prime_mover_code    capacity_mw
0              1               a               1                  CT             50
1              1               b               1                  CT             50
2              1               c               1                  CA            100
```

A good example of a plant-part that isn’t really logical also comes from a
combined-cycle unit. Grouping this example plant by the `prime_mover_code`
would generate two records that would basically never show up in FERC1.
This stems from the inseparability of the generators.

```pycon
>>> df_plant_prime_mover = pd.DataFrame({
...     'plant_id_eia': [1, 1],
...     'plant_part': ['plant_prime_mover', 'plant_prime_mover'],
...     'prime_mover_code': ['CT', 'CA'],
...     'capacity_mw': [100, 100],
... })
>>> df_plant_prime_mover
    plant_id_eia         plant_part    prime_mover_code    capacity_mw
0              1  plant_prime_mover                  CT            100
1              1  plant_prime_mover                  CA            100
```

In this case the unit is more relevant:

```pycon
>>> df_plant_unit = pd.DataFrame({
...     'plant_id_eia': [1],
...     'plant_part': ['plant_unit'],
...     'unit_id_pudl': [1],
...     'capacity_mw': [200],
... })
>>> df_plant_unit
    plant_id_eia    plant_part    unit_id_pudl    capacity_mw
0              1    plant_unit               1            200
```

But if this same plant had both this combined-cycle unit and two more
generators that were self contained “GT” or gas combustion turbine, a logical
way to group these generators is to have different recprds for the
combined-cycle unit and the gas-turbine.

```pycon
>>> df_gens = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1, 1, 1],
...     'generator_id': ['a', 'b', 'c', 'd', 'e'],
...     'unit_id_pudl': [1, 1, 1, 2, 3],
...     'prime_mover_code': ['CT', 'CT', 'CA', 'GT', 'GT'],
...     'capacity_mw': [50, 50, 100, 75, 75],
... })
>>> df_gens
    plant_id_eia    generator_id    unit_id_pudl    prime_mover_code    capacity_mw
0              1               a               1                  CT             50
1              1               b               1                  CT             50
2              1               c               1                  CA            100
3              1               d               2                  GT             75
4              1               e               3                  GT             75
```

```pycon
>>> df_plant_part = pd.DataFrame({
...     'plant_id_eia': [1, 1],
...     'plant_part': ['plant_unit', 'plant_prime_mover'],
...     'unit_id_pudl': [1, pd.NA],
...     'prime_mover_code': [pd.NA, 'GT',],
...     'capacity_mw': [200, 150],
... })
>>> df_plant_part
    plant_id_eia           plant_part    unit_id_pudl    prime_mover_code    capacity_mw
0              1           plant_unit               1                <NA>            200
1              1    plant_prime_mover            <NA>                  GT            150
```

In this case, the `plant_unit` record would have a null
`prime_mover_code` because the unit contains more than one
`prime_mover_code`. Same goes for the `unit_id_pudl` of the
`plant_prime_mover` record. This is handled in the :class:`AddConsistentAttributes`.

**Overview of flow for generating the plant-part table:**

The two main classes which enable the generation of the plant-part table are:

* [`MakeMegaGenTbl`](#pudl.analysis.plant_parts_eia.MakeMegaGenTbl): All of the plant parts are compiled from generators.
  So this class generates a big dataframe of generators with any ID and data
  columns we’ll need. This is also where we add records regarding utility
  ownership slices. The table includes two records for every generator-owner:
  one for the “total” generator (assuming the owner owns 100% of the generator)
  and one for the report ownership fraction of that generator with all of the
  data columns scaled to the ownership fraction.
* [`MakePlantParts`](#pudl.analysis.plant_parts_eia.MakePlantParts): This class uses the generator dataframe as well as
  the information stored in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS) to know how to aggregate each
  of the plant parts. Then we have plant part dataframes with the columns which
  identify the plant part and all of the data columns aggregated to the level of
  the plant part. With that compiled plant part dataframe we also add in qualifier
  columns with [`AddConsistentAttributes`](#pudl.analysis.plant_parts_eia.AddConsistentAttributes). A qualifier column is a column which
  contain data that is not endemic to the plant part record (it is not one of
  the identifying columns or aggregated data columns) but the data is still
  useful data that is attributable to each of the plant part records. For more
  detail on what a qualifier column is, see [`AddConsistentAttributes.execute()`](#pudl.analysis.plant_parts_eia.AddConsistentAttributes.execute).

**Generating the plant-parts list**

For debugging and development purposes, you can build the plant parts using the classes
from this module directly in a notebook or script like this:

```python
gens_mega = MakeMegaGenTbl().execute(
    mcoe=out_eia__yearly_generators,
    own_eia860=out_eia860__yearly_ownership,
)
plant_parts_eia = MakePlantParts().execute(
    gens_mega=out_eia__yearly_generators_by_ownership,
    plants_eia860=out_eia__yearly_plants,
    utils_eia860=out_eia__yearly_utilities,
)
```

## Attributes

| [`logger`](#pudl.analysis.plant_parts_eia.logger)                                       |                                                                                                                  |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS)                             | this dictionary contains a key for each of the 'plant parts' that should end up                                  |
| [`PLANT_PARTS_LITERAL`](#pudl.analysis.plant_parts_eia.PLANT_PARTS_LITERAL)             |                                                                                                                  |
| [`IDX_TO_ADD`](#pudl.analysis.plant_parts_eia.IDX_TO_ADD)                               | list of additional columns to add to the id_cols in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS). |
| [`IDX_OWN_TO_ADD`](#pudl.analysis.plant_parts_eia.IDX_OWN_TO_ADD)                       | list of additional columns beyond the [`IDX_TO_ADD`](#pudl.analysis.plant_parts_eia.IDX_TO_ADD) to add to the    |
| [`SUM_COLS`](#pudl.analysis.plant_parts_eia.SUM_COLS)                                   | list of columns to sum when aggregating a table.                                                                 |
| [`WTAVG_DICT`](#pudl.analysis.plant_parts_eia.WTAVG_DICT)                               | a dictionary of columns (keys) to perform weighted averages on and the weight                                    |
| [`CONSISTENT_ATTRIBUTE_COLS`](#pudl.analysis.plant_parts_eia.CONSISTENT_ATTRIBUTE_COLS) | a list of column names to add as attributes when they are consistent into the                                    |
| [`PRIORITY_ATTRIBUTES_DICT`](#pudl.analysis.plant_parts_eia.PRIORITY_ATTRIBUTES_DICT)   |                                                                                                                  |
| [`MAX_MIN_ATTRIBUTES_DICT`](#pudl.analysis.plant_parts_eia.MAX_MIN_ATTRIBUTES_DICT)     |                                                                                                                  |
| [`FIRST_COLS`](#pudl.analysis.plant_parts_eia.FIRST_COLS)                               |                                                                                                                  |
| [`plant_parts_assets`](#pudl.analysis.plant_parts_eia.plant_parts_assets)               |                                                                                                                  |

## Classes

| [`MakeMegaGenTbl`](#pudl.analysis.plant_parts_eia.MakeMegaGenTbl)                   | Compiler for a MEGA generator table with ownership integrated.              |
|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`MakePlantParts`](#pudl.analysis.plant_parts_eia.MakePlantParts)                   | Compile the plant parts for the master unit list.                           |
| [`PlantPart`](#pudl.analysis.plant_parts_eia.PlantPart)                             | Plant-part table maker.                                                     |
| [`TrueGranLabeler`](#pudl.analysis.plant_parts_eia.TrueGranLabeler)                 | Label the plant-part table records with their true granularity.             |
| [`AddAttribute`](#pudl.analysis.plant_parts_eia.AddAttribute)                       | Base class for adding attributes to plant-part tables.                      |
| [`AddConsistentAttributes`](#pudl.analysis.plant_parts_eia.AddConsistentAttributes) | Adder of attributes records to a plant-part table.                          |
| [`AddPriorityAttribute`](#pudl.analysis.plant_parts_eia.AddPriorityAttribute)       | Add Attributes based on a priority sorting from `PRIORITY_ATTRIBUTES`.      |
| [`AddMaxMinAttribute`](#pudl.analysis.plant_parts_eia.AddMaxMinAttribute)           | Add Attributes based on the maximum or minimum value of a sorted attribute. |

## Functions

| [`out_eia__yearly_generators_by_ownership`](#pudl.analysis.plant_parts_eia.out_eia__yearly_generators_by_ownership)(→ pandas.DataFrame)   | Create mega generators table asset.                                     |
|-------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`plant_part_asset_factory`](#pudl.analysis.plant_parts_eia.plant_part_asset_factory)(→ dagster.AssetsDefinition)                         | Asset factory to create assets for each individual plant part.          |
| [`out_eia__yearly_plant_parts`](#pudl.analysis.plant_parts_eia.out_eia__yearly_plant_parts)(→ pandas.DataFrame)                           | Create plant parts list asset by concatenating all plant-part assets.   |
| [`make_id_cols_list`](#pudl.analysis.plant_parts_eia.make_id_cols_list)(→ list[Any])                                                      | Get a list of the id columns (primary keys) for all of the plant parts. |
| [`make_parts_to_ids_dict`](#pudl.analysis.plant_parts_eia.make_parts_to_ids_dict)(→ dict[str, Any])                                       | Make dict w/ plant-part names (keys) to the main id column (values).    |
| [`add_record_id`](#pudl.analysis.plant_parts_eia.add_record_id)(→ pandas.DataFrame)                                                       | Add a record id to a compiled part df.                                  |
| [`match_to_single_plant_part`](#pudl.analysis.plant_parts_eia.match_to_single_plant_part)(→ pandas.DataFrame)                             | Match data with a variety of granularities to a single plant-part.      |
| [`plant_parts_eia_distinct`](#pudl.analysis.plant_parts_eia.plant_parts_eia_distinct)(→ pandas.DataFrame)                                 | Get the EIA plant_parts with only the unique granularities.             |
| [`reassign_id_ownership_dupes`](#pudl.analysis.plant_parts_eia.reassign_id_ownership_dupes)(→ pandas.DataFrame)                           | Reassign the record_id for the records that are labeled ownership_dupe. |
| [`out_eia__yearly_assn_plant_parts_plant_gen`](#pudl.analysis.plant_parts_eia.out_eia__yearly_assn_plant_parts_plant_gen)(...)            | Build association table between EIA plant parts and EIA generators.     |

## Module Contents

### pudl.analysis.plant_parts_eia.logger

### pudl.analysis.plant_parts_eia.PLANT_PARTS *: [collections.OrderedDict](https://docs.python.org/3/library/collections.html#collections.OrderedDict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)]]*

this dictionary contains a key for each of the ‘plant parts’ that should end up
in the plant parts list. The top-level value for each key is another dictionary, which
contains keys:

* id_cols (the primary key type id columns for this plant part). The
  plant_id_eia column must come first.

* **Type:**
  Dict

### pudl.analysis.plant_parts_eia.PLANT_PARTS_LITERAL

### pudl.analysis.plant_parts_eia.IDX_TO_ADD *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['report_date', 'operational_status_pudl']*

list of additional columns to add to the id_cols in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS).
The id_cols are the base columns that we need to aggregate on, but we also need
to add the report date to keep the records time sensitive and the
operational_status_pudl to separate the operating plant-parts from the
non-operating plant-parts.

* **Type:**
  [list](https://docs.python.org/3/library/stdtypes.html#list)

### pudl.analysis.plant_parts_eia.IDX_OWN_TO_ADD *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['utility_id_eia', 'ownership_record_type']*

list of additional columns beyond the [`IDX_TO_ADD`](#pudl.analysis.plant_parts_eia.IDX_TO_ADD) to add to the
id_cols in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS) when we are dealing with plant-part records
that have been broken out into “owned” and “total” records for each of their
owners.

* **Type:**
  [list](https://docs.python.org/3/library/stdtypes.html#list)

### pudl.analysis.plant_parts_eia.SUM_COLS *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['total_fuel_cost', 'net_generation_mwh', 'capacity_mw', 'capacity_eoy_mw', 'total_mmbtu']*

list of columns to sum when aggregating a table.

* **Type:**
  List

### pudl.analysis.plant_parts_eia.WTAVG_DICT

a dictionary of columns (keys) to perform weighted averages on and the weight
column (values)

* **Type:**
  Dict

### pudl.analysis.plant_parts_eia.CONSISTENT_ATTRIBUTE_COLS *= ['fuel_type_code_pudl', 'planned_generator_retirement_date', 'generator_retirement_date',...*

a list of column names to add as attributes when they are consistent into the
aggregated plant-part records.

All the plant part ID columns must be in consistent attributes.

* **Type:**
  List

### pudl.analysis.plant_parts_eia.PRIORITY_ATTRIBUTES_DICT

### pudl.analysis.plant_parts_eia.MAX_MIN_ATTRIBUTES_DICT

### pudl.analysis.plant_parts_eia.FIRST_COLS *= ['plant_id_eia', 'report_date', 'plant_part', 'generator_id', 'unit_id_pudl',...*

### pudl.analysis.plant_parts_eia.out_eia_\_yearly_generators_by_ownership(out_eia_\_yearly_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia860_\_yearly_ownership: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Create mega generators table asset.

### pudl.analysis.plant_parts_eia.plant_part_asset_factory(part_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Asset factory to create assets for each individual plant part.

### pudl.analysis.plant_parts_eia.plant_parts_assets

### pudl.analysis.plant_parts_eia.out_eia_\_yearly_plant_parts(out_eia_\_yearly_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \*\*part_dfs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Create plant parts list asset by concatenating all plant-part assets.

### *class* pudl.analysis.plant_parts_eia.MakeMegaGenTbl

Compiler for a MEGA generator table with ownership integrated.

### Examples:

**Input Tables**

Here is an example of one plant with three generators. We will use
`capacity_mw` as the data column.

```pycon
>>> mcoe = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1],
...     'report_date': ['2020-01-01', '2020-01-01','2020-01-01'],
...     'generator_id': ['a', 'b', 'c'],
...     'utility_id_eia': [111, 111, 111],
...     'unit_id_pudl': [1, 1, 1],
...     'prime_mover_code': ['CT', 'CT', 'CA'],
...     'technology_description': [
...         'Natural Gas Fired Combined Cycle', 'Natural Gas Fired Combined Cycle', 'Natural Gas Fired Combined Cycle'
...     ],
...     'operational_status': ['existing', 'existing','existing'],
...     'generator_retirement_date': [pd.NA, pd.NA, pd.NA],
...     'capacity_mw': [50, 50, 100],
... }).astype({
...     'generator_retirement_date': "datetime64[ns]",
...     'report_date': "datetime64[ns]",
... })
>>> mcoe
    plant_id_eia    report_date     generator_id   utility_id_eia   unit_id_pudl    prime_mover_code              technology_description   operational_status  generator_retirement_date    capacity_mw
0              1     2020-01-01                a              111              1                  CT    Natural Gas Fired Combined Cycle             existing               NaT              50
1              1     2020-01-01                b              111              1                  CT    Natural Gas Fired Combined Cycle             existing               NaT              50
2              1     2020-01-01                c              111              1                  CA    Natural Gas Fired Combined Cycle             existing               NaT             100
```

The ownership table from EIA 860 includes one record for every owner of
each generator. In this example generator `c` has two owners.

```pycon
>>> df_own_eia860 = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1, 1],
...     'report_date': ['2020-01-01', '2020-01-01','2020-01-01', '2020-01-01'],
...     'generator_id': ['a', 'b', 'c', 'c'],
...     'utility_id_eia': [111, 111, 111, 111],
...     'owner_utility_id_eia': [111, 111, 111, 888],
...     'fraction_owned': [1, 1, .75, .25]
... }).astype({'report_date': "datetime64[ns]"})
>>> df_own_eia860
    plant_id_eia    report_date   generator_id      utility_id_eia  owner_utility_id_eia  fraction_owned
0              1     2020-01-01              a                 111                   111            1.00
1              1     2020-01-01              b                 111                   111            1.00
2              1     2020-01-01              c                 111                   111            0.75
3              1     2020-01-01              c                 111                   888            0.25
```

**Output Mega Generators Table**

`MakeMegaGenTbl().execute(mcoe, df_own_eia860, slice_cols=['capacity_mw'])`
produces the output table `gens_mega` which includes two main sections:
the generators with a “total” ownership stake for each of their owners and
the generators with an “owned” ownership stake for each of their
owners. For the generators that are owned 100% by one utility, the
records are identical except the `ownership_record_type` column. For the
generators that have more than one owner, there are two “total” records
with 100% of the capacity of that generator - one for each owner - and
two “owned” records with the capacity scaled to the ownership stake
of each of the owner utilities - represented by `fraction_owned`.

#### id_cols_list

#### execute(mcoe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), own_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), slice_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = SUM_COLS, validate_own_merge: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'one_to_many') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Make the mega generators table with ownership integrated.

* **Parameters:**
  * **mcoe** – generator-based mcoe table with DEFAULT_GENS_COLS generator attributes
    from [out_eia_\_yearly_generators](../../../../data_dictionaries/pudl_db.md#out-eia-yearly-generators)
  * **own_eia860** – ownership table from [out_eia_\_yearly_generators](../../../../data_dictionaries/pudl_db.md#out-eia-yearly-generators)
  * **scale_cols** – list of columns to slice by ownership fraction in
    [`pudl.helpers.scale_by_ownership()`](../../helpers/index.md#pudl.helpers.scale_by_ownership). Default is [`SUM_COLS`](#pudl.analysis.plant_parts_eia.SUM_COLS)
  * **validate_own_merge** – how the merge between `mcoe` and `own_eia860`
    is to be validated via `pd.merge`. If there should be one
    record for each plant/generator/date in `mcoe` then the default
    `one_to_many` should be used.
* **Returns:**
  a table of all of the generators with identifying columns and data
  columns, sliced by ownership which makes “total” and “owned”
  records for each generator owner. The “owned” records have the
  generator’s data scaled to the ownership percentage (e.g. if a 200
  MW generator has a 75% stake owner and a 25% stake owner, this will
  result in two “owned” records with 150 MW and 50 MW). The “total”
  records correspond to the full plant for every owner (e.g. using
  the same 2-owner 200 MW generator as above, each owner will have a
  records with 200 MW).

#### get_gens_mega_table(mcoe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile the main generators table that will be used as base of PPL.

This generator table will be used to compile all of the “plant-parts”, so we
need to ensure that any of the id columns from the other plant-parts are in this
generator table as well as all of the data columns that we are going to
aggregate to the various plant-parts.

* **Returns:**
  A dataframe of all of the generators there ever were and all of the data
  PUDL has to offer about those generators.

#### label_operating_gens(gen_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Label the operating generators.

We want to distinguish between “operating” generators (those that
report as “existing” and those that retire mid-year) and everything
else so that we can group the operating generators into their own
plant-parts separate from retired or proposed generators. We do this by
creating a new label column called “operational_status_pudl”.

This method also adds a column called “capacity_eoy_mw”, which is the
end of year capacity of the generators. We assume that if a generator
isn’t “existing”, its EOY capacity should be zero.

* **Parameters:**
  **gen_df** – annual table of all generators from EIA.
* **Returns:**
  An annual table of all generators from EIA that operated within each
  reporting year.

### *class* pudl.analysis.plant_parts_eia.MakePlantParts

Compile the plant parts for the master unit list.

This object generates a master list of different “plant-parts”, which
are various collections of generators - i.e. units, fuel-types, whole
plants, etc. - as well as various ownership arrangements. Each
plant-part is included in the master plant-part table associated with
each of the plant-part’s owner twice - once with the data scaled to the
fraction of each owners’ ownership and another for a total plant-part
for each owner.

This master plant parts table is generated by first creating a complete
generators table - with all of the data columns we will be aggregating
to different plant-part’s and sliced and scaled by ownership. Then we use the
complete generator table to aggregate by each of the plant-part
categories. Next we add a label for each plant-part record which indicates
whether or not the record is a unique grouping of generator records.

The coordinating function here is [`execute()`](#pudl.analysis.plant_parts_eia.MakePlantParts.execute).

#### parts_to_ids

#### id_cols_list

#### create_one_plant_part(part_name: [str](https://docs.python.org/3/library/stdtypes.html#str), gens_mega: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Create a table of attributes for one plant part.

#### execute(concatenated_plant_parts: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plants_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utils_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Add 1:m matches and additional columns to concatenated plant parts.

* **Returns:**
  The complete plant parts list

#### add_one_to_many(plant_parts_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), part_name: Literal['plant_match_ferc1'], path_to_one_to_many: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Integrate 1:m FERC-EIA matches into the plant part list.

In the FERC:EIA manual match, more than one EIA record may be matched to a
FERC record. This method reads in a .csv of one to many matches generated during
the validation stage of.

* **Parameters:**
  * **plant_parts_eia** – the master unit list table.
  * **part_name** – should always be “plant_match_ferc1”.
  * **path_to_one_to_many** – a Path to the one_to_many csv file in
    [`pudl.package_data.glue`](../../package_data/glue/index.md#module-pudl.package_data.glue).
* **Returns:**
  The EIA plant parts table with one-to-many matches aggregated as plant
  parts.

#### add_additional_cols(plant_parts_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plants_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utils_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add additional data and id columns.

This method adds a set of either calculated columns or PUDL ID columns.

* **Returns:**
  * utility_id_pudl +
  * plant_id_pudl +
  * capacity_factor +
  * ownership_dupe (boolean): indicator of whether the “owned”
    record has a corresponding “total” duplicate.
* **Return type:**
  The EIA plant parts table with these additional columns

#### \_clean_plant_parts(plant_parts_eia)

#### add_attributes(part_df, attribute_df, part_name)

Add constant and min/max attributes to plant parts.

### *class* pudl.analysis.plant_parts_eia.PlantPart(part_name: Literal[PLANT_PARTS_LITERAL, 'plant_match_ferc1'])

Plant-part table maker.

The coordinating method here is [`execute()`](#pudl.analysis.plant_parts_eia.PlantPart.execute).

**Examples**

Below are some examples of how the main processing step in this class
operates: [`PlantPart.ag_part_by_own_slice()`](#pudl.analysis.plant_parts_eia.PlantPart.ag_part_by_own_slice). If we have a plant with
four generators that looks like this:

```pycon
>>> gens_mega = pd.DataFrame({
...     'plant_id_eia': [1, 1, 1, 1],
...     'report_date': ['2020-01-01', '2020-01-01', '2020-01-01', '2020-01-01',],
...     'utility_id_eia': [111, 111, 111, 111],
...     'generator_id': ['a', 'b', 'c', 'd'],
...     'prime_mover_code': ['ST', 'GT', 'CT', 'CA'],
...     'energy_source_code_1': ['BIT', 'NG', 'NG', 'NG'],
...     'ownership_record_type': ['total', 'total', 'total', 'total',],
...     'operational_status_pudl': ['operating', 'operating', 'operating', 'operating'],
...     'capacity_mw': [400, 50, 125, 75],
... }).astype({
...     'report_date': 'datetime64[ns]',
... })
>>> gens_mega
    plant_id_eia   report_date      utility_id_eia  generator_id    prime_mover_code        energy_source_code_1    ownership_record_type   operational_status_pudl         capacity_mw
0              1    2020-01-01                 111             a                  ST                         BIT                    total                 operating                 400
1              1    2020-01-01                 111             b                  GT                          NG                    total                 operating                  50
2              1    2020-01-01                 111             c                  CT                          NG                    total                 operating                 125
3              1    2020-01-01                 111             d                  CA                          NG                    total                 operating                  75
```

This `gens_mega` table can then be aggregated by `plant`, `plant_prime_fuel`,
`plant_prime_mover`, or `plant_gen`.

#### part_name

#### id_cols

#### execute(gens_mega: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), sum_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = SUM_COLS, wtavg_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict) = WTAVG_DICT) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get a table of data aggregated by a specific plant-part.

This method will take `gens_mega` and aggregate the generator records
to the level of the plant-part. This is mostly done via
[`ag_part_by_own_slice()`](#pudl.analysis.plant_parts_eia.PlantPart.ag_part_by_own_slice). Then several additional columns are added
and the records are labeled as true or false granularities.

* **Returns:**
  a table with records that have been aggregated to a plant-part.

#### ag_part_by_own_slice(gens_mega, sum_cols=SUM_COLS, wtavg_dict=WTAVG_DICT) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Aggregate the plant part by separating ownership types.

There are total records and owned records in this master unit list.
Those records need to be aggregated differently to scale. The “total”
ownership slice is now grouped and aggregated as a single version of the
full plant and then the utilities are merged back. The “owned”
ownership slice is grouped and aggregated with the utility_id_eia, so
the portions of generators created by scale_by_ownership will be
appropriately aggregated to each plant part level.

* **Returns:**
  dataframe aggregated to the level of the
  part_name
* **Return type:**
  [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

#### ag_fraction_owned(part_ag: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Calculate the fraction owned for a plant-part df.

This method takes a dataframe of records that are aggregated to the
level of a plant-part (with certain `id_cols`) and appends a
fraction_owned column, which indicates the % ownership that a
particular utility owner has for each aggregated plant-part record.

For partial owner records (ownership_record_type == “owned”), fraction_owned is
calculated based on the portion of the capacity and the total capacity
of the plant. For total owner records (ownership_record_type == “total”), the
fraction_owned is always 1.

This method is meant to be run after [`ag_part_by_own_slice()`](#pudl.analysis.plant_parts_eia.PlantPart.ag_part_by_own_slice).

* **Parameters:**
  **part_ag**

#### add_new_plant_name(part_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens_mega: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add plants names into the compiled plant part df.

* **Parameters:**
  * **part_df** – dataframe containing records associated
    with one plant part (ex: all plant’s or  plant_prime_mover’s).
  * **gens_mega** – a table of all of the generators with
    identifying columns and data columns, sliced by ownership which
    makes “total” and “owned” records for each generator owner.

#### add_record_count_per_plant(part_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add a record count for each set of plant part records in each plant.

* **Parameters:**
  **part_df** – dataframe containing records associated
  with one plant part (ex: all plant’s or  plant_prime_mover’s).
* **Returns:**
  augmented version of `part_df` with a new column named
  `record_count`

### *class* pudl.analysis.plant_parts_eia.TrueGranLabeler

Label the plant-part table records with their true granularity.

The coordinating function here is :meth\`\`execute\`\`.

#### execute(ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge the true granularity labels onto the plant part df.

This method will add the columns `true_gran`, `appro_part_label`, and
`appro_record_id_eia` to the plant parts list which denote whether
each plant-part is a true or false granularity.

First the plant part list records are matched to generators. Then
the matched records are sorted by the order of keys in PLANT_PARTS and the
highest granularity record for each generator is marked as the true
granularity. The appropriate true granular part label and record id
is then merged on to get the plant part table with true granularity labels.

* **Parameters:**
  **ppe** – (pd.DataFrame) The plant parts list

### *class* pudl.analysis.plant_parts_eia.AddAttribute(attribute_col: [str](https://docs.python.org/3/library/stdtypes.html#str), part_name: [str](https://docs.python.org/3/library/stdtypes.html#str), assign_col_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None)

Base class for adding attributes to plant-part tables.

#### attribute_col

#### part_name

#### id_cols

#### base_cols

#### assign_col_dict *= None*

#### assign_col(gens_mega)

Add a new column to gens_mega.

### *class* pudl.analysis.plant_parts_eia.AddConsistentAttributes(attribute_col: [str](https://docs.python.org/3/library/stdtypes.html#str), part_name: [str](https://docs.python.org/3/library/stdtypes.html#str), assign_col_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None)

Bases: [`AddAttribute`](#pudl.analysis.plant_parts_eia.AddAttribute)

Adder of attributes records to a plant-part table.

#### execute(part_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens_mega: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get qualifier records.

For an individual dataframe of one plant part (e.g. only
“plant_prime_mover” plant part records), we typically have identifying
columns and aggregated data columns. The identifying columns for a
given plant part are only those columns which are required to uniquely
specify a record of that type of plant part. For example, to uniquely
specify a plant_unit record, we need both `plant_id_eia`,
`unit_id_pudl`, `report_date` and nothing else. In other words,
the identifying columns for a given plant part would make up a natural
composite primary key for a table composed entirely of that type of
plant part. Every plant part is cobbled together from generator
records, so each record in each part_df can be thought of as a
collection of generators.

Identifier and qualifier columns are the same columns; whether a column
is an identifier or a qualifier is a function of the plant part you’re
considering. All the other columns which could be identifiers in the
context of other plant parts (but aren’t for this plant part) are
qualifiers.

This method takes a part_df and goes and checks whether or not the data
we are trying to grab from the record_name column is consistent across
every component generator from each record.

* **Parameters:**
  * **part_df** – dataframe containing records associated with one plant part.
  * **gens_mega** – a table of all of the generators with identifying columns and
    data columns, sliced by ownership which makes “total” and “owned”
    records for each generator owner.

#### get_consistent_qualifiers(record_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get fully consistent qualifier records.

When data is a qualifier column is identical for every record in a
plant part, we associate this data point with the record. If the data
points for the related generator records are not identical, then
nothing is associated with the record.

* **Parameters:**
  **record_df** – the dataframe with the record

### *class* pudl.analysis.plant_parts_eia.AddPriorityAttribute(attribute_col: [str](https://docs.python.org/3/library/stdtypes.html#str), part_name: [str](https://docs.python.org/3/library/stdtypes.html#str), assign_col_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None)

Bases: [`AddAttribute`](#pudl.analysis.plant_parts_eia.AddAttribute)

Add Attributes based on a priority sorting from `PRIORITY_ATTRIBUTES`.

This object associates one attribute from the generators that make up a plant-part
based on a sorted list within `PRIORITY_ATTRIBUTES`. For example, for
“operational_status” we will grab the highest level of operational status that is
associated with each records’ component generators. The order of operational status
is defined within the method as: ‘existing’, ‘proposed’, then ‘retired’. For example
if a plant_unit is composed of two generators, and one of them is “existing” and
another is “retired” the entire plant_unit will be considered “existing”.

#### execute(part_df, gens_mega)

Add the attribute to the plant-part df based on priority.

* **Parameters:**
  * **part_df** ([*pandas.DataFrame*](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) – dataframe containing records associated
    with one plant part.
  * **gens_mega** ([*pandas.DataFrame*](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) – a table of all of the generators with
    identifying columns and data columns, sliced by ownership which
    makes “total” and “owned” records for each generator owner.

### *class* pudl.analysis.plant_parts_eia.AddMaxMinAttribute(attribute_col: [str](https://docs.python.org/3/library/stdtypes.html#str), part_name: [str](https://docs.python.org/3/library/stdtypes.html#str), assign_col_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None)

Bases: [`AddAttribute`](#pudl.analysis.plant_parts_eia.AddAttribute)

Add Attributes based on the maximum or minimum value of a sorted attribute.

This object adds an attribute based on the maximum or minimum of another attribute
within a group of plant parts uniquely identified by their base ID columns.

#### execute(part_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), gens_mega: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), att_dtype: [str](https://docs.python.org/3/library/stdtypes.html#str), keep: Literal['first', 'last'] = 'first') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add the attribute to the plant part df based on sorting of another attribute.

* **Parameters:**
  * **part_df** – dataframe containing records associated
    with one plant part.
  * **gens_mega** – a table of all of the generators with
    identifying columns and data columns, sliced by ownership which
    makes “total” and “owned” records for each generator owner.
  * **att_dtype** – Pandas data type of the new attribute
  * **keep** – Whether to keep the first or last record in a sorted
    grouping of attributes. Passing in “first” indicates the new
    attribute is a maximum attribute.
    See [`pandas.DataFrame.drop_duplicates()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html#pandas.DataFrame.drop_duplicates).

### pudl.analysis.plant_parts_eia.make_id_cols_list() → [list](https://docs.python.org/3/library/stdtypes.html#list)[Any]

Get a list of the id columns (primary keys) for all of the plant parts.

* **Returns:**
  a list of the ID columns for all of the plant-parts, including
  `report_date`
* **Return type:**
  [list](https://docs.python.org/3/library/stdtypes.html#list)

### pudl.analysis.plant_parts_eia.make_parts_to_ids_dict() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Make dict w/ plant-part names (keys) to the main id column (values).

All plant-parts have 1 or 2 ID columns in [`PLANT_PARTS`](#pudl.analysis.plant_parts_eia.PLANT_PARTS) plant_id_eia and
a secondary column (with the exception of the “plant” plant-part). The
plant_id_eia column is always first, so we’re going to grab the last column.

* **Returns:**
  Dictionary with plant-part names (keys) corresponding to the main ID column
  (value).

### pudl.analysis.plant_parts_eia.add_record_id(part_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), id_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], plant_part_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'plant_part', year: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add a record id to a compiled part df.

We need a standardized way to refer to these compiled records that contains enough
information in the id itself that in theory we could deconstruct the id and
determine which plant id and plant part id columns are associated with this record.

### pudl.analysis.plant_parts_eia.match_to_single_plant_part(multi_gran_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), part_name: [PLANT_PARTS_LITERAL](#pudl.analysis.plant_parts_eia.PLANT_PARTS_LITERAL) = 'plant_gen', cols_to_keep: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = [], one_to_many: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Match data with a variety of granularities to a single plant-part.

This method merges an input dataframe (`multi_gran_df`) containing data that has a
heterogeneous set of plant-part granularities with a subset of the EIA plant-part
list that has a single granularity.  Currently this is only tested where the single
granularity is generators.  In general this will be a one-to-many merge in which
values from single records in the input data end up associated with several records
from the plant part list.

First, we select a subset of the full EIA plant-part list corresponding to the plant
part specified by the `part_name` argument. In theory this could be the plant,
generator, fuel type, etc. Currently only generators are supported. Then, we iterate
over all the possible plant parts, selecting the subset of records in
`multi_gran_df` that have that granularity, and merge the homogeneous subset of
the plant part list that we selected above onto that subset of the input data. Each
iteration uses a different set of columns to merge on – the columns which define
the primary key for the plant part being merged. Each iteration creates a separate
dataframe, corresponding to a particular plant part, and at the end they are all
concatenated together and returned.

* **Parameters:**
  * **multi_gran_df** – a data table where all records have been linked to EIA plant-part
    list but they may be heterogeneous in its plant-part granularities (i.e.
    some records could be of `plant` plant-part type while others are
    `plant_gen` or `plant_prime_mover`).  All of the plant-part list columns
    need to be present in this table.
  * **ppe** – the EIA plant-part list.
  * **part_name** – name of the single plant part to match to. Must be a key in
    PLANT_PARTS dictionary.
  * **cols_to_keep** – columns from the original data `multi_gran_df` that you want to
    show up in the output. These should not be columns that show up in the
    `ppe`.
  * **one_to_many** – boolean (False by default). If True, add plant_match_ferc1 into
    plant parts list.
* **Returns:**
  A dataframe in which records correspond to `part_name` (in the current
  implementation: the records all correspond to EIA generators!). This is an
  intermediate table that cannot be used directly for analysis because the data
  columns from the original dataset are duplicated and still need to be scaled
  up/down.

### pudl.analysis.plant_parts_eia.plant_parts_eia_distinct(plant_parts_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the EIA plant_parts with only the unique granularities.

Read in the pickled dataframe or generate it from the full PPE. Get only
the records of the PPE that are “true granularities” and those which are not
duplicates based on their ownership so the FERC to EIA matching model
doesn’t get confused as to which option to pick if there are many records
with duplicate data.

* **Parameters:**
  **plant_parts_eia** – EIA plant parts table.

### pudl.analysis.plant_parts_eia.reassign_id_ownership_dupes(plant_parts_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Reassign the record_id for the records that are labeled ownership_dupe.

This function is used after the EIA plant-parts table is created.

* **Parameters:**
  **plant_parts_eia** – EIA plant parts table.

### pudl.analysis.plant_parts_eia.out_eia_\_yearly_assn_plant_parts_plant_gen(out_eia_\_yearly_plant_parts: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build association table between EIA plant parts and EIA generators.

In order to easily determine what generator records are associated with every
plant part record, we made this association table. This table associates every plant part
record (identified as `record_id_eia`) to the possibly many ‘plant_gen’ records
(identified as `record_id_eia_plant_gen`).
