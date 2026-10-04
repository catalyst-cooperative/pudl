# pudl.dagster.assets.core.glue

Dagster assets for cross-dataset glue tables.

This module defines assets that build the association tables linking related records
across multiple datasets, including FERC, EIA, and EPA CAMD. Put asset definitions here
when they create or refine those crosswalk-style tables and supporting relationships,
rather than the domain-specific transforms for any one source dataset.

## Attributes

| [`logger`](#pudl.dagster.assets.core.glue.logger)   |    |
|-----------------------------------------------------------|----|

## Functions

| [`create_glue_tables`](#pudl.dagster.assets.core.glue.create_glue_tables)(context)                                 | Extract, transform and load CSVs for the FERC-EIA Glue tables.                   |
|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`raw_pudl__assn_eia_epacamd`](#pudl.dagster.assets.core.glue.raw_pudl__assn_eia_epacamd)(→ pandas.DataFrame)              | Extract the EPACAMD-EIA Crosswalk from the Datastore.                            |
| [`core_epa__assn_eia_epacamd`](#pudl.dagster.assets.core.glue.core_epa__assn_eia_epacamd)(→ pandas.DataFrame)              | Clean up the EPACAMD-EIA Crosswalk file.                                         |
| [`_core_epa__assn_eia_epacamd_unique`](#pudl.dagster.assets.core.glue._core_epa__assn_eia_epacamd_unique)(→ pandas.DataFrame)      | Intermediate asset that contains all unique core_epa_\_assn_eia_epacamd matches. |
| [`correct_epa_eia_plant_id_mapping`](#pudl.dagster.assets.core.glue.correct_epa_eia_plant_id_mapping)(→ pandas.DataFrame)        | Manually correct one plant ID.                                                   |
| [`core_epa__assn_eia_epacamd_subplant_ids`](#pudl.dagster.assets.core.glue.core_epa__assn_eia_epacamd_subplant_ids)(→ pandas.DataFrame) | Groups units and generators into unique subplant groups.                         |
| [`augment_crosswalk_with_generators_eia860`](#pudl.dagster.assets.core.glue.augment_crosswalk_with_generators_eia860)(...)               | Merge any plants that are missing from the EPA crosswalk but appear in EIA-860.  |
| [`augment_crosswalk_with_epacamd_ids`](#pudl.dagster.assets.core.glue.augment_crosswalk_with_epacamd_ids)(→ pandas.DataFrame)      | Merge all EPA CAMD IDs into the crosswalk.                                       |
| [`augment_crosswalk_with_bga_eia860`](#pudl.dagster.assets.core.glue.augment_crosswalk_with_bga_eia860)(→ pandas.DataFrame)       | Merge all EIA Unit IDs into the crosswalk.                                       |
| [`_prep_for_networkx`](#pudl.dagster.assets.core.glue._prep_for_networkx)(→ pandas.DataFrame)                      | Build composite node identities for combustors, generators, and EIA units.       |
| [`_subplant_ids_from_prepped_crosswalk`](#pudl.dagster.assets.core.glue._subplant_ids_from_prepped_crosswalk)(→ pandas.DataFrame)    | Use networkx graph analysis to assign a per-plant `subplant_id`.                 |
| [`make_subplant_ids`](#pudl.dagster.assets.core.glue.make_subplant_ids)(→ pandas.DataFrame)                       | Identify sub-plants in the EPA/EIA crosswalk graph.                              |
| [`manually_update_subplant_id`](#pudl.dagster.assets.core.glue.manually_update_subplant_id)(→ pandas.DataFrame)             | Manually update the subplant_id for `plant_id_eia` 1391.                         |

## Module Contents

### pudl.dagster.assets.core.glue.logger

### pudl.dagster.assets.core.glue.create_glue_tables(context)

Extract, transform and load CSVs for the FERC-EIA Glue tables.

* **Parameters:**
  * **context** – dagster keyword that provides access to resources and config.
  * **core_eia_\_entity_generators** – Static generator attributes compiled from across the EIA-860 and EIA-923 data.
  * **core_eia_\_entity_boilers** – core_eia_\_entity_boilers.
* **Returns:**
  A dictionary of DataFrames whose keys are the names of the corresponding
  database table.

### pudl.dagster.assets.core.glue.raw_pudl_\_assn_eia_epacamd(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Extract the EPACAMD-EIA Crosswalk from the Datastore.

### pudl.dagster.assets.core.glue.core_epa_\_assn_eia_epacamd(context, raw_pudl_\_assn_eia_epacamd: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_boilers: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean up the EPACAMD-EIA Crosswalk file.

In its raw form, the crosswalk contains many fields. The transform process removes
descriptive fields like state, location, facility name, capacity, operating status,
and fuel type that can be found by linking this dataset to others already in the
database. We’re primarily concerned with linking ids in this table, not including
any other plant information.

The raw file contains several fields with the prefix `MOD`. These fields are used
to create a single ID for matches with different spelling. For example,
`EIA_PLANT_ID` (`plant_id_eia` in PUDL) has the generator ID `CTG5` in EPA and
`GT5` in EIA. The `MOD` columns for both of these generators
(`MOD_CAMD_GENERATOR_ID`, `MOD_EIA_GENERATOR_ID_GEN`) converts that value to 5.
The `MATCH_TYPE_GEN`, `MATCH_TYPE_BOILER`, and `PLANT_ID_CHANGE_FLAG` fields
indicate whether these `MOD` fields contain new information. Because we’re not
concerned with creating a new, modified ID field for either EPA or EIA data, we
don’t need these `MOD` or `MATCH_TYPE` columns in our final output. We just care
which EPA value maps to which EIA value.

In terms of cleaning, we implement the standard column name changes: lower-case, no
special characters, and underscores instead of spaces. We also rename some of the
columns for clarity and to match how they appear in the tables you will merge with.
Besides standardizing datatypes (again for merge compatibility) the only meaningful
data alteration we employ here is removing leading zeros from numeric strings on
the `generator_id` and `emissions_unit_id_epa` fields. This is because the same
function is used to clean those same fields in all the other tables in which they
appear. In order to merge properly, we need to clean the values in the crosswalk the
same way. Lastly, we drop all rows without `EIA_PLANT_ID` (`plant_id_eia`)
values because that means that they are unmatched and do not provide any useful
information to users.

It’s important to note that the crosswalk is kept intact (and not separated into
smaller reference tables) because the relationship between the ids is not 1:1. For
example, you can’t isolate the plant_id fields, drop duplicates, and call it a day.
The plant discrepancies depend on which generator ids it’s referring to. This goes
for all fields. Be careful, and do some due diligence before eliminating columns.

We talk more about the complexities regarding EPA “units” in our [Data Source
documentation page for EPACEMS](../../../../../../data_sources/epacems.md).

In it’s original format, the crosswalk is a static file - however, we manually
run the crosswalk code for each year of EIA data, adding the report_date field
to the crosswalk. The plant_id_eia and generator_id fields are foreign keys from an
annualized table. If the fast ETL is run (on one year of data) the test will break
because the crosswalk tables with `plant_id_eia` and `generator_id` contain
values from various years. To keep the crosswalk in alignment with the available eia
data, we’ll restrict it based on the generator entity table which has
`plant_id_eia` and `generator_id` so long as it’s not using the full suite of
available years. If it is, we don’t want to restrict the crosswalk so we can get
warnings and errors from any foreign key discrepancies. This isn’t an ideal
solution, but it works for now.

* **Parameters:**
  * **context** – dagster keyword that provides access to resources and config. For this
    asset, this determines whether the years from the Eia860DataConfig object
    match the EIA860 working partitions. This indicates whether or not to
    restrict the crosswalk data so the tests don’t fail on foreign key
    restraints.
  * **raw_pudl_\_assn_eia_epacamd** – The result of running this module’s extract() function.
  * **core_eia_\_entity_generators** – The core_eia_\_entity_generator table.
  * **core_eia_\_entity_boilers** – The core_eia_\_entity_boilerstable.
* **Returns:**
  A dictionary containing the cleaned EPACAMD-EIA crosswalk DataFrame.

### pudl.dagster.assets.core.glue.\_core_epa_\_assn_eia_epacamd_unique(core_epa_\_assn_eia_epacamd: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Intermediate asset that contains all unique core_epa_\_assn_eia_epacamd matches.

The core_epa_\_assn_eia_epacamd asset contains crosswalk matches from 2018 through
the latest full year of EIA 860 data. This means there are many duplicate matches
found from both years. Several downstream assets expect these matches to be unique,
so this asset will drop duplicates to serve as the input to those downstream assets.
This asset, however, will not itself be written to the PUDL DB. This asset will also
address conflicting matches by taking the match from the most recent year.

* **Parameters:**
  **core_epa_\_assn_eia_epacamd** – Cleaned crosswalk with duplicate matches.
* **Returns:**
  Cleaned crosswalk with duplicates removed.

### pudl.dagster.assets.core.glue.correct_epa_eia_plant_id_mapping(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Manually correct one plant ID.

The EPA’s power sector data crosswalk incorrectly maps plant_id_epa 55248 to
plant_id_eia 55248, when it should be mapped to id 2847.

### pudl.dagster.assets.core.glue.core_epa_\_assn_eia_epacamd_subplant_ids(\_core_epa_\_assn_eia_epacamd_unique: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_scd_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_epacems_\_emissions_unit_ids: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_assn_boiler_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Groups units and generators into unique subplant groups.

This takes [`_core_epa__assn_eia_epacamd_unique()`](#pudl.dagster.assets.core.glue._core_epa__assn_eia_epacamd_unique) as an input so this asset
doesn’t have to deal with duplicate matches that may be present in the
[`core_epa__assn_eia_epacamd()`](#pudl.dagster.assets.core.glue.core_epa__assn_eia_epacamd) asset due to its use of multiple years of raw
crosswalk outputs.

This function consists of two primary parts:

1. Augment the EPA CAMD:EIA crosswalk with all IDs from EIA and EPA CAMD. Fill in
   key IDs when possible. Because the published crosswalk was only meant to map
   CAMD units to EIA generators, it is missing a large number of subplant_ids for
   generators that do not report to CEMS. Before applying this function to the
   subplant crosswalk, the crosswalk must be completed with all generators by outer
   merging in the complete list of generators from EIA-860. This dataframe also
   contains the complete list of `unit_id_pudl` mappings that will be necessary.
2. [`make_subplant_ids()`](#pudl.dagster.assets.core.glue.make_subplant_ids): Use graph analysis to identify distinct groupings of
   EPA units, EIA generators, and EIA-860 boiler-generator `unit_id_pudl`
   associations, based on 1:1, 1:m, m:1, or m:m relationships.

* **Returns:**
  table of cems_ids and with subplant_id added

### pudl.dagster.assets.core.glue.augment_crosswalk_with_generators_eia860(crosswalk_clean: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_scd_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge any plants that are missing from the EPA crosswalk but appear in EIA-860.

* **Parameters:**
  * **crosswalk_clean** – transformed EPA CEMS-EIA crosswalk.
  * **core_eia860_\_scd_generators** – EIA860 generators table.

### pudl.dagster.assets.core.glue.augment_crosswalk_with_epacamd_ids(crosswalk_clean: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_epacems_\_emissions_unit_ids: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge all EPA CAMD IDs into the crosswalk.

### pudl.dagster.assets.core.glue.augment_crosswalk_with_bga_eia860(crosswalk_clean: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia860_\_assn_boiler_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge all EIA Unit IDs into the crosswalk.

### pudl.dagster.assets.core.glue.\_prep_for_networkx(crosswalk: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build composite node identities for combustors, generators, and EIA units.

Node identities are plain `(type_tag, plant_id_eia, natural_id)` tuples rather
than numeric surrogates. networkx accepts any hashable object as a node, so
there’s no need to invent a global integer ID space – crosswalk associations
never connect two different plants, so embedding `plant_id_eia` directly in
each node is enough to keep every plant’s subgraph naturally disjoint from every
other plant’s, without ever computing a plant-spanning “global” ID.
`type_tag` (`"combustor"`/`"generator"`/`"unit"`) keeps otherwise-identical
IDs from colliding, e.g. a generator and a boiler that both happen to be named
`"1"` at the same plant, which is a common EIA naming convention.

A row missing its natural id (e.g. an EPA unit that never matched an EIA
generator) is keyed by its own row position instead of left null, so two such
rows are never accidentally treated as sharing an id.

* **Parameters:**
  **crosswalk** – The [core_epa_\_assn_eia_epacamd](../../../../../../data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) crosswalk, augmented with
  `unit_id_pudl` (see [`augment_crosswalk_with_bga_eia860()`](#pudl.dagster.assets.core.glue.augment_crosswalk_with_bga_eia860)).
* **Returns:**
  A copy of [core_epa_\_assn_eia_epacamd](../../../../../../data_dictionaries/pudl_db.md#core-epa-assn-eia-epacamd) crosswalk with new node-identity
  columns `combustor_node`, `generator_node`, and `unit_node`.

### pudl.dagster.assets.core.glue.\_subplant_ids_from_prepped_crosswalk(prepped: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Use networkx graph analysis to assign a per-plant `subplant_id`.

* **Parameters:**
  **prepped** – `core_epa__assn_eia_epacamd` crosswalk passed through
  [`_prep_for_networkx()`](#pudl.dagster.assets.core.glue._prep_for_networkx)
* **Returns:**
  A copy of `core_epa__assn_eia_epacamd` crosswalk plus a new `subplant_id`
  column. `subplant_id` is already 0-indexed and contiguous within each
  `plant_id_eia` – no separate global-to-composite-ID conversion step is
  needed, since node identities already embed `plant_id_eia` (see
  [`_prep_for_networkx()`](#pudl.dagster.assets.core.glue._prep_for_networkx)) and crosswalk edges never connect two plants.

### pudl.dagster.assets.core.glue.make_subplant_ids(crosswalk: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Identify sub-plants in the EPA/EIA crosswalk graph.

In graph analysis terminology, the crosswalk is a list of edges between nodes (EPA
emissions units and EIA generators) in a bipartite graph. A third node type, EIA-860
boiler-generator `unit_id_pudl` groupings, is folded into the same graph as well,
connecting generators that share a `unit_id_pudl` even where the EPA crosswalk
doesn’t connect them directly (see [`_subplant_ids_from_prepped_crosswalk()`](#pudl.dagster.assets.core.glue._subplant_ids_from_prepped_crosswalk)).
The networkx python package provides functions to analyze this graph and extract
disjoint subgraphs (groups of units, generators, and unit_id_pudl groupings that are
connected to each other). These are the distinct power plants. To avoid a name
collision with plant_id, we term these collections ‘subplants’, and identify them
with a subplant_id that is unique within each plant_id. Subplants are thus
identified with the composite key (plant_id_eia, subplant_id).

Through this analysis, we found that 56% of plant_ids contain multiple distinct
subplants, and 11% contain subplants with different technology types, such as a gas
boiler and gas turbine (not in a combined cycle).

Any row filtering should be done before this step if desired. Rows that are
duplicates with respect to the EPA unit/EIA generator association – e.g. two
crosswalk rows that only differ in `generator_id_epa` or `boiler_id`, neither
of which appears in the output – are collapsed here explicitly, since silently
leaving that to the graph construction below would make its row-count guarantee
(see [`_subplant_ids_from_prepped_crosswalk()`](#pudl.dagster.assets.core.glue._subplant_ids_from_prepped_crosswalk)) fail on real data.

Note that sub-plant ids should be used in conjunction with `plant_id_eia` rather
than `plant_id_epa` because the former is more granular and integrated into CEMS
during the transform process.

* **Parameters:**
  **crosswalk** – The core_epa_\_assn_eia_epacamd crosswalk
* **Returns:**
  An edge list connecting EPA units to EIA generators, with connected pieces
  issued a subplant_id

### pudl.dagster.assets.core.glue.manually_update_subplant_id(subplant_crosswalk: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Manually update the subplant_id for `plant_id_eia` 1391.

This function lumps all records within `plant_id_eia` 1391 into the same
`subplant_id` group. See comment <https://github.com/singularity-energy/open-grid-emissions/pull/142#issuecomment-1186579260>_
for explanation of why.
