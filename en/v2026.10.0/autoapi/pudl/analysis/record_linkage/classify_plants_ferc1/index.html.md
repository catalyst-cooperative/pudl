# pudl.analysis.record_linkage.classify_plants_ferc1

Scikit-Learn classification pipeline for identifying related FERC 1 plant records.

Sadly FERC doesn’t provide any kind of real IDs for the plants that report to them –
all we have is their names (a freeform string) and the data that is reported alongside
them. This is often enough information to be able to recognize which records ought to be
associated with each other year to year to create a continuous time series. However, we
want to do that programmatically, which means using some clustering / categorization
tools from scikit-learn

## Attributes

| [`_FUEL_COLS`](#pudl.analysis.record_linkage.classify_plants_ferc1._FUEL_COLS)              |    |
|--------------------------------------------------------------------------|----|
| [`ferc_dataframe_embedder`](#pudl.analysis.record_linkage.classify_plants_ferc1.ferc_dataframe_embedder) |    |
| [`_CANONICAL_RECORD_ORDER`](#pudl.analysis.record_linkage.classify_plants_ferc1._CANONICAL_RECORD_ORDER) |    |

## Functions

| [`_canonicalize_plant_ids`](#pudl.analysis.record_linkage.classify_plants_ferc1._canonicalize_plant_ids)(→ pandas.Series)   | Replace arbitrary cluster labels with IDs that depend only on the clusters.   |
|---------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [`assign_plant_ids`](#pudl.analysis.record_linkage.classify_plants_ferc1.assign_plant_ids)(→ pandas.DataFrame)       | Add canonical `plant_id_ferc1` values to the steam table.                     |
| [`merge_steam_fuel_dfs`](#pudl.analysis.record_linkage.classify_plants_ferc1.merge_steam_fuel_dfs)(→ pandas.DataFrame)   | Merge steam plants and fuel dfs to prepare inputs for ferc plant matching.    |
| [`ferc_to_ferc`](#pudl.analysis.record_linkage.classify_plants_ferc1.ferc_to_ferc)(→ pandas.DataFrame)           | Assign IDs to the large steam plants.                                         |

## Module Contents

### pudl.analysis.record_linkage.classify_plants_ferc1.\_FUEL_COLS *= ['coal_fraction_mmbtu', 'gas_fraction_mmbtu', 'nuclear_fraction_mmbtu', 'oil_fraction_mmbtu',...*

### pudl.analysis.record_linkage.classify_plants_ferc1.ferc_dataframe_embedder

### pudl.analysis.record_linkage.classify_plants_ferc1.\_CANONICAL_RECORD_ORDER *= ['report_year', 'utility_id_ferc1', 'plant_name_ferc1', 'record_id']*

### pudl.analysis.record_linkage.classify_plants_ferc1.\_canonicalize_plant_ids(labeled_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Replace arbitrary cluster labels with IDs that depend only on the clusters.

The labels assigned by the clustering models are numbered by the internals of the
algorithm, so they get permuted by tiny changes in the inputs even when the plants
they describe are identical. Instead, give every cluster the position of its
earliest record when all records are sorted by [`_CANONICAL_RECORD_ORDER`](#pudl.analysis.record_linkage.classify_plants_ferc1._CANONICAL_RECORD_ORDER).
Because that only depends on the members of a cluster, splitting, merging, or adding
a plant does not change the IDs of unrelated plants, and diffs between runs
highlight real changes to the clusters.

* **Parameters:**
  **labeled_df** – The records that were clustered, with their cluster label in a
  `record_label` column. Must also contain the columns in
  [`_CANONICAL_RECORD_ORDER`](#pudl.analysis.record_linkage.classify_plants_ferc1._CANONICAL_RECORD_ORDER).
* **Returns:**
  A series of plant IDs, indexed like `labeled_df`.

### pudl.analysis.record_linkage.classify_plants_ferc1.assign_plant_ids(ferc1_steam_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), labeled_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add canonical `plant_id_ferc1` values to the steam table.

* **Parameters:**
  * **ferc1_steam_df** – A DataFrame of the data from the FERC 1 Steam table.
  * **labeled_df** – The records that were clustered, with a `record_label` column
    giving each record’s assigned cluster.
* **Returns:**
  The steam dataframe with a `plant_id_ferc1` column added.

### pudl.analysis.record_linkage.classify_plants_ferc1.merge_steam_fuel_dfs(ferc1_steam_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), fuel_fractions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge steam plants and fuel dfs to prepare inputs for ferc plant matching.

### pudl.analysis.record_linkage.classify_plants_ferc1.ferc_to_ferc(experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.html.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker), core_ferc1_\_yearly_steam_plants_sched402: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_ferc1_\_yearly_steam_plants_fuel_by_plant_sched402: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Assign IDs to the large steam plants.
