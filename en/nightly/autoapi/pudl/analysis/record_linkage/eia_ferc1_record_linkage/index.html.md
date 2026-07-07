# pudl.analysis.record_linkage.eia_ferc1_record_linkage

Connect FERC1 plant tables to EIA’s plant-parts with record linkage.

FERC plant records are reported very non-uniformly. In the same table there are records
that are reported as whole plants, individual generators, and collections of prime
movers. This means portions of EIA plants that correspond to a plant record in FERC
Form 1 are heterogeneous, which complicates using the two data sets together.

The EIA plant data is much cleaner and more uniformly structured. The are generators
with ids and plants with ids reported in *separate* tables. Several generator IDs are
typically grouped under a single plant ID. In [`pudl.analysis.plant_parts_eia`](../../plant_parts_eia/index.md#module-pudl.analysis.plant_parts_eia),
we create a large number of synthetic aggregated records representing many possible
slices of a power plant which could in theory be what is actually reported in the FERC
Form 1.

In this module we infer which of the many `plant_parts_eia` records is most likely to
correspond to an actually reported FERC Form 1 plant record. This is done with
`splink`, a Python package that implements Fellegi-Sunter’s model of record linkage.

We train the parameters of the `splink` model using manually labeled training data
that links together several thousand EIA and FERC plant records. This trained model is
used to predict matches on the full dataset (see [`get_model_predictions()`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_model_predictions)) using a
threshold match probability to predict if records are a match or not.

The model can return multiple EIA match options for each FERC1 record, so we rank the
matches and choose the one with the highest score. Any matches identified by the model
which are in conflict with our training data are overwritten with the manually
assigned associations (see [`override_bad_predictions()`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.override_bad_predictions)). The final match results
are the connections we keep as the matches between FERC1 plant records and EIA
plant-parts.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.logger)                         |    |
|---------------------------------------------------------------------------------------------------|----|
| [`MATCHING_COLS`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.MATCHING_COLS)           |    |
| [`ID_COL`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.ID_COL)                         |    |
| [`EXTRA_COLS`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.EXTRA_COLS)                 |    |
| [`plant_name_cleaner`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.plant_name_cleaner) |    |
| [`col_cleaner`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.col_cleaner)               |    |

## Functions

| [`get_compiled_input_manager`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_compiled_input_manager)(plants_all_ferc1, ...)                | Get `InputManager` object with compiled inputs for model.                     |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [`get_input_dfs`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_input_dfs)(inputs)                                                         | Get EIA and FERC inputs for the model.                                        |
| [`prepare_for_matching`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.prepare_for_matching)(df, transformed_df)                               | Prepare the input dataframes for matching with splink.                        |
| [`get_training_data_df`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_training_data_df)(inputs)                                           | Get the manually created training data.                                       |
| [`get_model_predictions`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_model_predictions)(eia_df, ferc_df, train_df, ...)                 | Train splink model and output predicted matches.                              |
| [`get_best_matches`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_best_matches)(preds_df, inputs, experiment_tracker)                     | Get the best EIA match for each FERC record and log performance metrics.      |
| [`get_full_records_with_overrides`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_full_records_with_overrides)(best_match_df, inputs, ...) | Join full dataframe onto matches to make usable and get stats.                |
| [`ferc_to_eia`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.ferc_to_eia)(→ pandas.DataFrame)                                                 | Using splink model the connection between FERC1 plants and EIA plant-parts.   |
| [`get_true_pos`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_true_pos)(pred_df, train_df)                                                | Get the number of correctly predicted matches.                                |
| [`get_false_pos`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_false_pos)(pred_df, train_df)                                              | Get the number of incorrectly predicted matches.                              |
| [`get_false_neg`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_false_neg)(pred_df, train_df)                                              | Get the number of matches from the training data where no prediction is made. |
| [`prettyify_best_matches`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.prettyify_best_matches)(→ pandas.DataFrame)                           | Make the EIA-FERC best matches usable.                                        |
| [`check_match_consistency`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.check_match_consistency)(→ pandas.DataFrame)                         | Check how consistent FERC-EIA matches are with FERC-FERC matches.             |
| [`override_bad_predictions`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.override_bad_predictions)(→ pandas.DataFrame)                       | Override incorrect predictions with the correct match from training data.     |
| [`_log_match_coverage`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage._log_match_coverage)(connects_ferc1_eia, experiment_tracker)             |                                                                               |
| [`add_null_overrides`](#pudl.analysis.record_linkage.eia_ferc1_record_linkage.add_null_overrides)(connects_ferc1_eia)                                   | Override known null matches with pd.NA.                                       |

## Module Contents

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.logger

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.MATCHING_COLS *= ['plant_name', 'utility_name', 'fuel_type_code_pudl', 'installation_year', 'construction_year',...*

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.ID_COL *= ['record_id']*

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.EXTRA_COLS *= ['report_year', 'plant_id_pudl', 'utility_id_pudl', 'plant_name_mphone', 'utility_name_mphone']*

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.plant_name_cleaner

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.col_cleaner

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_compiled_input_manager(plants_all_ferc1, fbp_ferc1, plant_parts_eia)

Get `InputManager` object with compiled inputs for model.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_input_dfs(inputs)

Get EIA and FERC inputs for the model.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.prepare_for_matching(df, transformed_df)

Prepare the input dataframes for matching with splink.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_training_data_df(inputs)

Get the manually created training data.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_model_predictions(eia_df, ferc_df, train_df, experiment_tracker)

Train splink model and output predicted matches.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_best_matches(preds_df, inputs, experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker))

Get the best EIA match for each FERC record and log performance metrics.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_full_records_with_overrides(best_match_df, inputs, experiment_tracker)

Join full dataframe onto matches to make usable and get stats.

Override the predictions dataframe with the training data, so that all
known bad predictions are corrected. Then join the EIA and FERC data on
so that the matches are usable. Drop model parameter and match probability
columns generated by splink. Log the coverage of the matches on the
FERC input data.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.ferc_to_eia(experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker), out_ferc1_\_yearly_all_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_ferc1_\_yearly_steam_plants_fuel_by_plant_sched402: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_plant_parts: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Using splink model the connection between FERC1 plants and EIA plant-parts.

* **Parameters:**
  * **out_ferc1_\_yearly_all_plants** – Table of all of the FERC1-reporting plants.
  * **out_ferc1_\_yearly_steam_plants_fuel_by_plant_sched402** – Table of the fuel
    reported aggregated to the FERC1 plant-level.
  * **out_eia_\_yearly_plant_parts** – The EIA plant parts list.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_true_pos(pred_df, train_df)

Get the number of correctly predicted matches.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_false_pos(pred_df, train_df)

Get the number of incorrectly predicted matches.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.get_false_neg(pred_df, train_df)

Get the number of matches from the training data where no prediction is made.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.prettyify_best_matches(matches_best: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plant_parts_eia_true: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plants_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), debug: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Make the EIA-FERC best matches usable.

Use the ID columns from the best matches to merge together both EIA plant-parts data
and FERC plant data. This removes the comparison vectors (the floats between 0 and 1
that compare the two columns from each dataset).

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.check_match_consistency(connects_ferc1_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), train_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker), match_set: Literal['all', 'overrides'] = 'all') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Check how consistent FERC-EIA matches are with FERC-FERC matches.

We have two record linkage processes: one that links FERC plant records across time,
and another that links FERC plant records to EIA plant-parts. This function checks
that the two processes are as consistent with each other as we expect.  Here
“consistent” means that each FERC plant ID is associated with a single EIA plant
parts ID across time. The reverse is not necessarily required – a single EIA plant
part ID may be associated with various FERC plant IDs across time.

* **Parameters:**
  * **connects_ferc1_eia** – Matches of FERC1 to EIA.
  * **train_df** – training data.
  * **match_set** – either `all` - to check all of the matches - or `overrides` - to
    check just the overrides. Default is `all`. The overrides are less
    consistent than all of the data, so this argument changes the consistency
    threshold for this check.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.override_bad_predictions(match_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), train_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Override incorrect predictions with the correct match from training data.

* **Parameters:**
  * **match_df** – A dataframe of the best matches with only one match for each
    FERC1 record.
  * **train_df** – A dataframe of the training data.

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.\_log_match_coverage(connects_ferc1_eia, experiment_tracker: [pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker](../../ml_tools/experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker))

### pudl.analysis.record_linkage.eia_ferc1_record_linkage.add_null_overrides(connects_ferc1_eia)

Override known null matches with pd.NA.

There is no way to indicate in the training data that certain FERC records have no
proper EIA match. That is to say–you can’t specify a blank match or tell the AI
not to match a given record. Because we’ve gone through by hand and know for a fact
that some FERC records have no EIA match (even when you aggregate generators), we
have to add in these null matches after the fact.

This function reads in a list of record_id_ferc1 values that are known to have no
corresponding EIA record match and makes sure they are mapped as NA in the final
record linkage output. It also updates the match_type field to indicate that this
value has been overridden.
