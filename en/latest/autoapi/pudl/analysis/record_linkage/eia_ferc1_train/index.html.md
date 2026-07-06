# pudl.analysis.record_linkage.eia_ferc1_train

Create spreadsheets for manually mapping FERC-EIA records and validate matches.

[`pudl.analysis.record_linkage.eia_ferc1_record_linkage`](../eia_ferc1_record_linkage/index.md#module-pudl.analysis.record_linkage.eia_ferc1_record_linkage) uses machine learning to link records from FERC Form 1
with records from EIA. While this process is way more efficient and logical
than a human, it requires a set of hand-compiled training data in order to do it’s job.

The training data also serve as overrides for otherwise bad AI matches. There are
several examples of plants that require human intuition to make sense of. For instance,
sometimes FERC capacities lag behind by several years or are comprised of two or more
EIA records.

This module creates an output spreadsheet, based on a certain utility, that makes the
matching and machine-matched human validation process much easier. It also contains
functions that will read those new/updated/validated matches from the spreadsheet,
validate them, and incorporate them into the existing training data.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.eia_ferc1_train.logger)                               |    |
|------------------------------------------------------------------------------------------------|----|
| [`RENAME_COLS_FERC1_EIA`](#pudl.analysis.record_linkage.eia_ferc1_train.RENAME_COLS_FERC1_EIA) |    |
| [`RELEVANT_COLS_PPE`](#pudl.analysis.record_linkage.eia_ferc1_train.RELEVANT_COLS_PPE)         |    |

## Functions

| [`_pct_diff`](#pudl.analysis.record_linkage.eia_ferc1_train._pct_diff)(→ pandas.DataFrame)                                       | Calculate percent difference between EIA and FERC versions of a column.        |
|----------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`_is_best_match`](#pudl.analysis.record_linkage.eia_ferc1_train._is_best_match)(→ pandas.DataFrame)                             | Fill the best_match column with strings to show cap, net_gen, inst_year match. |
| [`_prep_eia_ferc1`](#pudl.analysis.record_linkage.eia_ferc1_train._prep_eia_ferc1)(→ pandas.DataFrame)                           | Prep FERC-EIA for use in override output sheet pre-utility subgroups.          |
| [`_prep_deprish`](#pudl.analysis.record_linkage.eia_ferc1_train._prep_deprish)(→ pandas.DataFrame)                               | Prep depreciation data for use in override output sheet pre-utility subgroups. |
| [`_get_util_year_subsets`](#pudl.analysis.record_linkage.eia_ferc1_train._get_util_year_subsets)(→ dict)                         | Get utility and year subsets for each of the input dfs.                        |
| [`_output_override_spreadsheet`](#pudl.analysis.record_linkage.eia_ferc1_train._output_override_spreadsheet)(→ None)             | Output spreadsheet with tabs for ferc-eia, ppe, deprish for one utility.       |
| [`generate_all_override_spreadsheets`](#pudl.analysis.record_linkage.eia_ferc1_train.generate_all_override_spreadsheets)(→ None) | Output override spreadsheets for all specified utilities and years.            |
| [`_check_id_consistency`](#pudl.analysis.record_linkage.eia_ferc1_train._check_id_consistency)(→ None)                           | Check for rogue FERC or EIA ids that don't exist.                              |
| [`check_if_already_in_training`](#pudl.analysis.record_linkage.eia_ferc1_train.check_if_already_in_training)(training_data, ...) | Check whether any manually mapped records aren't yet in the training data.     |
| [`validate_override_fixes`](#pudl.analysis.record_linkage.eia_ferc1_train.validate_override_fixes)(→ pandas.DataFrame)           | Process the verified and/or fixed matches and look for human error.            |
| [`get_multi_match_df`](#pudl.analysis.record_linkage.eia_ferc1_train.get_multi_match_df)(→ pandas.DataFrame)                     | Process the verified and/or fixed matches and generate a list of 1:m matches.  |
| [`_add_to_training`](#pudl.analysis.record_linkage.eia_ferc1_train._add_to_training)(→ None)                                     | Add the new overrides to the old override sheet.                               |
| [`_add_to_null_overrides`](#pudl.analysis.record_linkage.eia_ferc1_train._add_to_null_overrides)(→ None)                         | Take record_id_ferc1 values verified to have no EIA match and add them to csv. |
| [`_add_to_one_to_many_overrides`](#pudl.analysis.record_linkage.eia_ferc1_train._add_to_one_to_many_overrides)(→ None)           | Add record_id_ferc1 values verified to have multiple EIA matches to csv.       |
| [`validate_and_add_to_training`](#pudl.analysis.record_linkage.eia_ferc1_train.validate_and_add_to_training)(→ None)             | Validate, combine, and add overrides to the training data.                     |

## Module Contents

### pudl.analysis.record_linkage.eia_ferc1_train.logger

### pudl.analysis.record_linkage.eia_ferc1_train.RENAME_COLS_FERC1_EIA *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)*

### pudl.analysis.record_linkage.eia_ferc1_train.RELEVANT_COLS_PPE *: [list](https://docs.python.org/3/library/stdtypes.html#list)* *= ['record_id_eia', 'report_year', 'utility_id_pudl', 'utility_id_eia', 'utility_name_eia',...*

### pudl.analysis.record_linkage.eia_ferc1_train.\_pct_diff(df, col) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Calculate percent difference between EIA and FERC versions of a column.

### pudl.analysis.record_linkage.eia_ferc1_train.\_is_best_match(df, cap_pct_diff=6, net_gen_pct_diff=6, inst_year_diff=3) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill the best_match column with strings to show cap, net_gen, inst_year match.

The process of manually checking all of the FERC-EIA matches made by the machine
learning algorithm is tedius. This function makes it easier to speed through the
obviously good matches and pay more attention to those that are more questionable.

By default, a “best match” is comprised of a FERC-EIA match with a capacity percent
difference of less than 6%, a net generation percent difference of less than 6%, and
an installation year difference of less than 3 years.

### pudl.analysis.record_linkage.eia_ferc1_train.\_prep_eia_ferc1(eia_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utils_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Prep FERC-EIA for use in override output sheet pre-utility subgroups.

* **Parameters:**
  * **eia_ferc1** – The [out_pudl_\_yearly_assn_eia_ferc1_plant_parts](../../../../../data_dictionaries/pudl_db.md#out-pudl-yearly-assn-eia-ferc1-plant-parts) table,
    associating EIA and FERC Form 1 plant records.
  * **utils_eia860** – The [out_eia_\_yearly_utilities](../../../../../data_dictionaries/pudl_db.md#out-eia-yearly-utilities) table.
* **Returns:**
  A version of the EIA-FERC1 plant association table that’s been modified for the
  purposes of creating an manual mapping spreadsheet.

### pudl.analysis.record_linkage.eia_ferc1_train.\_prep_deprish(deprish, utils_eia860) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Prep depreciation data for use in override output sheet pre-utility subgroups.

### pudl.analysis.record_linkage.eia_ferc1_train.\_get_util_year_subsets(inputs_dict, util_id_eia_list, years) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Get utility and year subsets for each of the input dfs.

After generating the dictionary with all of the inputs tables loaded, we’ll want to
create subsets of each of those tables based on the utility and year inputs we’re
given. This function takes the input dict generated in \_generate_input_dfs() and
outputs an updated version with df values pertaining to the utilities in
util_id_eia_list and years in years.

* **Parameters:**
  * **inputs_dict** ([*dict*](https://docs.python.org/3/library/stdtypes.html#dict)) – The output of running \_generation_input_dfs()
  * **util_id_eia_list** ([*list*](https://docs.python.org/3/library/stdtypes.html#list)) – A list of the utility_id_eia values you want to
    include in a single spreadsheet output. Generally this is a list of the
    subsidiaries that pertain to a single parent company.
  * **years** ([*list*](https://docs.python.org/3/library/stdtypes.html#list)) – A list of the years you’d like to add to the override sheets.
* **Returns:**
  A subset of the inputs_dict that contains versions of the value dfs that
  : pertain only to the utilities and years specified in util_id_eia_list and
    years.
* **Return type:**
  [dict](https://docs.python.org/3/library/stdtypes.html#dict)

### pudl.analysis.record_linkage.eia_ferc1_train.\_output_override_spreadsheet(util_year_subset_dict, util_name, output_dir_path) → [None](https://docs.python.org/3/library/constants.html#None)

Output spreadsheet with tabs for ferc-eia, ppe, deprish for one utility.

* **Parameters:**
  * **util_year_subset_dict** ([*dict*](https://docs.python.org/3/library/stdtypes.html#dict)) – The output from \_get_util_year_subsets()
  * **util_name** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) – A string indicating the name of the utility that you are
    creating an override sheet for. The string will be used as the suffix for
    the name of the excel file. Ex: for util_name = “BHE”, the file name will be
    BHE_fix_FERC-EIA_overrides.xlsx.
  * **output_dir_path** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) – The relative path to the folder where you’d like to
    output the override spreadsheets that this function creates.

### pudl.analysis.record_linkage.eia_ferc1_train.generate_all_override_spreadsheets(eia_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utils_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), util_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]], years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], output_dir_path: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [None](https://docs.python.org/3/library/constants.html#None)

Output override spreadsheets for all specified utilities and years.

These manual override files will be output to a folder called “overrides” in the
output directory.

* **Parameters:**
  * **eia_ferc1** – The [out_pudl_\_yearly_assn_eia_ferc1_plant_parts](../../../../../data_dictionaries/pudl_db.md#out-pudl-yearly-assn-eia-ferc1-plant-parts) table as a
    dataframe, associating EIA and FERC Form 1 plant records.
  * **ppe** – The [out_eia_\_yearly_plant_parts](../../../../../data_dictionaries/pudl_db.md#out-eia-yearly-plant-parts) table as a dataframe.
  * **utils_eia860** – The [out_eia_\_yearly_utilities](../../../../../data_dictionaries/pudl_db.md#out-eia-yearly-utilities) table as a dataframe.
  * **util_dict** – A dictionary with keys that are the names of utility
    parent companies and values that are lists of subsidiary utility_id_eia
    values. EIA values are used instead of PUDL in this case because PUDL values
    are subject to change.
  * **years** – A list of the years you’d like to add to the override sheets.
  * **output_dir_path** – The relative path to the folder where you’d like to output the
    override spreadsheets that this function creates.

### pudl.analysis.record_linkage.eia_ferc1_train.\_check_id_consistency(id_col: Literal['record_id_eia_override_1', 'record_id_ferc1'], df, actual_ids, error_message) → [None](https://docs.python.org/3/library/constants.html#None)

Check for rogue FERC or EIA ids that don’t exist.

* **Parameters:**
  * **id_col** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) – The name of either the ferc record id column: record_id_ferc1 or
    the eia record override column: record_id_eia_override_1.
  * **df** (*pd.DataFrame*) – A dataframe of intended overrides.
  * **actual_ids** ([*list*](https://docs.python.org/3/library/stdtypes.html#list)) – A list of the ferc or eia ids that are valid and come from
    either the ppe or official ferc-eia record linkage.
  * **error_message** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) – A short string to indicate the type of error you’re
    checking for. This could be looking for values that aren’t in the official
    list or values that are already in the training data.

### pudl.analysis.record_linkage.eia_ferc1_train.check_if_already_in_training(training_data, validated_connections)

Check whether any manually mapped records aren’t yet in the training data.

This function is useful for instances where you’ve started the manual mapping
process, taken an extended break, and need to check whether the data you’ve mapped
has been integrated into the training data or not.

### pudl.analysis.record_linkage.eia_ferc1_train.validate_override_fixes(validated_connections: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), eia_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), training_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), expect_override_overrides: [bool](https://docs.python.org/3/library/functions.html#bool) = False, allow_mismatched_utilities: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Process the verified and/or fixed matches and look for human error.

* **Parameters:**
  * **validated_connections** – A dataframe in the add_to_training directory that is
    ready to be added to be validated and subsumed into the training data.
  * **ppe** – The [out_eia_\_yearly_plant_parts](../../../../../data_dictionaries/pudl_db.md#out-eia-yearly-plant-parts) table as a dataframe.
  * **eia_ferc1** – The [out_pudl_\_yearly_assn_eia_ferc1_plant_parts](../../../../../data_dictionaries/pudl_db.md#out-pudl-yearly-assn-eia-ferc1-plant-parts) table as a
    dataframe, associating EIA and FERC Form 1 plant records.
  * **training_data** – The current FERC-EIA training data
  * **expect_override_overrides** – Whether you expect the tables to have
    overridden matches already in the training data.
  * **allow_mismatched_utilities** – Whether you want to allow FERC and EIA
    record ids to come from different utilities.
* **Raises:**
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If there are EIA override id records that aren’t in the original
    FERC-EIA connection.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If there are FERC record ids that aren’t in the original
    FERC-EIA connection.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If there are EIA override ids that are duplicated throughout the
    override document.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the utility id in the EIA override id doesn’t match the pudl
    id corresponding with the FERC record.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If there are EIA override id records that don’t correspond to
    the correct report year.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If you didn’t expect to override overrides but the new training
    data implies an override to the existing training data.
* **Returns:**
  The validated FERC-EIA dataframe you’re trying to add to the training data.

### pudl.analysis.record_linkage.eia_ferc1_train.get_multi_match_df(training_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Process the verified and/or fixed matches and generate a list of 1:m matches.

Filter the dataframe to only include FERC records with more than one EIA match.
Melt this dataframe to report all matched EIA records in the
`record_id_eia_override_1` column.

* **Parameters:**
  **training_data** – A dataframe in the add_to_training directory that is ready to be
  validated and subsumed into the training data.
* **Returns:**
  A dataframe of one_to_many matches formatted to fit into the existing validation
  framework.

### pudl.analysis.record_linkage.eia_ferc1_train.\_add_to_training(new_overrides, path_to_current_training) → [None](https://docs.python.org/3/library/constants.html#None)

Add the new overrides to the old override sheet.

### pudl.analysis.record_linkage.eia_ferc1_train.\_add_to_null_overrides(null_matches, current_null_overrides_path) → [None](https://docs.python.org/3/library/constants.html#None)

Take record_id_ferc1 values verified to have no EIA match and add them to csv.

### pudl.analysis.record_linkage.eia_ferc1_train.\_add_to_one_to_many_overrides(one_to_many, current_one_to_many_path) → [None](https://docs.python.org/3/library/constants.html#None)

Add record_id_ferc1 values verified to have multiple EIA matches to csv.

### pudl.analysis.record_linkage.eia_ferc1_train.validate_and_add_to_training(ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), eia_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), input_dir_path: [str](https://docs.python.org/3/library/stdtypes.html#str), expect_override_overrides: [bool](https://docs.python.org/3/library/functions.html#bool) = False, allow_mismatched_utilities: [bool](https://docs.python.org/3/library/functions.html#bool) = True, one_to_many: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [None](https://docs.python.org/3/library/constants.html#None)

Validate, combine, and add overrides to the training data.

Validating and combining the records so you only have to loop through the files
once. Runs [`validate_override_fixes()`](#pudl.analysis.record_linkage.eia_ferc1_train.validate_override_fixes) and [`_add_to_training()`](#pudl.analysis.record_linkage.eia_ferc1_train._add_to_training).

* **Parameters:**
  * **input_dir_path** – The path to the place where the matched files that you want to
    validate or integrate are.
  * **expect_override_overrides** – This value is explicitly assigned at the top of the
    notebook.
  * **allow_mismatched_utilities** – Whether you are allowed to have FERC-EIA matches
    from different utilities.
  * **one_to_many** – If True, will also validate and save a CSV of one_to_many matches.
