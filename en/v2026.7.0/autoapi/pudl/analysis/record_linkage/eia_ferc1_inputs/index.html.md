# pudl.analysis.record_linkage.eia_ferc1_inputs

Prepare the inputs to the FERC1 to EIA record linkage model.

## Attributes

| [`logger`](#pudl.analysis.record_linkage.eia_ferc1_inputs.logger)   |    |
|---------------------------------------------------------------------|----|

## Classes

| [`InputManager`](#pudl.analysis.record_linkage.eia_ferc1_inputs.InputManager)   | Class to prepare inputs for linking FERC1 and EIA.   |
|---------------------------------------------------------------------------------|------------------------------------------------------|

## Functions

| [`restrict_train_connections_on_date_range`](#pudl.analysis.record_linkage.eia_ferc1_inputs.restrict_train_connections_on_date_range)(...)   | Restrict the training data based on the date ranges of the input tables.   |
|----------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [`prep_train_connections`](#pudl.analysis.record_linkage.eia_ferc1_inputs.prep_train_connections)(→ pandas.DataFrame)                        | Get and prepare the training connections for the model.                    |

## Module Contents

### pudl.analysis.record_linkage.eia_ferc1_inputs.logger

### *class* pudl.analysis.record_linkage.eia_ferc1_inputs.InputManager(plants_all_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), fbp_ferc1: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plant_parts_eia: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Class to prepare inputs for linking FERC1 and EIA.

#### plant_parts_eia

#### plants_all_ferc1

#### fbp_ferc1

#### start_date

#### end_date

#### plant_parts_eia_true *= None*

#### plants_ferc1 *= None*

#### train_df *= None*

#### train_ferc1 *= None*

#### train_eia *= None*

#### get_plant_parts_eia_true(clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the EIA plant-parts with only the unique granularities.

#### get_plants_ferc1(clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Prepare FERC1 plants data for record linkage with EIA plant-parts.

This method merges two internally cached dataframes (`self.plants_all_ferc1`
and `self.fuel_by_plant_ferc1` (originally obtained from
[out_ferc1_\_yearly_all_plants](../../../../../data_dictionaries/pudl_db.md#out-ferc1-yearly-all-plants) and
[out_ferc1_\_yearly_steam_plants_fuel_by_plant_sched402](../../../../../data_dictionaries/pudl_db.md#out-ferc1-yearly-steam-plants-fuel-by-plant-sched402)) respectively) and
ensures that key columns are have the same names and dtypes as the analogous
EIA columns so that they can be used in the FERC-EIA record linkage model
easily.

* **Returns:**
  A cleaned table of FERC1 plants plant records with fuel cost data.

#### get_train_df() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the training connections.

Prepare them if the training data hasn’t been connected to FERC data yet.

#### get_train_records(dataset_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), dataset_id_col: Literal['record_id_eia', 'record_id_ferc1']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generate a set of known connections from a dataset using training data.

This method grabs only the records from the the datasets (EIA or FERC)
that we have in our training data.

* **Parameters:**
  * **dataset_df** – either FERC1 plants table (result of [`get_plants_ferc1()`](#pudl.analysis.record_linkage.eia_ferc1_inputs.InputManager.get_plants_ferc1)) or
    EIA plant-parts (result of [`get_plant_parts_eia_true()`](#pudl.analysis.record_linkage.eia_ferc1_inputs.InputManager.get_plant_parts_eia_true)).
  * **dataset_id_col** – Identifying column name. Either `record_id_eia` for
    `plant_parts_eia_true` or `record_id_ferc1` for `plants_ferc1`.

#### get_train_eia(clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the known training data from EIA.

#### get_train_ferc1(clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the known training data from FERC1.

#### execute(clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

Compile all the inputs.

This method is only run if/when you want to ensure all of the inputs are
generated all at once. While using [`InputManager`](#pudl.analysis.record_linkage.eia_ferc1_inputs.InputManager), it is preferred to
access each input dataframe or index via their `get_` method instead of
accessing the attribute.

### pudl.analysis.record_linkage.eia_ferc1_inputs.restrict_train_connections_on_date_range(train_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), id_col: Literal['record_id_eia', 'record_id_ferc1'], start_date: [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp), end_date: [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Restrict the training data based on the date ranges of the input tables.

The training data for this model spans the full PUDL date range. We don’t want to
add training data from dates that are outside of the range of the FERC and EIA data
we are attempting to match. So this function restricts the training data based on
start and end dates.

The training data is only the record IDs, which contain the report year inside them.
This function compiles a regex using the date range to grab only training records
which contain the years in the date range followed by and preceded by `_` - in
the format of `record_id_eia``and ``record_id_ferc1`. We use that extracted year
to determine

### pudl.analysis.record_linkage.eia_ferc1_inputs.prep_train_connections(ppe: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), start_date: [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp), end_date: [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get and prepare the training connections for the model.

We have stored training data, which consists of records with ids
columns for both FERC and EIA. Those id columns serve as a connection
between ferc1 plants and the EIA plant-parts. These connections
indicate that a ferc1 plant records is reported at the same granularity
as the connected EIA plant-parts record.

* **Parameters:**
  * **ppe** – The EIA plant parts. Records from this dataframe will be connected to the
    training data records. This needs to be the full EIA plant parts, not just
    the distinct/true granularities because the training data could contain
    non-distinct records and this function reassigns those to their distinct
    counterparts.
  * **start_date** – Beginning date for records from the training data. Should match the
    start date of `ppe`. Default is None and all the training data will be used.
  * **end_date** – Ending date for records from the training data. Should match the end
    date of `ppe`. Default is None and all the training data will be used.
* **Returns:**
  A dataframe of training connections which has a MultiIndex of `record_id_eia`
  and `record_id_ferc1`.
