# pudl.analysis.ml_tools.experiment_tracking

This module implements experiment tracking tooling using mlflow as a backend.

[`ExperimentTracker`](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)’s are created using an op factory [`experiment_tracker_factory()`](#pudl.analysis.ml_tools.experiment_tracking.experiment_tracker_factory)
and can be passed around to op’s which make up a PUDL model. This class will maintain
state between ops, ensuring that all parameters and metrics are logged to the appropriate
mlflow run. The following command will launch the mlflow UI to view model results:
mlflow ui –backend-store-uri {tracking_uri}. tracking_uri by default will point
to a file named ‘experiments.sqlite’ in the base directory of your PUDL repo, but
this is a configurable value, which can be found in the dagster UI.

## Attributes

| [`logger`](#pudl.analysis.ml_tools.experiment_tracking.logger)   |    |
|------------------------------------------------------------------|----|

## Classes

| [`ExperimentTrackerConfig`](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTrackerConfig)   | Dagster config to setup experiment tracking with mlflow.        |
|----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`ExperimentTracker`](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)               | Class to manage tracking a machine learning model using MLflow. |

## Functions

| [`_flatten_model_config`](#pudl.analysis.ml_tools.experiment_tracking._flatten_model_config)(→ dict)                        | Take nested dictionary defining model config and flatten for logging purposes.   |
|-----------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`experiment_tracker_factory`](#pudl.analysis.ml_tools.experiment_tracking.experiment_tracker_factory)(→ ExperimentTracker) | Use config to create an experiment tracker.                                      |

## Module Contents

### pudl.analysis.ml_tools.experiment_tracking.logger

### pudl.analysis.ml_tools.experiment_tracking.\_flatten_model_config(model_config: [dict](https://docs.python.org/3/library/stdtypes.html#dict)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Take nested dictionary defining model config and flatten for logging purposes.

This is essentially a translation layer between Dagster configuration and mlflow,
which does not support displaying nested parameters in the UI.

### Examples

```pycon
>>> _flatten_model_config(
...     {
...         'ferc_to_ferc': {
...             'link_ids_cross_year': {
...                 'compute_distance_matrix': {
...                     'distance_threshold': .5,
...                      'metric': 'euclidean',
...                 },
...                 'match_orphaned_records': {'distance_threshold': 0.5},
...             }
...         }
...     }
... ) == {
...     'ferc_to_ferc.link_ids_cross_year.compute_distance_matrix.distance_threshold': 0.5,
...     'ferc_to_ferc.link_ids_cross_year.compute_distance_matrix.metric': 'euclidean',
...     'ferc_to_ferc.link_ids_cross_year.match_orphaned_records.distance_threshold': 0.5
... }
True
```

### *class* pudl.analysis.ml_tools.experiment_tracking.ExperimentTrackerConfig(\*\*config_dict)

Bases: [`dagster.Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config)

Dagster config to setup experiment tracking with mlflow.

#### tracking_uri *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'sqlite:///Instance of pathlib._local.Path/experiments.sqlite'*

#### tracking_enabled *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

#### run_context *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'production'*

#### artifact_location *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= ''*

### *class* pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Class to manage tracking a machine learning model using MLflow.

The following command will launch the mlflow UI to view model results:
mlflow ui –backend-store-uri {tracking_uri}. From here, you can compare metrics
from multiple runs, and track performance.

This class is designed to be created using the op [`create_experiment_tracker()`](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker.create_experiment_tracker).
This allows the ExperimentTracker to be passed around within a Dagster graph,
and be used for mlflow logging in any of the op’s that make up the graph. This
is useful because Dagster executes op’s in separate processes, while mlflow does
not maintain state between processes. This design also allows configuration of
the ExperimentTracker to be set from the Dagster UI.

Currently, we are only doing experiment tracking in a local context, but if we were
to setup a tracking server, we could point the tracking_uri at this remote server
without having to modify the models. Experiment tracking can also be done outside
of the PUDL context. If doing exploratory work in a notebook, you can use mlflow
directly in a notebook with the same experiment name used here, and mlflow will
seamlessly integrate the results with those from PUDL runs.

#### tracker_config *: [ExperimentTrackerConfig](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTrackerConfig)*

#### run_id *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### experiment_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### *classmethod* create_experiment_tracker(experiment_config: [ExperimentTrackerConfig](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTrackerConfig), experiment_name: [str](https://docs.python.org/3/library/stdtypes.html#str), model_config: [dict](https://docs.python.org/3/library/stdtypes.html#dict)) → [ExperimentTracker](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)

Create experiment tracker for specified experiment.

#### execute_logging(logging_func: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable))

Perform MLflow logging statement inside ExperimentTracker run.

* **Parameters:**
  **logging_func** – Callable which should perform an mlflow logging statement
  inside context manager for run. Passing in callable allows
  ExperimentTracker to only execute logging if tracking is enabled
  in configuration.

#### *static* get_or_create_experiment(experiment_name: [str](https://docs.python.org/3/library/stdtypes.html#str), artifact_location: [str](https://docs.python.org/3/library/stdtypes.html#str) = '') → [str](https://docs.python.org/3/library/stdtypes.html#str)

Retrieve the ID of an existing MLflow experiment or create a new one if it doesn’t exist.

This function checks if an experiment with the given name exists within MLflow.
If it does, the function returns its ID. If not, it creates a new experiment
with the provided name and returns its ID.

* **Returns:**
  ID of the existing or newly created MLflow experiment.

### pudl.analysis.ml_tools.experiment_tracking.experiment_tracker_factory(experiment_name: [str](https://docs.python.org/3/library/stdtypes.html#str), model_config: [dict](https://docs.python.org/3/library/stdtypes.html#dict)) → [ExperimentTracker](#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)

Use config to create an experiment tracker.
