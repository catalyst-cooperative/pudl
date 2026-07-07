# pudl.analysis.ml_tools.models

Provides tooling for developing/tracking ML models within PUDL.

The ML pipelines here use Dagster’s `@op` and `@graph` primitives rather than
`@asset`. Each pipeline (e.g. `ferc_to_ferc`, `ferc_to_eia`) is a multi-step
computation — embedding, clustering, matching — where the intermediate outputs (distance
matrices, cluster assignments, etc.) are not meaningful PUDL data products. They are
implementation details of the model.  Converting each `@op` to an `@asset` would
pollute the asset catalog with tables that have no meaning outside the model.

`graph_asset` is the Dagster idiom for exactly this use case: a complex computation
with internal steps that nevertheless produces a single named asset visible in the
catalog. Do not refactor these to chains of `@asset`.

## The `@pudl_model` decorator

[`pudl_model()`](#pudl.analysis.ml_tools.models.pudl_model) is a decorator factory that wraps a Dagster `@graph` and
converts it into a `graph_asset`. Applying it to a `@graph` function does
three things:

1. **Collects configuration.** It walks the graph’s op tree, harvesting default
   config values from each op’s [`Config`](https://docs.dagster.io/api/dagster/config/#dagster.Config) subclass. If
   `config_from_yaml=True`, it also merges overrides from
   `pudl.package_data.settings.pudl_models.yml`. The merged config is stored
   in the module-level `MODEL_CONFIGURATION` dict, which
   `get_ml_models_config()` later folds into the
   default job config so Dagster knows the defaults at launch time.
2. **Injects an ExperimentTracker.** An [`ExperimentTracker`](../experiment_tracking/index.md#pudl.analysis.ml_tools.experiment_tracking.ExperimentTracker)
   op is synthesized and called first inside the `graph_asset`, then passed as
   the first argument to the wrapped graph. Ops that want to log metrics receive
   it as an input parameter named `experiment_tracker`. The tracker input is
   excluded from the asset’s `ins` mapping so Dagster does not treat it as a
   dependency on an upstream asset.
3. **Returns a graph_asset.** The decorated function is replaced by a
   `graph_asset` whose name is `asset_name` and whose upstream asset
   dependencies are inferred from the graph’s remaining inputs.

Configuration precedence (lowest → highest):

* Default values on each op’s `Config` subclass (code)
* Entries in `pudl_models.yml` (repo-level YAML, only when `config_from_yaml=True`)
* Values entered in the Dagster UI Launchpad (single-run override)

## Attributes

| [`logger`](#pudl.analysis.ml_tools.models.logger)                           |    |
|-----------------------------------------------------------------------------|----|
| [`MODEL_CONFIGURATION`](#pudl.analysis.ml_tools.models.MODEL_CONFIGURATION) |    |

## Functions

| [`get_yml_config`](#pudl.analysis.ml_tools.models.get_yml_config)(→ dict)             | Load model configuration from yaml file.                                       |
|---------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`get_default_config`](#pudl.analysis.ml_tools.models.get_default_config)(→ dict)     | Get default config values for model.                                           |
| [`pudl_model`](#pudl.analysis.ml_tools.models.pudl_model)(→ dagster.AssetsDefinition) | Decorator for an ML model that will handle providing configuration to dagster. |

## Module Contents

### pudl.analysis.ml_tools.models.logger

### pudl.analysis.ml_tools.models.MODEL_CONFIGURATION

### pudl.analysis.ml_tools.models.get_yml_config(experiment_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Load model configuration from yaml file.

### pudl.analysis.ml_tools.models.get_default_config(model_graph: [dagster.GraphDefinition](https://docs.dagster.io/api/dagster/graphs/#dagster.GraphDefinition)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Get default config values for model.

### pudl.analysis.ml_tools.models.pudl_model(asset_name: [str](https://docs.python.org/3/library/stdtypes.html#str), config_from_yaml: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Decorator for an ML model that will handle providing configuration to dagster.
