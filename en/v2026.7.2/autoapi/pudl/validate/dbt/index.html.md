# pudl.validate.dbt

Wrap DBT invocations so we can get custom behavior.

## Attributes

| [`logger`](#pudl.validate.dbt.logger)   |    |
|-----------------------------------------|----|

## Classes

| [`NodeContext`](#pudl.validate.dbt.NodeContext)   | Associate a node's *name* with information describing what went wrong.   |
|---------------------------------------------------|--------------------------------------------------------------------------|
| [`BuildResult`](#pudl.validate.dbt.BuildResult)   | Combine overall result with any useful failure context.                  |

## Functions

| [`_preserve_logging_propagation`](#pudl.validate.dbt._preserve_logging_propagation)()                | Restore logging propagation settings after a dbt invocation.            |
|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`install_dbt_deps`](#pudl.validate.dbt.install_dbt_deps)(→ dbt.cli.main.dbtRunner)                  | Ensure dbt package dependencies are installed in the project directory. |
| [`__get_failed_nodes`](#pudl.validate.dbt.__get_failed_nodes)(...)                                   | Get test node output from tests that failed.                            |
| [`__get_quantile_contexts`](#pudl.validate.dbt.__get_quantile_contexts)(→ list[NodeContext])         | Run debug_quantile_constraints macro for failed quantile constraints.   |
| [`__get_compiled_sql_contexts`](#pudl.validate.dbt.__get_compiled_sql_contexts)(→ list[NodeContext]) | Run the compiled SQL against duckdb to get failure contexts.            |
| [`build_with_context`](#pudl.validate.dbt.build_with_context)(→ BuildResult)                         | Run the DBT build and get failure information back.                     |
| [`dagster_to_dbt_selection`](#pudl.validate.dbt.dagster_to_dbt_selection)(→ str)                     | Translate dagster asset selection to db node selection.                 |

## Module Contents

### pudl.validate.dbt.logger

### pudl.validate.dbt.\_preserve_logging_propagation()

Restore logging propagation settings after a dbt invocation.

Invoking dbt via dbtRunner triggers Dagster’s logging initialization, which
resets `logging.getLogger("dagster").propagate` to `False`. This context
manager saves and restores the setting so callers don’t experience unexpected
side effects on the global logging configuration.

### *class* pudl.validate.dbt.NodeContext

Bases: `NamedTuple`

Associate a node’s *name* with information describing what went wrong.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### context *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### pretty_print()

Nice output for logging to stdout.

### *class* pudl.validate.dbt.BuildResult

Bases: `NamedTuple`

Combine overall result with any useful failure context.

#### success *: [bool](https://docs.python.org/3/library/functions.html#bool)*

#### failure_contexts *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[NodeContext](#pudl.validate.dbt.NodeContext)]*

#### format_failure_contexts() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Nice legible output for logs.

### pudl.validate.dbt.install_dbt_deps(dbt: dbt.cli.main.dbtRunner | [None](https://docs.python.org/3/library/constants.html#None) = None) → dbt.cli.main.dbtRunner

Ensure dbt package dependencies are installed in the project directory.

### pudl.validate.dbt.\_\_get_failed_nodes(results: dbt.artifacts.schemas.run.RunExecutionResult) → [list](https://docs.python.org/3/library/stdtypes.html#list)[dbt.contracts.graph.nodes.GenericTestNode]

Get test node output from tests that failed.

### pudl.validate.dbt.\_\_get_quantile_contexts(nodes: [list](https://docs.python.org/3/library/stdtypes.html#list)[dbt.contracts.graph.nodes.GenericTestNode], dbt: dbt.cli.main.dbtRunner, dbt_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[NodeContext](#pudl.validate.dbt.NodeContext)]

Run debug_quantile_constraints macro for failed quantile constraints.

This is a little tricky because the macro output is just logged to
stdout, and not stored in the dbt.invoke result. So, for each node, we:

* redirect stdout
* run the macro based on node information
* parse stdout to get the context

Also, if a node has multiple parents, we don’t know which table to pass into
`debug_quantile_constraints` so we just skip it.

### pudl.validate.dbt.\_\_get_compiled_sql_contexts(nodes: [list](https://docs.python.org/3/library/stdtypes.html#list)[dbt.contracts.graph.nodes.GenericTestNode]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[NodeContext](#pudl.validate.dbt.NodeContext)]

Run the compiled SQL against duckdb to get failure contexts.

### pudl.validate.dbt.build_with_context(node_selection: [str](https://docs.python.org/3/library/stdtypes.html#str), dbt_target: [str](https://docs.python.org/3/library/stdtypes.html#str), node_exclusion: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [BuildResult](#pudl.validate.dbt.BuildResult)

Run the DBT build and get failure information back.

* run the DBT build using our selection, returning test failures
* split the test failures by type - for most, we will just run the compiled
  SQL, but other tests such as the weighted quantile tests need extra
  handling
* get contexts for various test failure types
* print out test failure context

### pudl.validate.dbt.dagster_to_dbt_selection(selection: [str](https://docs.python.org/3/library/stdtypes.html#str), defs: [dagster.Definitions](https://docs.dagster.io/api/dagster/definitions/#dagster.Definitions), manifest=None) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Translate dagster asset selection to db node selection.

We use the dbt manifest to determine which sources are defined in dbt so
that we can map them to dagster assets. So, we need to generate a fresh dbt
manifest via `dbt parse` whenever we run this function.

* turn asset selection into asset keys
* turn asset keys into node names
* turn node names into selection string
