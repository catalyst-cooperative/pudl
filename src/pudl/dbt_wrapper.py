"""Wrap DBT invocations so we can get custom behavior."""

from contextlib import chdir
from typing import NamedTuple, cast

import duckdb
import pandas as pd
from dbt.artifacts.schemas.results import TestStatus
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.nodes import GenericTestNode

from pudl.workspace.setup import PUDL_ROOT_PATH, PudlPaths


def get_failed_nodes(results: RunExecutionResult) -> list[GenericTestNode]:
    """Get test node output from tests that failed."""
    return [res.node for res in results if res.status == TestStatus.Fail]


class NodeContext(NamedTuple):
    """Associate a node's *name* with information describing what went wrong."""

    name: str
    context: pd.DataFrame

    def pretty_print(self):
        """Nice output for logging to stdout."""
        return f"{self.name}:\n\n{self.context}"


def get_context_for_nodes(nodes: list[GenericTestNode]) -> list[NodeContext]:
    """Post-process node output to figure out failure context.

    For most tests, just run the compiled code in the node.

    (TODO 2025-06-09) for weighted quantile tests, run the "debug quantiles" DBT
    operation.
    """
    contexts = []
    duckdb_path = PudlPaths().output_file("pudl_dbt_tests.duckdb")
    with duckdb.connect(duckdb_path) as con:
        for node in nodes:
            con.execute(node.compiled_code)
            node_df = con.fetchdf()
            contexts.append(NodeContext(name=node.name, context=node_df))
    return contexts


class BuildResult(NamedTuple):
    """Combine overall result with any useful failure context."""

    success: bool
    failure_contexts: list[NodeContext]

    def format_failure_contexts(self) -> str:
        """Nice legible output for logs."""
        return "\n=====\n".join(ctx.pretty_print() for ctx in self.failure_contexts)


def build_with_context(
    node_selection: str,
    dbt_target: str,
    node_exclusion: str | None = None,
) -> BuildResult:
    """Run the DBT build and get failure information back.

    * run the DBT build using our selection, returning test failures
    * get extra context for each test failure
    * print out test failure context
    """
    cli_args = ["--target", dbt_target, "--select", node_selection]
    if node_exclusion is not None:
        cli_args += ["--exclude", node_exclusion]
    dbt = dbtRunner()
    dbt_dir = PUDL_ROOT_PATH / "dbt"

    # 2025-06-06 TODO: use the --project-dir flag instead of chdiring all the time?
    with chdir(dbt_dir):
        _ = dbt.invoke(["deps"])
        _ = dbt.invoke(["seed"])
        build_output: dbtRunnerResult = dbt.invoke(["build"] + cli_args)
        build_results = cast(RunExecutionResult, build_output.result)

    failed_nodes = get_failed_nodes(build_results)
    failure_contexts = get_context_for_nodes(failed_nodes)

    return BuildResult(success=build_output.success, failure_contexts=failure_contexts)
