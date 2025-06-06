"""Wrap DBT invocations so we can get custom behavior."""

from contextlib import chdir
from typing import cast, NamedTuple

import pandas as pd

from dbt.artifacts.schemas.results import TestStatus
from dbt.artifacts.schemas.run import RunExecutionResult, RunResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.nodes import GenericTestNode
from pudl.workspace.setup import PUDL_ROOT_PATH, PudlPaths


def get_failed_nodes(results: RunExecutionResult) -> list[GenericTestNode]:
    # TODO 2025-06-06 handle errors too?
    return [res.node for res in results if res.status == TestStatus.Fail]


class NodeContext(NamedTuple):
    name: str
    context: pd.DataFrame

    def pretty_print(self):
        return f"{self.name}:\n\n{self.context}\n\n====\n"


def get_context_for_nodes(nodes: list[GenericTestNode]) -> list[NodeContext]:
    contexts = [NodeContext(name=node.name, context=pd.DataFrame()) for node in nodes]
    return contexts


def run_build(model_selection):
    """Run the DBT build and get failure information back.

    * run the DBT build using our selection, returning test failures
    * get extra context for each test failure
    * print out test failure context
    """
    dbt_target = "etl-full"
    cli_args = ["--target", dbt_target, "--select", model_selection]
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

    print("\n".join(c.pretty_print() for c in failure_contexts))
