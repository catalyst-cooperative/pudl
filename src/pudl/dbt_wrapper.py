"""Wrap DBT invocations so we can get custom behavior."""

import io
import json
from contextlib import chdir, redirect_stdout
from pathlib import Path
from typing import NamedTuple, cast

import dagster as dg
import duckdb
from dbt.artifacts.schemas.results import TestStatus
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.nodes import GenericTestNode

import pudl.etl
from pudl.logging_helpers import get_logger
from pudl.workspace.setup import PUDL_ROOT_PATH, PudlPaths

logger = get_logger(__name__)

DBT_DIR: Path = PUDL_ROOT_PATH / "dbt"


class NodeContext(NamedTuple):
    """Associate a node's *name* with information describing what went wrong."""

    name: str
    context: str

    def pretty_print(self):
        """Nice output for logging to stdout."""
        return f"{self.name}:\n\n{self.context}"


class BuildResult(NamedTuple):
    """Combine overall result with any useful failure context."""

    success: bool
    failure_contexts: list[NodeContext]

    def format_failure_contexts(self) -> str:
        """Nice legible output for logs."""
        return "\n=====\n".join(ctx.pretty_print() for ctx in self.failure_contexts)


def __get_failed_nodes(results: RunExecutionResult) -> list[GenericTestNode]:
    """Get test node output from tests that failed."""
    return [res.node for res in results if res.status == TestStatus.Fail]


def __get_quantile_contexts(
    nodes: list[GenericTestNode], dbt: dbtRunner, dbt_dir: Path
) -> list[NodeContext]:
    """Run debug_quantile_constraints macro for failed quantile constraints.

    This is a little tricky because the macro output is just logged to
    stdout, and not stored in the dbt.invoke result. So, for each node, we:

    * redirect stdout
    * run the macro based on node information
    * parse stdout to get the context

    Also, if a node has multiple parents, we don't know which table to pass into
    ``debug_quantile_constraints`` so we just skip it.
    """
    contexts = []
    for node in nodes:
        parents = node.depends_on.nodes
        if len(parents) != 1:
            logger.warning(
                f"Found {len(parents)} parents for {node.name}, expected 1. Skipping"
            )
            continue

        table_name = parents[0].rsplit(".")[-1]
        cmd = [
            "run-operation",
            "debug_quantile_constraints",
            "--target",
            "etl-full",
            "--no-use-colors",
            "--args",
            json.dumps({"table": table_name, "test": node.name}),
        ]
        buffer = io.StringIO()
        with chdir(dbt_dir), redirect_stdout(buffer):
            dbt.invoke(cmd)

        context_lines = buffer.getvalue().split("\n")
        no_header = context_lines[3:]
        no_timestamp = [line.split(" ", 1)[-1] for line in no_header]
        contexts.append(NodeContext(name=node.name, context="\n".join(no_timestamp)))
    return contexts


def __get_compiled_sql_contexts(nodes: list[GenericTestNode]) -> list[NodeContext]:
    """Run the compiled SQL against duckdb to get failure contexts."""
    contexts = []
    duckdb_path = PudlPaths().output_file("pudl_dbt_tests.duckdb")
    with duckdb.connect(duckdb_path) as con:
        for node in nodes:
            con.execute(node.compiled_code)
            node_df = con.fetchdf()
            node_str = node_df.head(20).to_markdown(maxcolwidths=40, index=False)
            if node_str is None:
                logger.warning(f"Couldn't format data for node {node.name}.")
                continue
            if node_df.shape[0] > 20:
                node_str += f"\n(of {node_df.shape[0]})"
            contexts.append(NodeContext(name=node.name, context=node_str))
    return contexts


def build_with_context(
    node_selection: str,
    dbt_target: str,
    node_exclusion: str | None = None,
) -> BuildResult:
    """Run the DBT build and get failure information back.

    * run the DBT build using our selection, returning test failures
    * split the test failures by type - for most, we will just run the compiled
      SQL, but other tests such as the weighted quantile tests need extra
      handling
    * get contexts for various test failure types
    * print out test failure context
    """
    cli_args = ["--target", dbt_target, "--select", node_selection]
    if node_exclusion is not None:
        cli_args += ["--exclude", node_exclusion]
    dbt = dbtRunner()

    with chdir(DBT_DIR):
        dbt.invoke(["deps"])
        dbt.invoke(["seed"])
        build_output: dbtRunnerResult = dbt.invoke(["build"] + cli_args)
        build_results = cast(RunExecutionResult, build_output.result)

    failed_nodes = __get_failed_nodes(build_results)

    weighted_quantile_failures, compiled_sql_failures = [], []
    for node in failed_nodes:
        if "expect_quantile_constraints_" in node.name:
            weighted_quantile_failures.append(node)
        else:
            compiled_sql_failures.append(node)

    weighted_quantile_contexts = __get_quantile_contexts(
        weighted_quantile_failures, dbt=dbt, dbt_dir=DBT_DIR
    )
    compiled_sql_contexts = __get_compiled_sql_contexts(compiled_sql_failures)

    return BuildResult(
        success=build_output.success,
        failure_contexts=compiled_sql_contexts + weighted_quantile_contexts,
    )


def dagster_to_dbt_selection(
    selection: str, defs: dg.Definitions | None = None, manifest=None
) -> str:
    """Translate dagster asset selection to db node selection.

    We use the dbt manifest to determine which sources are defined in dbt so
    that we can map them to dagster assets. So, we need to generate a fresh dbt
    manifest via ``dbt parse`` whenever we run this function.

    * turn asset selection into asset keys
    * turn asset keys into node names
    * turn node names into selection string
    """
    if defs is None:
        defs = pudl.etl.defs

    asset_keys = dg.AssetSelection.from_string(selection).resolve(
        defs.resolve_asset_graph()
    )
    asset_names = {asset_key.to_user_string() for asset_key in asset_keys}

    if manifest is None:
        manifest_path = PUDL_ROOT_PATH / "dbt" / "target" / "manifest.json"
        with chdir(PUDL_ROOT_PATH / "dbt"):
            dbt = dbtRunner()
            dbt.invoke(["parse"])

        with manifest_path.open("r") as f:
            manifest = json.load(f)

    # all dagster assets are treated as sources so we only have to look here.
    dbt_node_selectors = [
        f"source:{s['source_name']}.{s['name']}"
        for s in manifest["sources"].values()
        if s["name"] in asset_names
    ]
    return " ".join(dbt_node_selectors)
