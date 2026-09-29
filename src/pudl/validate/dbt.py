"""Wrap DBT invocations so we can get custom behavior."""

import io
import json
import logging
import os
from contextlib import chdir, contextmanager, redirect_stdout
from pathlib import Path
from typing import NamedTuple, cast

import dagster as dg
import duckdb
from dbt.artifacts.schemas.results import TestStatus
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.nodes import GenericTestNode

from pudl import PUDL_DBT_PATH
from pudl.logging_helpers import get_logger
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

# Where to find the nightly build outputs for each dbt source. The base path of each
# source is the ``<source>_parquet_base_path`` var in ``dbt_project.yml``.
NIGHTLY_PARQUET_BASE_PATHS = {
    "pudl": "s3://pudl.catalyst.coop/nightly",
    "ferceqr": "s3://pudl.catalyst.coop/ferceqr",
}

# Tables with ``large: true`` in their meta config in ``dbt_project.yml`` are much
# larger than the others, so we run them in a separate dbt invocation against a target
# with its own DuckDB resource limits. This maps a target to the one that runs its
# large tables. (We can't use a tag, because dbt ignores ``+tags`` for sources in
# ``dbt_project.yml``.)
LARGE_TABLE_SELECTOR = "config.meta.large:true"
LARGE_TABLE_TARGETS = {"etl-full": "etl-full-large"}


@contextmanager
def _preserve_logging_propagation():
    """Restore logging propagation settings after a dbt invocation.

    Invoking dbt via dbtRunner triggers Dagster's logging initialization, which
    resets ``logging.getLogger("dagster").propagate`` to ``False``. This context
    manager saves and restores the setting so callers don't experience unexpected
    side effects on the global logging configuration.
    """
    dagster_logger = logging.getLogger("dagster")
    original_propagate = dagster_logger.propagate
    try:
        yield
    finally:
        dagster_logger.propagate = original_propagate


class DbtPass(NamedTuple):
    """The arguments that define a single ``dbt build`` invocation."""

    target: str
    select: str
    exclude: str | None


def split_by_table_size(
    node_selection: str, node_exclusion: str | None, dbt_target: str
) -> list[DbtPass]:
    """Split a dbt selection into the passes needed to give big tables their own target.

    Tables marked ``large`` are built with the target in ``LARGE_TABLE_TARGETS``, and
    everything else with ``dbt_target``. A target without an entry in
    ``LARGE_TABLE_TARGETS`` gets a single pass with all of the tables.
    """
    large_target = LARGE_TABLE_TARGETS.get(dbt_target)
    if large_target is None:
        return [DbtPass(dbt_target, node_selection, node_exclusion)]

    exclude_large = (
        f"{node_exclusion} {LARGE_TABLE_SELECTOR}"
        if node_exclusion
        else LARGE_TABLE_SELECTOR
    )
    # dbt can only intersect single selectors, so intersect each of the space-separated
    # (unioned) ones with the large tables.
    select_large = " ".join(
        f"{atom},{LARGE_TABLE_SELECTOR}" for atom in node_selection.split()
    )
    return [
        DbtPass(dbt_target, node_selection, exclude_large),
        DbtPass(large_target, select_large, node_exclusion),
    ]


def duckdb_settings(dbt_target: str) -> dict[str, str]:
    """Get the DuckDB settings that dbt applies for a target.

    This mirrors the ``configure_duckdb`` macro, which reads the same environment
    variables. We need it to run failing test queries outside of dbt with the same
    resource limits as the test itself, since tests on huge tables might otherwise
    run out of memory a second time while we're trying to explain how they failed.
    """
    prefixes = ["PUDL_DBT_"]
    if dbt_target in LARGE_TABLE_TARGETS.values():
        prefixes.insert(0, "PUDL_DBT_LARGE_")

    settings = {"preserve_insertion_order": "false"}
    for setting, suffix in {
        "memory_limit": "MEMORY_LIMIT",
        "threads": "THREADS",
        "temp_directory": "TEMP_DIR",
    }.items():
        values = (os.environ.get(prefix + suffix) for prefix in prefixes)
        if value := next((v for v in values if v), None):
            settings[setting] = value
    return settings


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


def install_dbt_deps(dbt: dbtRunner | None = None) -> dbtRunner:
    """Ensure dbt package dependencies are installed in the project directory."""
    if dbt is None:
        dbt = dbtRunner()

    with chdir(PUDL_DBT_PATH):
        dbt.invoke(["deps"])

    return dbt


def __get_failed_nodes(results: RunExecutionResult) -> list[GenericTestNode]:
    """Get test node output from tests that failed."""
    return [res.node for res in results if res.status == TestStatus.Fail]


def __get_quantile_contexts(
    nodes: list[GenericTestNode],
    dbt: dbtRunner,
    dbt_dir: Path,
    dbt_target: str,
    vars_args: list[str],
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
            dbt_target,
            *vars_args,
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


def __get_compiled_sql_contexts(
    nodes: list[GenericTestNode], dbt_target: str
) -> list[NodeContext]:
    """Run the compiled SQL against duckdb to get failure contexts."""
    contexts = []
    duckdb_path = PudlPaths().output_file("pudl_dbt_tests.duckdb")
    with duckdb.connect(duckdb_path) as con:
        for setting, value in duckdb_settings(dbt_target).items():
            con.execute(f"SET {setting} = '{value}'")
        for node in nodes:
            con.execute(node.compiled_code)
            node_df = con.fetchdf()
            # tabulate can raise on pd.NA, so normalize nulls to a sentinel string.
            node_head = node_df.head(20).astype(object)
            node_head = node_head.where(node_head.notna(), "NULL")
            node_str = node_head.to_markdown(maxcolwidths=40, index=False)
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
    use_nightly_builds: bool = False,
) -> BuildResult:
    """Run the DBT build and get failure information back.

    * run the DBT build using our selection, returning test failures. Tables marked
      ``large`` are built in their own pass, see :func:`split_by_table_size`.
    * split the test failures by type - for most, we will just run the compiled
      SQL, but other tests such as the weighted quantile tests need extra
      handling
    * get contexts for various test failure types
    * print out test failure context
    """
    vars_args = []
    if use_nightly_builds:
        base_paths = {
            f"{source}_parquet_base_path": path
            for source, path in NIGHTLY_PARQUET_BASE_PATHS.items()
        }
        vars_args = ["--vars", json.dumps(base_paths)]

    dbt = install_dbt_deps()
    success = True
    failure_contexts: list[NodeContext] = []
    for dbt_pass in split_by_table_size(node_selection, node_exclusion, dbt_target):
        cli_args = ["--target", dbt_pass.target, "--select", dbt_pass.select]
        if dbt_pass.exclude is not None:
            cli_args += ["--exclude", dbt_pass.exclude]
        cli_args += vars_args

        with _preserve_logging_propagation(), chdir(PUDL_DBT_PATH):
            dbt.invoke(["deps"])
            # The seed's column types can change (e.g. ``partition`` was all integers
            # before FERC EQR quarters were added), and dbt won't recreate an existing
            # table with new types unless we ask.
            dbt.invoke(["seed", "--full-refresh"])
            build_output: dbtRunnerResult = dbt.invoke(["build"] + cli_args)
            build_results = cast(RunExecutionResult, build_output.result)

        weighted_quantile_failures, compiled_sql_failures = [], []
        for node in __get_failed_nodes(build_results):
            if "expect_quantile_constraints_" in node.name:
                weighted_quantile_failures.append(node)
            else:
                compiled_sql_failures.append(node)

        success = success and build_output.success
        failure_contexts += __get_compiled_sql_contexts(
            compiled_sql_failures, dbt_pass.target
        ) + __get_quantile_contexts(
            weighted_quantile_failures,
            dbt=dbt,
            dbt_dir=PUDL_DBT_PATH,
            dbt_target=dbt_pass.target,
            vars_args=vars_args,
        )

    return BuildResult(success=success, failure_contexts=failure_contexts)


def dagster_to_dbt_selection(
    selection: str, defs: dg.Definitions, manifest=None
) -> str:
    """Translate dagster asset selection to db node selection.

    We use the dbt manifest to determine which sources are defined in dbt so
    that we can map them to dagster assets. So, we need to generate a fresh dbt
    manifest via ``dbt parse`` whenever we run this function.

    * turn asset selection into asset keys
    * turn asset keys into node names
    * turn node names into selection string
    """
    asset_keys = dg.AssetSelection.from_string(selection).resolve(
        defs.resolve_asset_graph()
    )
    asset_names = {asset_key.to_user_string() for asset_key in asset_keys}

    if manifest is None:
        manifest_path = PUDL_DBT_PATH / "target" / "manifest.json"
        with _preserve_logging_propagation(), chdir(PUDL_DBT_PATH):
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
