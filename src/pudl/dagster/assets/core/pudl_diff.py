"""Dagster asset that compares this build's Parquet outputs against baselines.

The comparison runs by default on Google Batch, where ``dg_nightly.yml`` turns it on,
and is otherwise opt-in, since it downloads whole baseline datasets. Which baselines a
build compares against, and how the reports are named, is decided by
:func:`pudl.deploy.pudl_diff.plan_build_diffs`.

The reports are written to ``$PUDL_OUTPUT/pudl_diff``, so they are saved with the rest
of the build's outputs and distributed along with them when it's deployed. A stable
release's report is made when it's deployed rather than here (see
:func:`pudl.deploy.pudl_diff.prepare_diff_reports`).
"""

import os

import dagster as dg
from pudl_diff.dataset import PudlDiffDataset

from pudl.deploy.pudl import PUDL_DIFF_DIRNAME
from pudl.deploy.pudl_diff import plan_build_diffs, run_diff_plan
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

RUN_ENV_VAR = "PUDL_DIFF_RUN"
LEFT_ROOT_ENV_VAR = "PUDL_DIFF_LEFT_ROOT"


class PudlDiffConfig(dg.Config):
    """Whether to run PUDL Diff, and what to compare against."""

    run: bool = False
    """Run PUDL Diff, even if ``PUDL_DIFF_RUN`` isn't ``true``."""
    left_root: str | None = None
    """Compare only against the dataset at this root, unless
    ``PUDL_DIFF_LEFT_ROOT`` is set, which takes precedence."""


@dg.asset(
    name="pudl_diff",
    group_name="core_pudl",
    deps=["pudl_datapackage"],
    required_resource_keys={"pudl_paths"},
    description=(
        "Reports on how this build's Parquet outputs differ from the last nightly "
        "build and the last stable release. Only run in builds on Google Batch, or "
        "when PUDL_DIFF_RUN=true. Written to $PUDL_OUTPUT/pudl_diff."
    ),
)
def pudl_diff(
    context: dg.AssetExecutionContext, config: PudlDiffConfig
) -> dg.MaterializeResult:
    """Compare this build against its baselines, and save a report on each."""
    run = config.run or os.environ.get(RUN_ENV_VAR, "").lower() == "true"
    if not run:
        logger.info(f"Skipping PUDL Diff: it's off, unless {RUN_ENV_VAR}=true.")
        return dg.MaterializeResult(metadata={"skipped": True})

    plans = plan_build_diffs(
        git_tag=os.environ.get("GIT_TAG"),
        build_id=os.environ.get("BUILD_ID"),
        gcs_output=os.environ.get("PUDL_GCS_OUTPUT"),
        left_root=os.environ.get(LEFT_ROOT_ENV_VAR) or config.left_root,
    )
    if not plans:
        logger.info("No PUDL Diff reports are made in the ETL for this build.")
        return dg.MaterializeResult(metadata={"skipped": True})

    pudl_paths = context.resources.pudl_paths
    reports_dir = pudl_paths.output_file(PUDL_DIFF_DIRNAME)
    metadata: dict[str, dg.MetadataValue] = {}
    for plan in plans:
        # The right dataset's provenance is cached, so it needs a fresh instance
        # for each plan; it records the build's outputs' durable location, if known.
        right = PudlDiffDataset(
            pudl_paths.parquet_path(), display_root=plan.right_display_root
        )
        # A failed comparison must never fail the build: differences are expected,
        # and this report is informational.
        try:
            report = run_diff_plan(plan, right, reports_dir)
        except Exception:
            logger.exception(f"PUDL Diff against {plan.left_root} failed.")
            metadata[f"{plan.left_root} error"] = dg.MetadataValue.bool(True)
            continue
        if report is None:
            metadata[f"{plan.left_root} skipped"] = dg.MetadataValue.bool(True)
            continue
        summary = report.summary
        metadata[plan.left_root] = dg.MetadataValue.json(
            {
                "success": report.success,
                "is_identical": report.is_identical,
                "tables_compared": summary.table_count,
                "tables_changed": summary.changed_table_count,
                "tables_failed": summary.failed_table_count,
                "peak_rss": summary.peak_rss,
                "elapsed_seconds": report.elapsed_seconds,
            }
        )
    return dg.MaterializeResult(metadata=metadata)


__all__ = ["PudlDiffConfig", "pudl_diff"]
