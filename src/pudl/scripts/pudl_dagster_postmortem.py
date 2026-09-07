"""Diagnose a Dagster run that ended without a clean failure event.

The nightly PUDL job runs *in process* (``dg launch`` is ``execute_job``, not a
launcher-managed run), so there is no ``dagster-daemon`` and ``run_monitoring``
never gets a chance to run. Two OOM-style failure modes still need surfacing:

* A step subprocess is OOM-killed. The multiprocess executor already reports this
  with a "terminated by signal 9 (SIGKILL) ... out of memory" engine event, so
  here we just re-echo it prominently.
* The *orchestrating* process is OOM-killed. Nothing gets logged -- the run stops
  mid-stream and stays in a non-terminal state in the instance. This script
  detects that "silent gap" after the fact, logs a loud explanation, and marks
  the run FAILED so the instance is left consistent (the manual analog of what
  ``run_monitoring`` does for the FERC EQR backfill).

Run it right after the Dagster stage in ``builds/pudl_batch.sh``, regardless of
the stage's exit code. It is purely diagnostic and always exits 0.
"""

import click

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

# Substrings the multiprocess executor / run machinery use when a worker dies
# without raising a normal exception. See dagster._utils.get_run_crash_explanation.
_CRASH_SIGNATURES = (
    "terminated by signal",
    "unexpectedly exited",
    "out of memory",
)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--run-id",
    default=None,
    help="Run to inspect. Defaults to the most recent run in the instance.",
)
def main(run_id: str | None) -> None:
    """Log a post-mortem for the given (or most recent) Dagster run."""
    from dagster import DagsterEventType, DagsterInstance, DagsterRunStatus
    from dagster._core.execution.stats import StepEventStatus

    nonterminal_statuses = (
        DagsterRunStatus.NOT_STARTED,
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    )

    instance = DagsterInstance.get()

    if run_id is not None:
        run = instance.get_run_by_id(run_id)
    else:
        runs = instance.get_runs(limit=1)
        run = runs[0] if runs else None

    if run is None:
        logger.warning("No Dagster run found to post-mortem.")
        return

    logger.info(
        f"Post-mortem for run {run.run_id} (job {run.job_name!r}): "
        f"status={run.status.value}"
    )

    # Re-echo any worker-crash engine events (step-level OOM kills, etc.).
    crash_messages = [
        record.dagster_event.message
        for record in instance.all_logs(
            run.run_id,
            of_type={DagsterEventType.ENGINE_EVENT, DagsterEventType.STEP_FAILURE},
        )
        if record.dagster_event is not None
        and record.dagster_event.message is not None
        and any(sig in record.dagster_event.message for sig in _CRASH_SIGNATURES)
    ]
    for message in crash_messages:
        logger.error(f"Worker crash detected: {message}")

    # Steps that started but never reached a terminal state -- the fingerprint of
    # a killed step worker (or a killed orchestrator that took its steps down).
    stranded_steps = sorted(
        stat.step_key
        for stat in instance.get_run_step_stats(run.run_id)
        if stat.status in (None, StepEventStatus.IN_PROGRESS)
    )
    if stranded_steps:
        logger.error(
            f"{len(stranded_steps)} step(s) started but never reported success or "
            f"failure (worker likely killed): {stranded_steps}"
        )

    # A non-terminal run status here means the orchestrating process exited
    # without finishing -- almost always an OOM SIGKILL of the run process.
    if run.status in nonterminal_statuses:
        logger.error(
            f"Run {run.run_id} is still {run.status.value} but its process has "
            "exited. This is the signature of an OOM-killed orchestrator: check "
            "the VM memory metric on the build dashboard, and consider a larger "
            "machine type or a lower memory-use:high concurrency limit in "
            "dg_nightly.yml. Marking the run FAILED so the instance is consistent."
        )
        instance.report_run_failed(
            run,
            message=(
                "Marked FAILED by pudl_dagster_postmortem: the run process exited "
                "while the run was still in a non-terminal state (likely OOM SIGKILL)."
            ),
        )
    elif not crash_messages and not stranded_steps:
        logger.info("No worker-crash signatures found in this run.")


if __name__ == "__main__":
    main()
