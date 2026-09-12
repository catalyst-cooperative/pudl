"""Unit tests for the ``pudl_dagster_postmortem`` diagnostic script."""

from click.testing import CliRunner
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.test_utils import create_run_for_test, instance_for_test

from pudl.scripts.pudl_dagster_postmortem import main


def _status(instance: DagsterInstance, run_id: str) -> DagsterRunStatus:
    run = instance.get_run_by_id(run_id)
    assert run is not None
    return run.status


def test_postmortem_marks_silently_killed_run_failed() -> None:
    """A run left in a non-terminal state is reported FAILED."""
    with instance_for_test() as instance:
        run = create_run_for_test(
            instance,
            job_name="pudl_with_ferc_to_sqlite",
            status=DagsterRunStatus.STARTED,
        )

        result = CliRunner().invoke(main, [])
        assert result.exit_code == 0

        assert _status(instance, run.run_id) == DagsterRunStatus.FAILURE


def test_postmortem_leaves_terminal_run_untouched() -> None:
    """A run that already succeeded is not modified."""
    with instance_for_test() as instance:
        run = create_run_for_test(
            instance,
            job_name="pudl_with_ferc_to_sqlite",
            status=DagsterRunStatus.SUCCESS,
        )

        result = CliRunner().invoke(main, [])
        assert result.exit_code == 0

        assert _status(instance, run.run_id) == DagsterRunStatus.SUCCESS


def test_postmortem_no_runs() -> None:
    """No runs in the instance is handled gracefully."""
    with instance_for_test():
        result = CliRunner().invoke(main, [])
        assert result.exit_code == 0
