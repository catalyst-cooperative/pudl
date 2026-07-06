"""Tests for the deployment side-effect gating logic in pudl_deploy."""

import os

import pytest
from click.testing import CliRunner

from pudl.deploy.pudl import (
    DeploymentPlan,
    DeploymentType,
    ResolvedBuild,
    new_deploy_stage_results,
)
from pudl.scripts import pudl_deploy
from pudl.scripts.pudl_deploy import _deploy_outputs


@pytest.fixture
def deploy_mocks(mocker):
    """Patch every side-effecting call made by ``_deploy_outputs``."""
    return {
        "upload_outputs": mocker.patch("pudl.scripts.pudl_deploy.upload_outputs"),
        "update_pudl_viewer": mocker.patch(
            "pudl.scripts.pudl_deploy.update_pudl_viewer"
        ),
        "update_git_branch": mocker.patch("pudl.scripts.pudl_deploy.update_git_branch"),
        "trigger_zenodo_release": mocker.patch(
            "pudl.scripts.pudl_deploy.trigger_zenodo_release"
        ),
        "set_gcs_temporary_hold": mocker.patch(
            "pudl.scripts.pudl_deploy.set_gcs_temporary_hold"
        ),
    }


@pytest.mark.parametrize(
    "deploy_type,environment,expect_viewer,expect_git,expect_zenodo,expect_hold",
    [
        (DeploymentType.NIGHTLY, "production", True, True, True, False),
        (DeploymentType.NIGHTLY, "staging", True, True, True, False),
        (DeploymentType.STABLE, "production", False, True, True, True),
        (DeploymentType.STABLE, "staging", False, True, True, False),
        (DeploymentType.BRANCH, "staging", False, False, False, False),
    ],
)
def test_deploy_outputs_gates_side_effects_by_deploy_type(
    mocker,
    deploy_mocks,
    deploy_type,
    environment,
    expect_viewer,
    expect_git,
    expect_zenodo,
    expect_hold,
):
    """Each side effect should only fire for the deploy types the spec allows.

    - Eel Hole redeploys: nightly only.
    - Git branch update: everything except branch builds.
    - Zenodo release: everything except branch builds.
    - GCS temporary hold: stable + production only.
    """
    git_tags = {
        DeploymentType.NIGHTLY: "nightly-2026-07-05",
        DeploymentType.STABLE: "v2026.7.0",
        DeploymentType.BRANCH: "branch-2026-07-05-0600-abc123456-my-branch",
    }
    git_tag = git_tags[deploy_type]

    plan = DeploymentPlan(git_tag=git_tag, environment=environment)

    _deploy_outputs(
        source_dir=mocker.MagicMock(),
        plan=plan,
        github_token="fake-token",  # noqa: S106
        stage_results=new_deploy_stage_results(),
    )

    assert deploy_mocks["upload_outputs"].called
    assert deploy_mocks["update_pudl_viewer"].called == expect_viewer
    assert deploy_mocks["update_git_branch"].called == expect_git
    assert deploy_mocks["trigger_zenodo_release"].called == expect_zenodo
    assert deploy_mocks["set_gcs_temporary_hold"].called == expect_hold


@pytest.fixture
def mock_deploy_dependencies(mocker, tmp_path):
    """Patch every external call ``main()`` makes, to exercise its exit code."""
    fake_build_path = mocker.MagicMock()
    fake_build_path.name = "2026-07-04-0600-abc123456-main"

    mocker.patch(
        "pudl.scripts.pudl_deploy.resolve_build",
        return_value=ResolvedBuild(
            plan=DeploymentPlan(git_tag="nightly-2026-07-04", environment="staging"),
            build_path=fake_build_path,
            build_id=fake_build_path.name,
            local_copy_path=tmp_path,
            local_logfile=tmp_path
            / f"{fake_build_path.name}-deploy-2026-07-04-0600.log",
        ),
    )
    mocker.patch("pudl.scripts.pudl_deploy.download_build_outputs")
    mocker.patch("pudl.scripts.pudl_deploy.prepare_outputs_for_distribution")
    mocker.patch("pudl.scripts.pudl_deploy.upload_outputs")
    mocker.patch("pudl.scripts.pudl_deploy.update_pudl_viewer")
    mocker.patch("pudl.scripts.pudl_deploy.update_git_branch")
    mocker.patch("pudl.scripts.pudl_deploy.trigger_zenodo_release")
    mocker.patch("pudl.scripts.pudl_deploy.set_gcs_temporary_hold")
    mocker.patch("pudl.scripts.pudl_deploy.send_zulip_message")
    mocker.patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"})  # noqa: S106  # pragma: allowlist secret
    return fake_build_path


def test_main_exits_zero_when_all_stages_succeed(mock_deploy_dependencies):
    """A fully successful deployment should exit 0."""
    result = CliRunner().invoke(pudl_deploy.main, ["nightly-2026-07-04"])

    assert result.exit_code == 0


def test_main_exits_nonzero_when_a_best_effort_stage_fails(
    mocker, mock_deploy_dependencies
):
    """A best-effort stage failure must produce a nonzero exit code.

    Regression test: a Click command body returning a plain int is silently
    discarded by Click's standalone mode, so this previously always exited 0
    even when a stage failed. ``ctx.exit(1)`` is what actually makes this work.
    """
    mocker.patch("pudl.deploy.pudl.logger")  # silence the expected logger.exception
    mocker.patch(
        "pudl.scripts.pudl_deploy.trigger_zenodo_release",
        side_effect=RuntimeError("zenodo is down"),
    )

    result = CliRunner().invoke(pudl_deploy.main, ["nightly-2026-07-04"])

    assert result.exit_code == 1


def test_main_sends_zulip_notification_when_build_resolution_fails(mocker):
    """A failure to resolve the build must still notify Zulip and exit nonzero.

    Regression test: ``resolve_build()`` used to run as plain code before the
    ``try/finally`` that sends the Zulip notification, so a failure there (e.g.
    no successful build yet for the given tag -- easy to hit with a hand-picked
    ``workflow_dispatch`` tag) crashed with only a bare traceback and no
    notification at all.
    """
    mocker.patch(
        "pudl.scripts.pudl_deploy.resolve_build",
        side_effect=RuntimeError("no build found for tag"),
    )
    mock_send_zulip = mocker.patch("pudl.scripts.pudl_deploy.send_zulip_message")
    mocker.patch.dict(
        os.environ,
        {
            "GITHUB_TOKEN": "fake-token",  # noqa: S106  # pragma: allowlist secret
            "ZULIP_API_KEY": "fake-key",  # pragma: allowlist secret
        },
    )

    result = CliRunner().invoke(pudl_deploy.main, ["nightly-2026-07-04"])

    assert result.exit_code == 1
    mock_send_zulip.assert_called_once()
    message = mock_send_zulip.call_args.args[0]
    assert ":x: PUDL Deployment Failed" in message
    assert "| Resolve build | :x: |" in message
