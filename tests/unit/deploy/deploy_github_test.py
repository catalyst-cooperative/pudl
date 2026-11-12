"""Test helpers for dispatching GitHub Actions workflows."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from pudl.deploy.github import dispatch_github_workflow, update_pudl_viewer


@pytest.mark.parametrize(
    "inputs,expected_json",
    [
        (None, {"ref": "main"}),
        (
            {"env": "sandbox", "publish": "publish"},
            {"ref": "main", "inputs": {"env": "sandbox", "publish": "publish"}},
        ),
    ],
)
def test_dispatch_github_workflow_posts_expected_payload(inputs, expected_json):
    """dispatch_github_workflow should POST a bare ref, or one with inputs attached."""
    with patch("pudl.deploy.github.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=204)

        dispatch_github_workflow(
            repo="catalyst-cooperative/eel-hole",
            workflow_file="build-deploy.yml",
            ref="main",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs=inputs,
        )

        mock_post.return_value.raise_for_status.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == (
            "https://api.github.com/repos/catalyst-cooperative/eel-hole"
            "/actions/workflows/build-deploy.yml/dispatches"
        )
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token"
        assert kwargs["json"] == expected_json


def test_dispatch_github_workflow_raises_on_http_error():
    """A failed dispatch should propagate the HTTP error."""
    with patch("pudl.deploy.github.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=422)
        mock_post.return_value.raise_for_status.side_effect = requests.HTTPError(
            "422 Client Error"
        )

        with pytest.raises(requests.HTTPError):
            dispatch_github_workflow(
                repo="catalyst-cooperative/pudl",
                workflow_file="zenodo-data-release.yml",
                ref="main",
                token="fake-token",  # noqa: S106  # pragma: allowlist secret
            )


@pytest.mark.parametrize(
    "environment,expected_workflow_file",
    [
        ("staging", "build-deploy-staging.yml"),
        ("production", "build-deploy.yml"),
    ],
)
def test_update_pudl_viewer_selects_workflow_by_environment(
    environment, expected_workflow_file
):
    """Staging and production should dispatch different Eel Hole workflows."""
    with patch("pudl.deploy.github.dispatch_github_workflow") as mock_dispatch:
        update_pudl_viewer(
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            environment=environment,
        )

        mock_dispatch.assert_called_once_with(
            repo="catalyst-cooperative/eel-hole",
            workflow_file=expected_workflow_file,
            ref="main",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )
