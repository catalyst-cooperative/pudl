"""Helpers for triggering GitHub Actions workflows from deployment code."""

from typing import Literal

import requests

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


def dispatch_github_workflow(
    repo: str,
    workflow_file: str,
    ref: str,
    token: str,
    inputs: dict[str, str] | None = None,
) -> None:
    """Trigger a workflow_dispatch event on a GitHub Actions workflow.

    Args:
        repo: GitHub repo in "owner/name" form (e.g. "catalyst-cooperative/pudl").
        workflow_file: Workflow filename (e.g. "zenodo-data-release.yml").
        ref: Git branch or tag to run the workflow from.
        token: Bearer token to authenticate to GitHub.
        inputs: workflow_dispatch inputs, if the workflow takes any.
    """
    payload: dict[str, str | dict[str, str]] = {"ref": ref}
    if inputs:
        payload["inputs"] = inputs

    response = requests.post(
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
        },
        json=payload,
        timeout=10,
    )
    response.raise_for_status()


def redeploy_eel_hole(
    token: str,
    environment: Literal["staging", "production"],
) -> None:
    """Redeploy Eel Hole (the PUDL data viewer) with its latest image.

    Args:
        token: the bearer token to authenticate to GitHub.
        environment: redeploy the staging or production Eel Hole.
    """
    logger.info(f"Redeploying Eel Hole ({environment})")

    workflow_file = (
        "build-deploy-staging.yml" if environment == "staging" else "build-deploy.yml"
    )

    dispatch_github_workflow(
        repo="catalyst-cooperative/eel-hole",
        workflow_file=workflow_file,
        ref="main",
        token=token,
    )

    logger.info("Eel Hole redeployment workflow triggered")
