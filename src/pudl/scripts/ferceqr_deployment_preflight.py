"""Preflight checks for FERC EQR deployment destinations and prerequisites."""

from __future__ import annotations

import sys
from contextlib import suppress
from uuid import uuid4

import click
from upath import UPath


def ensure_path_exists(path: UPath, description: str) -> None:
    """Raise if the given source path is not readable via :class:`~upath.UPath`."""
    if not path.exists():
        raise click.ClickException(
            f"{description} does not exist or is not readable: {path}"
        )


def verify_path_is_writable(path: UPath, description: str, build_id: str) -> None:
    """Verify write-read-delete access to a target path using a UUID canary."""
    canary_name = f".ferceqr-preflight-{build_id}-{uuid4().hex}.txt"
    canary_path = path / canary_name
    canary_bytes = f"ferceqr-preflight:{description}:{build_id}".encode()

    try:
        canary_path.write_bytes(canary_bytes)
        if canary_path.read_bytes() != canary_bytes:
            raise click.ClickException(
                f"{description} write/read canary verification failed: {canary_path}"
            )
    except OSError as exc:
        raise click.ClickException(
            f"{description} is not writable: {path} ({exc})"
        ) from exc
    finally:
        with suppress(OSError):
            canary_path.unlink()


def load_deployment_targets(deployment_config_path: str | None) -> list[UPath]:
    """Load resolved deployment targets through the Dagster resource."""
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    from pudl.dagster.resources import FercEqrDeploymentResource  # noqa: PLC0415

    resource = FercEqrDeploymentResource(
        deployment_config_path=deployment_config_path or None
    )
    return resource.resolved_targets()


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--archive-path",
    envvar="PUDL_FERCEQR_ARCHIVE_PATH",
    required=True,
    show_envvar=True,
    help="Archive path that will be read during the FERC EQR build.",
)
@click.option(
    "--deployment-config-path",
    envvar="PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH",
    default=None,
    show_envvar=True,
    help="Optional deployment target YAML path. If unset, deployment is skipped.",
)
@click.option(
    "--logs-path",
    envvar="GCS_LOGS_BUCKET",
    required=True,
    show_envvar=True,
    help="Logs destination that must be writable for batch runs.",
)
@click.option(
    "--build-id",
    envvar="BUILD_ID",
    default="no-build-id",
    show_envvar=True,
    help="Build identifier used to namespace canary objects.",
)
def main(
    archive_path: str,
    deployment_config_path: str | None,
    logs_path: str,
    build_id: str,
) -> int:
    """Verify that FERC EQR inputs and destinations are accessible before a build."""
    archive_upath = UPath(archive_path)
    logs_upath = UPath(logs_path)

    click.echo(f"Checking archive readability: {archive_upath}")
    ensure_path_exists(archive_upath, "FERC EQR archive path")

    click.echo(f"Checking logs destination write access: {logs_upath}")
    verify_path_is_writable(logs_upath, "FERC EQR logs destination", build_id)

    deployment_targets = load_deployment_targets(deployment_config_path)
    if not deployment_targets:
        click.echo(
            "No FERC EQR deployment targets configured; skipping deployment checks."
        )
        return 0

    for target in deployment_targets:
        click.echo(f"Checking deployment target write access: {target}")
        verify_path_is_writable(target, "FERC EQR deployment target", build_id)

    click.echo("FERC EQR deployment preflight checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
