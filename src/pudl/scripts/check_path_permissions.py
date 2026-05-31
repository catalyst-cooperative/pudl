"""Check read, write, and delete permissions for local or remote paths."""

import json
import logging
import os
import sys
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import click
from upath import UPath

NOISY_BACKEND_LOGGERS = (
    "fsspec",
    "gcsfs",
    "gcsfs.retry",
    "s3fs",
)


class PermissionCheck(StrEnum):
    """Named permission-check stages used by the CLI and its reports."""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"


@dataclass
class PathPermissionError(click.ClickException):
    """Permission check failure annotated with the stage that failed."""

    check: PermissionCheck
    message: str

    def __post_init__(self) -> None:
        """Initialize the underlying Click exception message."""
        super().__init__(self.message)


@dataclass
class CheckReport:
    """Structured result for a single permission check."""

    requested: bool
    success: bool | None = None
    messages: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class PathReport:
    """Structured result for all permission checks against one path."""

    path: str
    resolved_path: str | None
    anon: bool
    checks: dict[PermissionCheck, CheckReport]
    success: bool = False


@dataclass
class PathCheckReport:
    """Top-level report for a CLI invocation across one or more paths."""

    paths: list[str]
    anon: bool
    results: list[PathReport]
    success: bool = False


def _get_ferceqr_deployment_paths() -> list[str]:
    """Return resolved FERCEQR deployment targets from the configured YAML path."""
    deployment_config_path = os.getenv("PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH")
    if not deployment_config_path:
        return []

    from pudl.dagster.resources import FercEqrDeploymentResource

    resource = FercEqrDeploymentResource(deployment_config_path=deployment_config_path)
    return [str(path) for path in resource.resolved_targets()]


@contextmanager
def _suppress_backend_tracebacks() -> Any:
    """Temporarily silence noisy storage-backend exception logging."""
    original_levels: dict[str, int] = {}
    for logger_name in NOISY_BACKEND_LOGGERS:
        logger = logging.getLogger(logger_name)
        original_levels[logger_name] = logger.level
        logger.setLevel(logging.CRITICAL)

    try:
        yield
    finally:
        for logger_name, original_level in original_levels.items():
            logging.getLogger(logger_name).setLevel(original_level)


def _build_upath(path: str, anon: bool) -> UPath:
    """Return a :class:`~upath.UPath` configured for the requested auth mode."""
    scheme = urlparse(path).scheme
    storage_options: dict[str, Any] = {}

    if anon and scheme == "gs":
        storage_options["token"] = "anon"  # noqa: S105
    elif anon and scheme == "s3":
        storage_options["anon"] = True

    return UPath(path, **storage_options)


def _ensure_directory_like_path(path: UPath) -> None:
    """Raise if the provided path points at an existing file/object."""
    try:
        if path.exists() and path.is_file():
            raise PathPermissionError(
                check=PermissionCheck.WRITE,
                message=(
                    f"Path must be a directory-like location, not an existing file: {path}"
                ),
            )
    except PathPermissionError:
        raise
    except Exception as exc:
        raise PathPermissionError(
            check=PermissionCheck.WRITE,
            message=f"Unable to inspect path type for {path}: {exc}",
        ) from exc


def check_read_access(path: UPath) -> None:
    """Raise if the given path cannot be read as a directory-like location."""
    _ensure_directory_like_path(path)

    try:
        if not path.exists():
            raise PathPermissionError(
                check=PermissionCheck.READ,
                message=f"Path does not exist or is not readable: {path}",
            )

        with suppress(StopIteration):
            next(path.iterdir())
    except PathPermissionError:
        raise
    except Exception as exc:
        raise PathPermissionError(
            check=PermissionCheck.READ,
            message=f"Path is not readable: {path} ({exc})",
        ) from exc


def check_write_access(path: UPath) -> None:
    """Raise if a canary file cannot be written, read back, and deleted."""
    _ensure_directory_like_path(path)

    canary_name = f".check-path-permissions-{uuid4().hex}.txt"
    canary_path = path / canary_name
    canary_bytes = f"check_path_permissions:{canary_name}".encode()

    try:
        canary_path.write_bytes(canary_bytes)
    except Exception as exc:
        raise PathPermissionError(
            check=PermissionCheck.WRITE,
            message=f"Path is not writable: {path} ({exc})",
        ) from exc

    try:
        observed_bytes = canary_path.read_bytes()
        if observed_bytes != canary_bytes:
            raise PathPermissionError(
                check=PermissionCheck.WRITE,
                message=(
                    f"Canary verification failed for {canary_path}: content mismatch"
                ),
            )
    except PathPermissionError:
        raise
    except Exception as exc:
        raise PathPermissionError(
            check=PermissionCheck.WRITE,
            message=f"Unable to read back written canary at {canary_path}: {exc}",
        ) from exc

    try:
        canary_path.unlink()
    except Exception as exc:
        raise PathPermissionError(
            check=PermissionCheck.DELETE,
            message=f"Unable to delete written canary at {canary_path}: {exc}",
        ) from exc


def _emit(message: str, *, json_output: bool, summary: PathReport) -> None:
    """Record a message in JSON mode or print it for humans."""
    if not json_output:
        click.echo(message)


def _emit_error(message: str, *, json_output: bool, summary: PathReport) -> None:
    """Record an error in JSON mode or print it for humans."""
    if not json_output:
        click.echo(message, err=True)


def _emit_check_message(
    *,
    check_name: PermissionCheck,
    message: str,
    json_output: bool,
    summary: PathReport,
) -> None:
    """Record a message for a specific check in JSON mode or print it for humans."""
    summary.checks[check_name].messages.append(message)
    _emit(message, json_output=json_output, summary=summary)


def _emit_check_error(
    *,
    check_name: PermissionCheck,
    message: str,
    json_output: bool,
    summary: PathReport,
) -> None:
    """Record an error for a specific check in JSON mode or print it for humans."""
    summary.checks[check_name].errors.append(message)
    _emit_error(message, json_output=json_output, summary=summary)


def _run_check(
    *,
    action: PermissionCheck,
    resolved_path: UPath,
    json_output: bool,
    summary: PathReport,
) -> None:
    """Run a single permission check and update the structured summary."""
    if action is PermissionCheck.READ:
        _emit_check_message(
            check_name=PermissionCheck.READ,
            message=f"Checking read access for: {resolved_path}",
            json_output=json_output,
            summary=summary,
        )
        try:
            check_read_access(resolved_path)
        except PathPermissionError as exc:
            summary.checks[PermissionCheck.READ].success = False
            _emit_check_error(
                check_name=PermissionCheck.READ,
                message=str(exc),
                json_output=json_output,
                summary=summary,
            )
        else:
            summary.checks[PermissionCheck.READ].success = True
            _emit_check_message(
                check_name=PermissionCheck.READ,
                message=f"Read check passed for: {resolved_path}",
                json_output=json_output,
                summary=summary,
            )
        return

    _emit_check_message(
        check_name=PermissionCheck.WRITE,
        message=f"Checking write/delete access for: {resolved_path}",
        json_output=json_output,
        summary=summary,
    )
    try:
        check_write_access(resolved_path)
    except PathPermissionError as exc:
        if exc.check is PermissionCheck.DELETE:
            summary.checks[PermissionCheck.WRITE].success = True
            summary.checks[PermissionCheck.DELETE].success = False
            _emit_check_error(
                check_name=PermissionCheck.DELETE,
                message=str(exc),
                json_output=json_output,
                summary=summary,
            )
        else:
            summary.checks[PermissionCheck.WRITE].success = False
            summary.checks[PermissionCheck.DELETE].success = False
            _emit_check_error(
                check_name=PermissionCheck.WRITE,
                message=str(exc),
                json_output=json_output,
                summary=summary,
            )
    else:
        summary.checks[PermissionCheck.WRITE].success = True
        summary.checks[PermissionCheck.DELETE].success = True
        _emit_check_message(
            check_name=PermissionCheck.WRITE,
            message=f"Write/delete check passed for: {resolved_path}",
            json_output=json_output,
            summary=summary,
        )
        _emit_check_message(
            check_name=PermissionCheck.DELETE,
            message=f"Delete check passed for: {resolved_path}",
            json_output=json_output,
            summary=summary,
        )


def _check_single_path(
    *,
    path: str,
    read_requested: bool,
    write_requested: bool,
    json_output: bool,
    anon: bool,
) -> PathReport:
    """Run the requested checks for one path and return a structured summary."""
    summary = PathReport(
        path=path,
        resolved_path=None,
        anon=anon,
        checks={
            PermissionCheck.READ: CheckReport(requested=read_requested),
            PermissionCheck.WRITE: CheckReport(requested=write_requested),
            PermissionCheck.DELETE: CheckReport(requested=write_requested),
        },
    )

    try:
        with _suppress_backend_tracebacks():
            resolved_path = _build_upath(path, anon=anon)
            summary.resolved_path = str(resolved_path)

            if read_requested:
                _run_check(
                    action=PermissionCheck.READ,
                    resolved_path=resolved_path,
                    json_output=json_output,
                    summary=summary,
                )

            if write_requested:
                _run_check(
                    action=PermissionCheck.WRITE,
                    resolved_path=resolved_path,
                    json_output=json_output,
                    summary=summary,
                )
    except PathPermissionError as exc:
        if exc.check in summary.checks:
            summary.checks[exc.check].success = False
            _emit_check_error(
                check_name=exc.check,
                message=str(exc),
                json_output=json_output,
                summary=summary,
            )
    except Exception as exc:
        _emit_check_error(
            check_name=(
                PermissionCheck.READ if read_requested else PermissionCheck.WRITE
            ),
            message=f"Unexpected error while checking path permissions for {path}: {exc}",
            json_output=json_output,
            summary=summary,
        )

    summary.success = all(
        (not check_summary.requested) or check_summary.success is True
        for check_summary in summary.checks.values()
    )
    return summary


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("paths", nargs=-1, required=True)
@click.option(
    "--read",
    "read_requested",
    is_flag=True,
    help="Check only that the path is readable.",
)
@click.option(
    "--write",
    "write_requested",
    is_flag=True,
    help="Check that the path can be written to and deleted from.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Emit a structured JSON summary of the permission checks.",
)
@click.option(
    "--anon",
    is_flag=True,
    help="Force anonymous access for s3:// and gs:// paths.",
)
@click.option(
    "--check-ferceqr-deployment-paths",
    is_flag=True,
    help=(
        "Append FERC EQR deployment targets resolved from "
        "PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH."
    ),
)
@click.pass_context
def main(
    ctx: click.Context,
    paths: tuple[str, ...],
    read_requested: bool,
    write_requested: bool,
    json_output: bool,
    anon: bool,
    check_ferceqr_deployment_paths: bool,
) -> int:
    """Check path permissions using UPath for local filesystems and cloud buckets."""
    if not read_requested and not write_requested:
        read_requested = True
        write_requested = True

    paths_to_check = list(paths)
    if check_ferceqr_deployment_paths:
        paths_to_check.extend(_get_ferceqr_deployment_paths())

    path_summaries = [
        _check_single_path(
            path=path,
            read_requested=read_requested,
            write_requested=write_requested,
            json_output=json_output,
            anon=anon,
        )
        for path in paths_to_check
    ]
    summary = PathCheckReport(
        paths=paths_to_check,
        anon=anon,
        results=path_summaries,
        success=all(path_summary.success for path_summary in path_summaries),
    )

    if json_output:
        click.echo(json.dumps(asdict(summary), sort_keys=True, indent=2))

    status_code = 0 if summary.success else 1
    if status_code != 0:
        ctx.exit(status_code)
    return status_code


if __name__ == "__main__":
    sys.exit(main())
