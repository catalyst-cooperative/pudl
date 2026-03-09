"""Configure logging for the PUDL package."""

import logging
import os

import coloredlogs
from dagster import get_dagster_logger

DEFAULT_DEPENDENCY_LOGLEVELS: dict[str, int] = {
    "aiobotocore": logging.WARNING,
    "alembic": logging.WARNING,
    "arelle": logging.INFO,
    "asyncio": logging.INFO,
    "boto3": logging.WARNING,
    "botocore": logging.WARNING,
    "fsspec": logging.INFO,
    "google": logging.INFO,
    "matplotlib": logging.WARNING,
    "numba": logging.WARNING,
    "urllib3": logging.INFO,
}


def _get_env_bool(var_name: str, default: bool) -> bool:
    """Parse a boolean from environment variable values."""
    value = os.getenv(var_name)
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def get_logger(name: str):
    """Helper function to append 'catalystcoop' to logger name and return logger."""
    return get_dagster_logger(f"catalystcoop.{name}")


def configure_root_logger(
    logfile: str | None = None,
    loglevel: str | None = None,
    dependency_loglevels: dict[str, int] | None = None,
    color_logs: bool | None = None,
    propagate: bool = False,
) -> None:
    """Configure the root catalystcoop logger.

    Args:
        logfile: Path to logfile or None.
        loglevel: Level of detail at which to log. If omitted, read from
            ``PUDL_LOGLEVEL`` env var and default to ``INFO``.
        dependency_loglevels: Dictionary mapping dependency name to desired loglevel.
            This allows us to filter excessive logs from dependencies.
        color_logs: Whether to emit ANSI color codes. If omitted, read from
            ``PUDL_COLOR_LOGS`` env var and default to ``True``.
        propagate: Whether to propagate logs to ancestor loggers. Useful for ensuring
            that pytest has access to PUDL logs during testing.
    """
    if loglevel is None:
        loglevel = os.getenv("PUDL_LOGLEVEL", "INFO")
    if logfile is None:
        logfile = os.getenv("PUDL_LOGFILE")
    if color_logs is None:
        color_logs = _get_env_bool("PUDL_COLOR_LOGS", default=True)

    if dependency_loglevels is None:
        dependency_loglevels = dict(DEFAULT_DEPENDENCY_LOGLEVELS)

    # Explicitly set log-level for dependency loggers
    for dependency_name, dependency_loglevel in dependency_loglevels.items():
        logging.getLogger(dependency_name).setLevel(dependency_loglevel)

    # Normalize upstream ferc_xbrl_extractor logging to flow through our configured
    # handlers and formatter without requiring changes in that package.
    ferc_xbrl_logger = logging.getLogger("catalystcoop.ferc_xbrl_extractor")
    if ferc_xbrl_logger.handlers:
        ferc_xbrl_logger.handlers.clear()
    ferc_xbrl_logger.propagate = True

    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    loggers_to_configure = [
        get_dagster_logger("catalystcoop"),
        logging.getLogger("catalystcoop"),
    ]
    for logger in loggers_to_configure:
        coloredlogs.install(
            fmt=log_format,
            level=loglevel,
            logger=logger,
            isatty=color_logs,
        )

        logger.addHandler(logging.NullHandler())

        if logfile is not None:
            file_logger = logging.FileHandler(logfile)
            file_logger.setFormatter(logging.Formatter(log_format))
            logger.addHandler(file_logger)

        logger.propagate = propagate

    if propagate:
        logging.getLogger("dagster").propagate = True
