"""Configure logging for the PUDL package."""

import logging

import coloredlogs
from dagster import get_dagster_logger


def get_logger(name: str):
    """Helper function to append 'catalystcoop' to logger name and return logger."""
    return get_dagster_logger(f"catalystcoop.{name}")


def configure_root_logger(
    logfile: str | None = None,
    loglevel: str = "INFO",
    dependency_loglevels: dict[str, int] | None = None,
    propagate: bool = False,
) -> None:
    """Configure the root catalystcoop logger.

    Args:
        logfile: Path to logfile or None.
        loglevel: Level of detail at which to log, by default INFO.
        dependency_loglevels: Dictionary mapping dependency name to desired loglevel.
            This allows us to filter excessive logs from dependencies.
        propagate: Whether to propagate logs to ancestor loggers. Useful for ensuring
            that pytest has access to PUDL logs during testing.
    """
    if dependency_loglevels is None:
        dependency_loglevels = {"numba": logging.WARNING}
    # Explicitly set log-level for dependency loggers
    [
        get_dagster_logger(dependency_name).setLevel(dependency_loglevel)
        for dependency_name, dependency_loglevel in dependency_loglevels.items()
    ]

    logger = get_dagster_logger("catalystcoop")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=logger)

    logger.addHandler(logging.NullHandler())

    if logfile is not None:
        file_logger = logging.FileHandler(logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)

    logger.propagate = propagate
