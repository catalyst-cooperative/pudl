"""Configure logging for the PUDL package."""
import logging

import coloredlogs


def get_logger(name: str):
    """Helper function to append 'catalystcoop' to logger name and return logger."""
    return logging.getLogger(f"catalystcoop.{name}")


def configure_root_logger(logfile: str | None = None, loglevel: str = "INFO"):
    """Configure the root catalystcoop logger.

    Args:
        logfile: Path to logfile or None.
        loglevel: Level of detail at which to log, by default INFO.
    """
    logger = logging.getLogger("catalystcoop")
    log_format = "%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s"
    coloredlogs.install(fmt=log_format, level=loglevel, logger=logger)

    logger.addHandler(logging.NullHandler())

    if logfile is not None:
        file_logger = logging.FileHandler(logfile)
        file_logger.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_logger)
