"""This module exists specifically to configure the root ``catalystcoop`` logger.

The module is only ever imported when dagster kicks off a run of the full/fast ETL
from the dagster UI. See ``get_dagster_execution_config`` in ``pudl.helpers`` where we
create the configuration for the dagster multiprocess executor to load this module for
each subprocess. This means when we run the ETL from ``pytest``, we avoid configuring
the logger at import time, which avoids a weird case where ``pytest`` won't apply the
``log_cli`` option to loggers which are created at import time.
"""

from pudl.logging_helpers import configure_root_logger

configure_root_logger()
