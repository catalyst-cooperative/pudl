"""Modules providing programmatic analyses that make use of PUDL data.

The :mod:`pudl.analysis` subpackage is a collection of modules which implement various
systematic analyses using the data compiled by PUDL. Over time this should grow into a
rich library of tools that show how the data can be put to use. We may also generate
post-ETL derived database tables for distribution at some point.
"""
from . import (  # noqa: F401
    allocate_net_gen,
    epacamd_eia,
    mcoe,
    plant_parts_eia,
    service_territory,
    spatial,
    state_demand,
    timeseries_cleaning,
)
