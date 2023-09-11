"""Modules implementing the "Extract" step of the PUDL ETL pipeline.

Each module in this subpackage implements data extraction for a single data source from
the PUDL :ref:`data-sources`. This process begins with the original data as retrieved by
the :mod:`pudl.workspace` subpackage, and ends with a dictionary of "raw"
:class:`pandas.DataFrame`s, that have been minimally altered from the original data, and
are ready for normalization and data cleaning by the data source specific modules in the
:mod:`pudl.transform` subpackage.
"""
from . import (
    eia860,
    eia860m,
    eia861,
    eia923,
    eia_bulk_elec,
    epacems,
    excel,
    ferc1,
    ferc714,
    xbrl,
)
