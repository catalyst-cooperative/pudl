"""Tools for converting datasets between various formats in bulk.

It's often useful to be able to convert entire datasets in bulk from one format
to another, both independent of and within the context of the ETL pipeline.
This subpackage collects those tools together in one place.

Currently the tools use a mix of idioms, referring either to a particular
dataset and a particular format, or two formats. Some of them read from the
original raw data as organized by the :mod:`pudl.workspace` package (e.g.
:mod:`pudl.convert.ferc_to_sqlite` or :mod:`pudl.convert.epacems_to_parquet`),
and others convert metadata into RST for use in documentation
(e.g. :mod:`pudl.convert.metadata_to_rst`).
"""
from . import (  # noqa: F401
    censusdp1tract_to_sqlite,
    datasette_metadata_to_yml,
    epacems_to_parquet,
    metadata_to_rst,
)
