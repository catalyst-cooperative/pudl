"""Tools for converting datasets between various formats in bulk.

It's often useful to be able to convert entire datasets in bulk from one format
to another, both independent of and within the context of the ETL pipeline.
This subpackage collects those tools together in one place.
"""

from . import (
    censusdp1tract_to_sqlite,
    metadata_to_rst,
)
