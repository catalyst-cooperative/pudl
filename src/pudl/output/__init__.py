"""Useful post-processing and denormalized outputs based on PUDL core tables.

The core PUDL database tables are well normalized. This minimizes data duplication and
helps avoid many kinds of data corruption and the potential for internal inconsistency.
However, normalized tables aren't always the easiest for humans to work with. For
readability and analysis, we often want all the names and IDs and related values in a
single "denormalized" table.

This subpackage compiles a bunch of derived output tables that we've found to be useful,
so that they can accessed in a uniform way rather than being reconstructed on the fly.
"""

from . import (
    censusdp1tract,
    eia,
    eia860,
    eia923,
    eia930,
    eiaapi,
    epacems,
    ferc1,
    ferc714,
    sec10k,
)
