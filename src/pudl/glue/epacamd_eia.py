"""Extract, clean, and normalize the EPACAMD-EIA crosswalk.

This module defines functions that read the raw EPACAMD-EIA crosswalk file and clean up
the column names.

The crosswalk file was a joint effort on behalf on EPA and EIA and is published on the
EPA's github account at www.github.com/USEPA/camd-eia-crosswalk". It's a work in
progress and worth noting that, at present, only pulls from 2018 data.
"""

import pudl
import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)
