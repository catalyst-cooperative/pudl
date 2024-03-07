"""Hooks to integrate ferc to sqlite functionality into dagster graph."""

import pudl
from pudl.extract.ferc1 import Ferc1DbfExtractor
from pudl.extract.ferc2 import Ferc2DbfExtractor
from pudl.extract.ferc6 import Ferc6DbfExtractor
from pudl.extract.ferc60 import Ferc60DbfExtractor

logger = pudl.logging_helpers.get_logger(__name__)

ALL_DBF_EXTRACTORS = [
    Ferc1DbfExtractor,
    Ferc2DbfExtractor,
    Ferc6DbfExtractor,
    Ferc60DbfExtractor,
]
