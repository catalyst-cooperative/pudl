"""Generate and clean up all dynamically generated documentation files."""

import shutil
from pathlib import Path

from pudl import PUDL_DOCS_PATH
from pudl.docs.citations import citations_media_page_paths, citations_media_to_rst
from pudl.docs.data_dictionary import (
    CODES_CSV_SUBDIR,
    CODES_RST,
    DATA_DICTIONARY_RST,
    codes_to_rst,
    data_dictionary_to_rst,
)
from pudl.docs.data_sources import data_source_page_paths, data_sources_to_rst


def generate_all(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Generate all dynamic documentation content under ``docs_dir``."""
    data_dictionary_to_rst(docs_dir)
    data_sources_to_rst(docs_dir)
    citations_media_to_rst(docs_dir)
    codes_to_rst(docs_dir)


def generated_files(docs_dir: Path = PUDL_DOCS_PATH) -> list[Path]:
    """List every file that :func:`generate_all` writes under ``docs_dir``."""
    return [
        docs_dir / DATA_DICTIONARY_RST,
        docs_dir / CODES_RST,
        *data_source_page_paths(docs_dir),
        *citations_media_page_paths(docs_dir),
    ]


def remove_generated_files(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Remove everything :func:`generate_all` creates under ``docs_dir``."""
    for path in generated_files(docs_dir):
        path.unlink(missing_ok=True)
    shutil.rmtree(docs_dir / CODES_CSV_SUBDIR, ignore_errors=True)
