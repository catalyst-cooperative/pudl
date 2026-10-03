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
    """Generate all dynamic documentation content under ``docs_dir``.

    Runs, in order, the data dictionary, data source, Citations & Media and code
    table generators. Everything written here is listed by
    :func:`generated_files` (plus the CSV directory removed by
    :func:`remove_generated_files`), so a new generator must be added to both
    this function and that registry. Existing generated files are overwritten.

    Args:
        docs_dir: The documentation source directory.

    Raises:
        FileNotFoundError: If an output directory (``data_dictionaries/``,
            ``data_sources/`` or ``citations_media/``) or an input .bib file is
            missing.
        jinja2.TemplateNotFound: If a required template is missing.
    """
    data_dictionary_to_rst(docs_dir)
    data_sources_to_rst(docs_dir)
    citations_media_to_rst(docs_dir)
    codes_to_rst(docs_dir)


def generated_files(docs_dir: Path = PUDL_DOCS_PATH) -> list[Path]:
    """List every RST file that :func:`generate_all` writes under ``docs_dir``.

    This registry drives cleanup, and it replaces maintaining a separate list of
    generated files by hand. It doesn't include the directory of CSV files
    written by :func:`~pudl.docs.data_dictionary.codes_to_rst`, which
    :func:`remove_generated_files` handles separately.

    Args:
        docs_dir: The documentation source directory.

    Returns:
        Paths of the data dictionary, code tables, data source and Citations &
        Media pages. The files need not exist.
    """
    return [
        docs_dir / DATA_DICTIONARY_RST,
        docs_dir / CODES_RST,
        *data_source_page_paths(docs_dir),
        *citations_media_page_paths(docs_dir),
    ]


def remove_generated_files(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Remove everything :func:`generate_all` creates under ``docs_dir``.

    Deletes each file from :func:`generated_files` and the directory of code
    table CSV files. Hand-written files are left alone. Anything already absent
    is skipped, so this is safe to call on a clean tree and more than once.

    Args:
        docs_dir: The documentation source directory.
    """
    for path in generated_files(docs_dir):
        path.unlink(missing_ok=True)
    shutil.rmtree(docs_dir / CODES_CSV_SUBDIR, ignore_errors=True)
