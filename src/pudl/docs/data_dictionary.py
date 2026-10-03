"""Generate the PUDL data dictionary pages."""

from pathlib import Path

import pudl.logging_helpers
from pudl import PUDL_DOCS_PATH
from pudl.docs.templates import get_environment
from pudl.metadata.classes import CodeMetadata, Package
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.resources import RESOURCE_METADATA
from pudl.metadata.warnings import USAGE_WARNINGS

logger = pudl.logging_helpers.get_logger(__name__)

# Tables that are intentionally left out of the data dictionary.
SKIPPED_TABLES = ["datasets", "accumulated_depreciation_ferc1"]

# Paths relative to the docs directory. The Sphinx csv-table directive wants a path
# relative to the source directory, but pandas to_csv wants a true absolute path.
DATA_DICTIONARY_RST = "data_dictionaries/pudl_db.rst"
CODES_RST = "data_dictionaries/codes_and_labels.rst"
CODES_CSV_SUBDIR = Path("data_dictionaries/code_csvs")
USAGE_WARNINGS_RST = "data_dictionaries/usage_warnings.rst"


def data_dictionary_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Write the PUDL database data dictionary page as RST.

    Builds a :class:`~pudl.metadata.classes.Package` from every table in
    ``RESOURCE_METADATA`` except those in :data:`SKIPPED_TABLES`, sorts the
    fields of each table by name, and renders the package with its ``to_rst``
    method into :data:`DATA_DICTIONARY_RST`. An existing file is overwritten.

    Args:
        docs_dir: The documentation source directory. It must contain the
            templates that ``Package.to_rst`` renders and an existing
            ``data_dictionaries/`` output directory.

    Raises:
        FileNotFoundError: If the ``data_dictionaries/`` output directory is
            missing.
        jinja2.TemplateNotFound: If a required template is missing from
            ``docs_dir``.
    """
    logger.info("Exporting PUDL DB data dictionary metadata to RST.")
    names = [name for name in RESOURCE_METADATA if name not in SKIPPED_TABLES]
    package = Package.from_resource_ids(resource_ids=tuple(sorted(names)))
    # Sort fields within each resource by name:
    for resource in package.resources:
        resource.schema.fields = sorted(resource.schema.fields, key=lambda x: x.name)
    package.to_rst(docs_dir=docs_dir, path=str(docs_dir / DATA_DICTIONARY_RST))


def codes_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Write the code and label tables as RST, with a CSV file for each table.

    Renders every table in ``CODE_METADATA`` into :data:`CODES_RST`. Each table
    is also written as a CSV file under :data:`CODES_CSV_SUBDIR`, which the RST
    pulls in with ``csv-table`` directives. The CSV directory is created if it
    doesn't exist. It's removed again by
    :func:`pudl.docs.build.remove_generated_files`.

    Args:
        docs_dir: The documentation source directory. It must contain the
            templates that ``CodeMetadata.to_rst`` renders and an existing
            ``data_dictionaries/`` output directory.

    Raises:
        FileNotFoundError: If the ``data_dictionaries/`` output directory is
            missing.
        jinja2.TemplateNotFound: If a required template is missing from
            ``docs_dir``.
    """
    logger.info("Exporting code and label tables to RST.")
    (docs_dir / CODES_CSV_SUBDIR).mkdir(parents=True, exist_ok=True)
    codemetadata = CodeMetadata.from_code_ids(sorted(CODE_METADATA.keys()))
    codemetadata.to_rst(
        top_dir=docs_dir,
        csv_subdir=CODES_CSV_SUBDIR,
        rst_path=str(docs_dir / CODES_RST),
    )


def usage_warnings_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Write the usage warnings page as RST.

    Lists every warning in :data:`pudl.metadata.warnings.USAGE_WARNINGS` that has a
    title, sorted alphabetically by title, each with its description and any extra
    details. Warnings without a title aren't listed. Each entry gets an RST label
    named for its key, so other pages can link to ``harvested`` and so on. The page
    is written to :data:`USAGE_WARNINGS_RST`, overwriting any existing file.

    Args:
        docs_dir: The documentation source directory. It must contain
            ``usage_warnings.rst.jinja`` under ``templates/`` and an existing
            ``data_dictionaries/`` output directory.

    Raises:
        FileNotFoundError: If the ``data_dictionaries/`` output directory is
            missing.
        jinja2.TemplateNotFound: If the template is missing from ``docs_dir``.
    """
    logger.info("Exporting usage warnings to RST.")
    warnings = sorted(
        (
            {
                "key": key,
                "title": w.title,
                "description": w.description,
                "details": w.details,
            }
            for key, w in USAGE_WARNINGS.items()
            if w.title is not None
        ),
        key=lambda w: w["title"].lower(),
    )
    # autoescape=False: this template produces RST, not HTML, and the warning text
    # is trusted, hand-written RST markup that escaping would corrupt.
    template = get_environment(docs_dir / "templates", autoescape=False).get_template(
        "usage_warnings.rst.jinja"
    )
    (docs_dir / USAGE_WARNINGS_RST).write_text(template.render(warnings=warnings))
