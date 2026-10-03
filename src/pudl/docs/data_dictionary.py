"""Generate the PUDL data dictionary pages."""

from pathlib import Path

import pudl.logging_helpers
from pudl import PUDL_DOCS_PATH
from pudl.docs.templates import get_environment
from pudl.metadata.classes import PUDL_PACKAGE, Encoder, Package
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


def package_to_rst(package: Package, docs_dir: Path, path: str) -> None:
    """Render a data package as the data dictionary page and write it to a file.

    Renders ``package.rst.jinja``, which writes a heading and then, for each
    resource in :meth:`~pudl.metadata.classes.Package.get_sorted_resources`, a
    Sphinx reference label followed by ``resource.rst.jinja`` (including the
    per-table access examples). The caller controls which tables are included
    and how their fields are ordered by what it puts in ``package``.

    Args:
        package: The data package to document.
        docs_dir: The documentation source directory. It must contain
            ``package.rst.jinja``, ``resource.rst.jinja`` and the
            ``access_examples/`` templates they include, under ``templates/``.
        path: File to write the RST to. An existing file is overwritten, and its
            parent directory must already exist.

    Raises:
        FileNotFoundError: If the parent directory of ``path`` doesn't exist.
        jinja2.TemplateNotFound: If a required template is missing from
            ``docs_dir``.
    """
    template = get_environment(docs_dir / "templates").get_template("package.rst.jinja")
    rendered = template.render(package=package)
    Path(path).write_text(rendered)


def data_dictionary_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Write the PUDL database data dictionary page as RST.

    Builds a :class:`~pudl.metadata.classes.Package` from every table in
    ``RESOURCE_METADATA`` except those in :data:`SKIPPED_TABLES`, sorts the
    fields of each table by name, and renders the package with
    :func:`package_to_rst` into :data:`DATA_DICTIONARY_RST`. An existing file is
    overwritten.

    Args:
        docs_dir: The documentation source directory. It must contain the
            templates that :func:`package_to_rst` renders and an existing
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
    package_to_rst(package, docs_dir=docs_dir, path=str(docs_dir / DATA_DICTIONARY_RST))


def encoder_to_rst(
    encoder: Encoder, top_dir: Path, csv_subdir: Path, is_header: bool
) -> str:
    """Render one code table as an RST section and write its CSV file.

    The table's codes, labels and descriptions are written to
    ``<top_dir>/<csv_subdir>/<encoder.name>.csv``, overwriting any existing file.
    The section is rendered from ``codemetadata.rst.jinja``. It embeds that CSV
    with a ``csv-table`` directive, using a path relative to the Sphinx source
    root. If the encoder has non-standard codes that get fixed, they are listed
    in a second table. The section's introduction is the first paragraph of the
    description of the table with the same name in ``PUDL_PACKAGE``.

    Args:
        encoder: The encoder for the code table to document. Its name must be the
            name of a table in ``PUDL_PACKAGE``.
        top_dir: The documentation source directory. It must contain
            ``codemetadata.rst.jinja`` under ``templates/``.
        csv_subdir: Directory for the CSV file, relative to ``top_dir``. It must
            already exist.
        is_header: Whether to start the output with the page title and
            introduction. This should be true only for the first section on a
            page.

    Returns:
        The rendered RST for this table's section.

    Raises:
        FileNotFoundError: If ``top_dir / csv_subdir`` doesn't exist.
        ValueError: If ``encoder.name`` isn't a table in ``PUDL_PACKAGE``.
        jinja2.TemplateNotFound: If the template is missing from ``top_dir``.
    """
    encoder.df.to_csv(Path(top_dir) / csv_subdir / f"{encoder.name}.csv", index=False)
    template = get_environment(top_dir / "templates").get_template(
        "codemetadata.rst.jinja"
    )
    rendered = template.render(
        Encoder=encoder,
        # just get the resolved resource summary & drop all the other sections of the description
        description=PUDL_PACKAGE.get_resource(encoder.name).description.partition(
            "\n\n"
        )[0],
        csv_filepath=(Path("/") / csv_subdir / f"{encoder.name}.csv"),
        is_header=is_header,
    )
    return rendered


def encoders_to_rst(
    encoders: list[Encoder], top_dir: Path, csv_subdir: Path, rst_path: str
) -> None:
    """Render a list of code tables into a single RST page.

    Each encoder is rendered with :func:`encoder_to_rst`, in the order given. The
    page title and introduction are included once, with the first encoder. Each
    encoder's CSV file is also written. An empty list produces an empty file.

    Args:
        encoders: The encoders for the code tables to document.
        top_dir: The documentation source directory. It must contain
            ``codemetadata.rst.jinja`` under ``templates/``.
        csv_subdir: Directory for the CSV files, relative to ``top_dir``. It must
            already exist.
        rst_path: File to write the RST to. An existing file is overwritten.

    Raises:
        FileNotFoundError: If ``top_dir / csv_subdir`` or the parent directory of
            ``rst_path`` doesn't exist.
        ValueError: If an encoder's name isn't a table in ``PUDL_PACKAGE``.
        jinja2.TemplateNotFound: If the template is missing from ``top_dir``.
    """
    with Path(rst_path).open("w") as f:
        for idx, encoder in enumerate(encoders):
            header = idx == 0
            rendered = encoder_to_rst(
                encoder, top_dir=top_dir, csv_subdir=csv_subdir, is_header=header
            )
            f.write(rendered)


def codes_to_rst(docs_dir: Path = PUDL_DOCS_PATH) -> None:
    """Write the code and label tables as RST, with a CSV file for each table.

    Renders every table in ``CODE_METADATA`` into :data:`CODES_RST`. Each table
    is also written as a CSV file under :data:`CODES_CSV_SUBDIR`, which the RST
    pulls in with ``csv-table`` directives. The CSV directory is created if it
    doesn't exist. It's removed again by
    :func:`pudl.docs.build.remove_generated_files`.

    Args:
        docs_dir: The documentation source directory. It must contain the
            templates that :func:`encoders_to_rst` renders and an existing
            ``data_dictionaries/`` output directory.

    Raises:
        FileNotFoundError: If the ``data_dictionaries/`` output directory is
            missing.
        jinja2.TemplateNotFound: If a required template is missing from
            ``docs_dir``.
    """
    logger.info("Exporting code and label tables to RST.")
    (docs_dir / CODES_CSV_SUBDIR).mkdir(parents=True, exist_ok=True)
    encoders = [Encoder.from_code_id(name) for name in sorted(CODE_METADATA)]
    encoders_to_rst(
        encoders,
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
