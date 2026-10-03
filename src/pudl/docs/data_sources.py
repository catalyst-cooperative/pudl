"""Generate the data source pages."""

import sys
from pathlib import Path

import pudl.logging_helpers
from pudl import PUDL_DOCS_PATH
from pudl.docs.templates import get_environment
from pudl.metadata.classes import PUDL_PACKAGE, DataSource, Resource
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

# When adding a new data source add it here and ALSO in pyproject.toml in the
# docs-clean pixi task so generated files are removed.
INCLUDED_SOURCES = [
    "censusdp1tract",
    "censuspep",
    "eiaapi",
    "eia176",
    "eia191",
    "eia860",
    "eia861",
    "eia923",
    "eia930",
    "eiaaeo",
    "ferc1",
    "ferc714",
    "ferceqr",
    "epacems",
    "epacamd_eia",
    "phmsagas",
    "rus12",
    "rus7",
    "sec10k",
    "gridpathratoolkit",
    "nrelatb",
    "vcerare",
]


# Resources from these additional ETL groups are also described on a source's page.
EXTRA_ETL_GROUPS = {
    "eia860": ["entity_eia"],
    "ferc1": ["glue"],
    "epacamd_eia": ["glue"],
}


def data_source_page_paths(docs_dir: Path = PUDL_DOCS_PATH) -> list[Path]:
    """Return the paths of all generated data source pages."""
    return [docs_dir / "data_sources" / f"{name}.rst" for name in INCLUDED_SOURCES]


def data_source_to_rst(
    source: DataSource,
    docs_dir: Path,
    source_resources: list[Resource],
    extra_resources: list[Resource],
    output_path: str | None = None,
    datastore: Datastore | None = None,
) -> None:
    """Output a representation of the data source in RST for documentation."""
    source.add_datastore_metadata(datastore=datastore)
    template = get_environment(docs_dir / "templates").get_template(
        f"{source.name}_child.rst.jinja"
    )
    data_source_dir = docs_dir / "data_sources"
    download_paths = [
        path.relative_to(data_source_dir)
        for path in (
            list((data_source_dir / source.name).glob("*.pdf"))
            + list((data_source_dir / source.name).glob("*.html"))
        )
        if path.is_file()
    ]
    # If PHMSA, also include .txt files in documentation
    if source.name == "phmsagas":
        download_paths += [
            path.relative_to(data_source_dir)
            for path in (list((data_source_dir / source.name).glob("*.txt")))
            if path.is_file()
        ]
    download_paths = sorted(download_paths)
    rendered = template.render(
        source=source,
        source_resources=source_resources,
        extra_resources=extra_resources,
        download_paths=download_paths,
    )
    if output_path:
        Path(output_path).write_text(rendered)
    else:
        sys.stdout.write(rendered)


def data_sources_to_rst(
    docs_dir: Path = PUDL_DOCS_PATH, datastore: Datastore | None = None
) -> None:
    """Export data source metadata to RST for inclusion in the documentation."""
    logger.info("Exporting data source metadata to RST.")
    package = PUDL_PACKAGE
    if datastore is None:
        datastore = Datastore(local_cache_path=PudlPaths().pudl_input)
    for name, output_path in zip(
        INCLUDED_SOURCES, data_source_page_paths(docs_dir), strict=True
    ):
        source = DataSource.from_id(name)
        source_resources = [res for res in package.resources if res.etl_group == name]
        extra_resources: list[Resource] = []
        if name in EXTRA_ETL_GROUPS:
            # get resources for this source from extra etl groups
            extra_resources = [
                res
                for res in package.resources
                if res.etl_group in EXTRA_ETL_GROUPS[name]
                and name in [src.name for src in res.sources]
            ]
        data_source_to_rst(
            source,
            docs_dir=docs_dir,
            source_resources=source_resources,
            extra_resources=extra_resources,
            output_path=str(output_path),
            datastore=datastore,
        )
