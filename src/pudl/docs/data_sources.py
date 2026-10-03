"""Generate the data source pages."""

from pudl import PUDL_DOCS_PATH
from pudl.metadata.classes import PUDL_PACKAGE, DataSource, Resource
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

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


def data_sources_metadata_to_rst(app):
    """Export data source metadata to RST for inclusion in the documentation."""
    print("Exporting data source metadata to RST.")
    package = PUDL_PACKAGE
    extra_etl_groups = {
        "eia860": ["entity_eia"],
        "ferc1": ["glue"],
        "epacamd_eia": ["glue"],
    }
    datastore = Datastore(local_cache_path=PudlPaths().pudl_input)
    for name in INCLUDED_SOURCES:
        source = DataSource.from_id(name)
        source_resources = [res for res in package.resources if res.etl_group == name]
        extra_resources: list[Resource] = []
        if name in extra_etl_groups:
            # get resources for this source from extra etl groups
            extra_resources = [
                res
                for res in package.resources
                if res.etl_group in extra_etl_groups[name]
                and name in [src.name for src in res.sources]
            ]
        source.to_rst(
            docs_dir=PUDL_DOCS_PATH,
            output_path=str(PUDL_DOCS_PATH / f"data_sources/{name}.rst"),
            source_resources=source_resources,
            extra_resources=extra_resources,
            datastore=datastore,
        )
