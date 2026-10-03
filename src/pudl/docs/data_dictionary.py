"""Generate the PUDL data dictionary pages."""

from pudl import PUDL_DOCS_PATH
from pudl.metadata.classes import CodeMetadata, Package
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.resources import RESOURCE_METADATA


def data_dictionary_metadata_to_rst(app):
    """Export data dictionary metadata to RST for inclusion in the documentation."""
    # Create an RST Data Dictionary for the PUDL DB:
    print("Exporting PUDL DB data dictionary metadata to RST.")
    skip_names = ["datasets", "accumulated_depreciation_ferc1"]
    names = [name for name in RESOURCE_METADATA if name not in skip_names]
    package = Package.from_resource_ids(resource_ids=tuple(sorted(names)))
    # Sort fields within each resource by name:
    for resource in package.resources:
        resource.schema.fields = sorted(resource.schema.fields, key=lambda x: x.name)
    package.to_rst(
        docs_dir=PUDL_DOCS_PATH,
        path=str(PUDL_DOCS_PATH / "data_dictionaries/pudl_db.rst"),
    )


def static_dfs_to_rst(app):
    """Export static code labeling dataframes to RST for inclusion in documentation."""
    # Sphinx csv-table directive wants an absolute path relative to source directory,
    # but pandas to_csv wants a true absolute path
    csv_subdir = "data_dictionaries/code_csvs"
    abs_csv_dir_path = PUDL_DOCS_PATH / csv_subdir
    abs_csv_dir_path.mkdir(parents=True, exist_ok=True)
    codemetadata = CodeMetadata.from_code_ids(sorted(CODE_METADATA.keys()))
    codemetadata.to_rst(
        top_dir=PUDL_DOCS_PATH,
        csv_subdir=csv_subdir,
        rst_path=str(PUDL_DOCS_PATH / "data_dictionaries/codes_and_labels.rst"),
    )
