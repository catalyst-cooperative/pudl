"""Configuration file for the Sphinx documentation builder."""
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import datetime
import shutil
from pathlib import Path

import pkg_resources

from pudl.metadata.classes import CodeMetadata, DataSource, Package
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.resources import RESOURCE_METADATA

DOCS_DIR = Path(__file__).parent.resolve()

# -- Path setup --------------------------------------------------------------
# We are building and installing the pudl package in order to get access to
# the distribution metadata, including an automatically generated version
# number via pkg_resources.get_distribution() so we need more than just an
# importable path.

# The full version, including alpha/beta/rc tags
release = pkg_resources.get_distribution("catalystcoop.pudl").version

# -- Project information -----------------------------------------------------

project = "PUDL"
copyright = (  # noqa: A001
    f"2016-{datetime.date.today().year}, Catalyst Cooperative, CC-BY-4.0"
)
author = "Catalyst Cooperative"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "autoapi.extension",
    "sphinx_issues",
    "sphinx_reredirects",
    "sphinxcontrib.bibtex",
]
todo_include_todos = True
bibtex_bibfiles = [
    "catalyst_pubs.bib",
    "catalyst_cites.bib",
    "further_reading.bib",
]

# Redirects to keep folks from hitting 404 errors:
redirects = {
    "data_dictionary": "data_dictionaries/pudl_db.html",
}

# Automatically generate API documentation during the doc build:
autoapi_type = "python"
autoapi_dirs = [
    "../src/pudl",
]
autoapi_ignore = [
    "*_test.py",
]

# GitHub repo
issues_github_path = "catalyst-cooperative/pudl"

# In order to be able to link directly to documentation for other projects,
# we need to define these package to URL mappings:
intersphinx_mapping = {
    "arrow": ("https://arrow.apache.org/docs/", None),
    "dask": ("https://docs.dask.org/en/latest/", None),
    "geopandas": ("https://geopandas.org/en/stable/", None),
    "networkx": ("https://networkx.org/documentation/stable/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "pytest": ("https://docs.pytest.org/en/latest/", None),
    "python": ("https://docs.python.org/3", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "setuptools": ("https://setuptools.pypa.io/en/latest/", None),
    "sklearn": ("https://scikit-learn.org/stable", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/latest/", None),
    "tox": ("https://tox.wiki/en/latest/", None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
master_doc = "index"
html_theme = "furo"
html_logo = "_static/catalyst_logo-200x200.png"
html_icon = "_static/favicon.ico"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "navigation_with_keys": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# -- Custom build operations -------------------------------------------------
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
    package.to_rst(docs_dir=DOCS_DIR, path=DOCS_DIR / "data_dictionaries/pudl_db.rst")


def data_sources_metadata_to_rst(app):
    """Export data source metadata to RST for inclusion in the documentation."""
    print("Exporting data source metadata to RST.")
    included_sources = ["eia860", "eia861", "eia923", "ferc1", "ferc714", "epacems"]
    package = Package.from_resource_ids()
    extra_etl_groups = {"eia860": ["entity_eia"], "ferc1": ["glue"]}
    for name in included_sources:
        source = DataSource.from_id(name)
        source_resources = [res for res in package.resources if res.etl_group == name]
        extra_resources = None
        if name in extra_etl_groups:
            # get resources for this source from extra etl groups
            extra_resources = [
                res
                for res in package.resources
                if res.etl_group in extra_etl_groups[name]
                and name in [src.name for src in res.sources]
            ]
        source.to_rst(
            docs_dir=DOCS_DIR,
            output_path=DOCS_DIR / f"data_sources/{name}.rst",
            source_resources=source_resources,
            extra_resources=extra_resources,
        )


def static_dfs_to_rst(app):
    """Export static code labeling dataframes to RST for inclusion in documentation."""
    # Sphinx csv-table directive wants an absolute path relative to source directory,
    # but pandas to_csv wants a true absolute path
    csv_subdir = "data_dictionaries/code_csvs"
    abs_csv_dir_path = DOCS_DIR / csv_subdir
    abs_csv_dir_path.mkdir(parents=True, exist_ok=True)
    codemetadata = CodeMetadata.from_code_ids(sorted(CODE_METADATA.keys()))
    codemetadata.to_rst(
        top_dir=DOCS_DIR,
        csv_subdir=csv_subdir,
        rst_path=DOCS_DIR / "data_dictionaries/codes_and_labels.rst",
    )


def cleanup_rsts(app, exception):
    """Remove generated RST files when the build is finished."""
    (DOCS_DIR / "data_dictionaries/pudl_db.rst").unlink()
    (DOCS_DIR / "data_dictionaries/codes_and_labels.rst").unlink()
    (DOCS_DIR / "data_sources/eia860.rst").unlink()
    (DOCS_DIR / "data_sources/eia861.rst").unlink()
    (DOCS_DIR / "data_sources/eia923.rst").unlink()
    (DOCS_DIR / "data_sources/ferc1.rst").unlink()
    (DOCS_DIR / "data_sources/ferc714.rst").unlink()
    (DOCS_DIR / "data_sources/epacems.rst").unlink()


def cleanup_csv_dir(app, exception):
    """Remove generated CSV files when the build is finished."""
    csv_dir = DOCS_DIR / "data_dictionaries/code_csvs"
    if csv_dir.exists() and csv_dir.is_dir():
        shutil.rmtree(csv_dir)


def setup(app):
    """Add custom CSS defined in _static/custom.css."""
    app.add_css_file("custom.css")
    app.connect("builder-inited", data_dictionary_metadata_to_rst)
    app.connect("builder-inited", data_sources_metadata_to_rst)
    app.connect("builder-inited", static_dfs_to_rst)
    app.connect("build-finished", cleanup_rsts)
    app.connect("build-finished", cleanup_csv_dir)
