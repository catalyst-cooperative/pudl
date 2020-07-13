# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import pkg_resources

# -- Path setup --------------------------------------------------------------
# We are building and installing the pudl package in order to get access to
# the distribution metadata, including an automatically generated version
# number via pkg_resources.get_distribution() so we need more than just an
# importable path.

# -- Project information -----------------------------------------------------

project = 'PUDL'
copyright = '2020, Catalyst Cooperative'
author = 'Catalyst Cooperative'

# The full version, including alpha/beta/rc tags
release = pkg_resources.get_distribution('catalystcoop.pudl').version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    "sphinx_issues",
]
todo_include_todos = True

# GitHub repo
issues_github_path = "catalyst-cooperative/pudl"

# In order to be able to link directly to documentation for other projects,
# we need to define these package to URL mappings:
intersphinx_mapping = {
    'arrow': ('https://arrow.apache.org/docs/', None),
    'dask': ('https://docs.dask.org/en/latest/', None),
    'geopandas': ('https://geopandas.org/', None),
    'networkx': ('https://networkx.github.io/documentation/stable/', None),
    'numpy': ('https://docs.scipy.org/doc/numpy/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable', None),
    'pytest': ('https://docs.pytest.org/en/latest/', None),
    'python': ('https://docs.python.org/3', None),
    'scipy': ('https://docs.scipy.org/doc/scipy/reference', None),
    'setuptools': ('https://setuptools.readthedocs.io/en/latest/', None),
    'sklearn': ('https://scikit-learn.org/stable', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/latest/', None),
    'tox': ('https://tox.readthedocs.io/en/latest/', None),
}

# List of packages that should not really be installed, because they are
# written in C or have C extensions. Instead they should be mocked for import
# purposes only to prevent the doc build from failing.
autodoc_mock_imports = ['snappy', 'pyarrow']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

master_doc = 'index'
html_theme = 'sphinx_rtd_theme'
html_logo = '_static/catalyst_logo-200x200.png'
html_icon = '_static/favicon.ico'

html_context = {
    "display_github": True,  # Integrate GitHub
    "github_user": "catalyst-cooperative",  # Username
    "github_repo": "pudl",  # Repo name
    "github_version": "master",  # Version
    "conf_py_path": "/docs/",  # Path in the checkout to the docs root
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
