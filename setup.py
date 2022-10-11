#!/usr/bin/env python
"""Setup script to make PUDL directly installable with pip."""

from pathlib import Path

from setuptools import find_packages, setup

readme_path = Path(__file__).parent / "README.rst"
long_description = readme_path.read_text()


setup(
    name="catalystcoop.pudl",
    description="An open data processing pipeline for public US utility data.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    use_scm_version=True,
    author="Catalyst Cooperative",
    author_email="pudl@catalyst.coop",
    maintainer="Zane A. Selvans",
    maintainer_email="zane.selvans@catalyst.coop",
    url="https://catalyst.coop/pudl",
    project_urls={
        "Source": "https://github.com/catalyst-cooperative/pudl",
        "Documentation": "https://catalystcoop-pudl.readthedocs.io",
        "Issue Tracker": "https://github.com/catalyst-cooperative/pudl/issues",
    },
    license="MIT",
    keywords=[
        "electricity",
        "energy",
        "data",
        "analysis",
        "mcoe",
        "climate change",
        "finance",
        "eia 923",
        "eia 860",
        "ferc",
        "form 1",
        "epa ampd",
        "epa cems",
        "coal",
        "natural gas",
        "eia 861",
        "ferc 714",
    ],
    python_requires=">=3.10,<3.11",
    setup_requires=["setuptools_scm"],
    install_requires=[
        "addfips>=0.3.1,<0.4.0",
        "catalystcoop.dbfread>=3.0,<3.1",
        "catalystcoop.ferc-xbrl-extractor==0.4.0",
        "coloredlogs>=15.0,<15.1",
        "dask>=2021.8,<2022.9.3",
        "datapackage>=1.11,<1.16",  # Transition datastore to use frictionless.
        # "email-validator>=1.0.3",  # pydantic[email] dependency
        "fsspec>=2021.7,<2022.8.3",  # For caching datastore on GCS
        "gcsfs>=2021.7,<2022.8.3",  # For caching datastore on GCS
        "geopandas>=0.9,<0.12",
        "jinja2>=2,<3.2",
        "matplotlib>=3.3,<3.7",  # Should make this optional with a "viz" extras
        "networkx>=2.2,<2.9",
        "numpy>=1.18.5,<1.24,!=1.23.0",
        "pandas>=1.4,<1.4.5",
        "pyarrow>=5,<9.1",
        "pydantic[email]>=1.7,<2",
        "python-snappy>=0.6,<0.7",
        "pygeos>=0.10,<0.14",
        "pyyaml>=5,<6.1",
        "scikit-learn>=1.0,<1.2",
        "Shapely!=1.8.3",  # Seems to have a bug or incompatibility
        "scipy>=1.6,<1.10",
        "Shapely!=1.8.3",  # Bug or incompatibility in upstream dependencies
        "sqlalchemy>=1.4,<1.4.42",
        "timezonefinder>=5,<6.2",
        "xlsxwriter>=3,<3.1",
    ],
    extras_require={
        "dev": [
            "black>=22.0,<22.11",
            "docformatter>=1.5,<1.6",
            "ipdb>=0.13,<0.14",
            "isort>=5.0,<5.11",
            "jedi>=0.18,<0.19",
            "lxml>=4.6,<4.10",
            "tox>=3.20,<4.0.0",
            "twine>=3.3,<4.1",
        ],
        "doc": [
            "doc8>=0.9,<1.1",
            "furo>=2022.4.7",
            "sphinx>=4,!=5.1.0,<5.2.4",
            "sphinx-autoapi>=1.8,<2.1",
            "sphinx-issues>=1.2,<3.1",
            "sphinx-reredirects",
            "sphinxcontrib_bibtex>=2.4,<2.6",
        ],
        "test": [
            "bandit>=1.6,<1.8",
            "coverage>=5.3,<6.6",
            "doc8>=0.9,<1.1",
            "flake8>=4.0,<5.1",
            "flake8-builtins>=1.5,<2.1",
            "flake8-docstrings>=1.5,<1.7",
            "flake8-rst-docstrings>=0.2,<0.3",
            "flake8-use-fstring>=1.0,<1.5",
            "mccabe>=0.6,<0.8",
            "nbval>=0.9,<0.10",
            "pep8-naming>=0.12,<0.14",
            "pre-commit>=2.9,<2.21",
            "pydocstyle>=5.1,<6.2",
            "pytest>=6.2,<7.2",
            "pytest-console-scripts>=1.1,<1.4",
            "pytest-cov>=2.10,<4.1",
            "responses>=0.14,<0.22",
            "rstcheck[sphinx]>=5.0,<6.2",
            "tox>=3.20,<3.27",
        ],
        "datasette": [
            "datasette>=0.60,<0.63",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
    ],
    packages=find_packages("src"),
    package_dir={"": "src"},
    # package_data is data that is deployed within the python package on the
    # user"s system. setuptools will get whatever is listed in MANIFEST.in
    include_package_data=True,
    # This defines the interfaces to the command line scripts we"re including:
    entry_points={
        "console_scripts": [
            "censusdp1tract_to_sqlite = pudl.convert.censusdp1tract_to_sqlite:main",
            "metadata_to_rst = pudl.convert.metadata_to_rst:main",
            "epacems_to_parquet = pudl.convert.epacems_to_parquet:main",
            "ferc_to_sqlite = pudl.convert.ferc_to_sqlite:main",
            "datasette_metadata_to_yml = pudl.convert.datasette_metadata_to_yml:main",
            "pudl_datastore = pudl.workspace.datastore:main",
            "pudl_etl = pudl.cli:main",
            "pudl_setup = pudl.workspace.setup_cli:main",
            # Currently blows up memory usage to 100+ GB.
            # See https://github.com/catalyst-cooperative/pudl/issues/1174
            # "pudl_territories = pudl.analysis.service_territory:main",
            "state_demand = pudl.analysis.state_demand:main",
        ]
    },
)
