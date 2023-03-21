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
        "addfips>=0.4,<0.5",
        "catalystcoop.dbfread>=3.0,<3.1",
        # "catalystcoop.ferc-xbrl-extractor==0.8.1",
        "catalystcoop-ferc_xbrl_extractor @ git+https://github.com/catalyst-cooperative/ferc-xbrl-extractor@pandas-2.0",
        "coloredlogs>=15.0,<15.1",
        "dask>=2021.8,<2023.3.2",
        "datapackage>=1.11,<1.16",  # Transition datastore to use frictionless.
        # "email-validator>=1.0.3",  # pydantic[email] dependency
        "fsspec>=2021.7,<2023.3.1",  # For caching datastore on GCS
        "gcsfs>=2021.7,<2023.3.1",  # For caching datastore on GCS
        "geopandas>=0.9,<0.13",
        "jinja2>=2,<3.2",
        "matplotlib>=3.3,<3.8",  # Should make this optional with a "viz" extras
        "networkx>=2.2,<3.1",
        "numpy>=1.18.5,!=1.23.0,<1.25",
        "pandas==2.0.0rc1",
        "pyarrow>=5,<11.1",
        "pydantic[email]>=1.7,<2",
        "python-snappy>=0.6,<0.7",
        "pyyaml>=5,<6.1",
        "recordlinkage>=0.14,<0.16",
        "scikit-learn>=1.0,<1.3",
        "scipy>=1.6,<1.11",
        "Shapely>=2.0,<2.1",
        "sqlalchemy>=1.4,<2",
        "timezonefinder>=5,<6.2",
        "xlsxwriter>=3,<3.1",
    ],
    extras_require={
        "dev": [
            "black>=22.0,<23.2",
            "docformatter>=1.5,<1.6",
            "isort>=5.0,<5.13",
            "jedi>=0.18,<0.19",
            "lxml>=4.6,<4.10",
            "tox>=3.20,<4.5",
            "twine>=3.3,<4.1",
        ],
        "doc": [
            "doc8>=0.9,<1.2",
            "furo>=2022.4.7",
            "sphinx>=4,!=5.1.0,<6.1.4",
            "sphinx-autoapi>=1.8,<2.1",
            "sphinx-issues>=1.2,<3.1",
            "sphinx-reredirects",
            "sphinxcontrib_bibtex>=2.4,<2.6",
        ],
        "test": [
            "bandit>=1.6,<1.8",
            "coverage>=5.3,<7.3",
            "doc8>=0.9,<1.2",
            "flake8>=4.0,<6.1",
            "flake8-builtins>=1.5,<2.2",
            "flake8-docstrings>=1.5,<1.8",
            "flake8-rst-docstrings>=0.2,<0.4",
            "flake8-use-fstring>=1.0,<1.5",
            "mccabe>=0.6,<0.8",
            "nbval>=0.9,<0.11",
            "pep8-naming>=0.12,<0.14",
            "pre-commit>=2.9,<3.3",
            "pydocstyle>=5.1,<6.4",
            "pytest>=6.2,<7.3",
            "pytest-console-scripts>=1.1,<1.4",
            "pytest-cov>=2.10,<4.1",
            "pytest-mock>=3.0,<3.11",
            "responses>=0.14,<0.24",
            "rstcheck[sphinx]>=5.0,<6.2",
            "tox>=3.20,<4.5",
        ],
        "datasette": [
            "datasette>=0.60,<0.65",
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
