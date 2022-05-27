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
    python_requires=">=3.8,<3.11",
    setup_requires=["setuptools_scm"],
    install_requires=[
        "addfips~=0.3.1",
        "catalystcoop.dbfread~=3.0",
        "coloredlogs~=15.0",
        "dask>=2021.8,<2022.5.3",
        "datapackage~=1.11",  # Transition datastore to use frictionless.
        # "email-validator>=1.0.3",  # pydantic[email] dependency
        "fsspec>=2021.7,<2022.5.1",  # For caching datastore on GCS
        "gcsfs>=2021.7,<2022.5.1",  # For caching datastore on GCS
        "geopandas>=0.9,<0.11",
        "jinja2>=2,<4",
        "matplotlib>=3.3,<4",  # Should make this optional with a "viz" extras
        "networkx>=2.2,<3",
        "numpy>=1.18.5,<2",
        "pandas>=1.4,<1.5",
        "pyarrow>=5,<9",
        "pydantic[email]>=1.7,<2",
        "python-snappy>=0.6,<1",
        "pygeos>=0.10,<0.13",
        "pyyaml>=5,<7",
        "scikit-learn>=1.0,<1.2",
        "scipy>=1.6,<1.9",
        "sqlalchemy>=1.4,<2",
        "timezonefinder>=5,<7",
        "xlsxwriter>=3,<4",
    ],
    extras_require={
        "dev": [
            "black>=22,<23",
            "ipdb~=0.13.4",
            "isort~=5.0",
            "jedi~=0.18",
            "lxml~=4.6",
            "tox~=3.20",
            "twine>=3.3,<5.0",
        ],
        "doc": [
            "doc8~=0.9",
            "sphinx~=4.0",
            "sphinx-autoapi~=1.8",
            "sphinx-issues>=1.2,<4.0",
            "sphinx-reredirects",
            "sphinx-rtd-dark-mode~=1.2",
            "sphinx-rtd-theme~=1.0",
            "sphinxcontrib_bibtex~=2.4",
        ],
        "test": [
            "bandit~=1.6",
            "coverage>=5.3,<7.0",
            "doc8~=0.9",
            "flake8~=4.0",
            "flake8-builtins~=1.5",
            "flake8-colors~=0.1",
            "flake8-docstrings~=1.5",
            "flake8-rst-docstrings~=0.2",
            "flake8-use-fstring~=1.0",
            "mccabe~=0.6",
            "nbval~=0.9",
            "pep8-naming~=0.12",
            "pre-commit~=2.9",
            "pydocstyle>=5.1,<7.0",
            "pytest>=6.2,<8.0",
            "pytest-console-scripts~=1.1",
            "pytest-cov>=2.10,<4.0",
            "responses~=0.14",
            "rstcheck[sphinx]>=5,<6",
            "tox~=3.20",
        ],
        "datasette": [
            "datasette~=0.60",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
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
            "ferc1_to_sqlite = pudl.convert.ferc1_to_sqlite:main",
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
