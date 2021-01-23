#!/usr/bin/env python
"""Setup script to make PUDL directly installable with pip."""

import os
from pathlib import Path

from setuptools import find_packages, setup

install_requires = [
    "addfips~=0.3.0",
    "catalystcoop.dbfread~=3.0",
    "coloredlogs~=15.0",
    "contextily~=1.0",
    "datapackage~=1.11",
    "geopandas~=0.8.1",
    "goodtables-pandas-py~=0.2.0",
    "google-cloud-storage~=1.35.0",
    "matplotlib~=3.0",
    "networkx~=2.2",
    "numpy~=1.19",
    "pandas~=1.2",
    "prefect[viz, gcp]~=0.14.2",
    "pyarrow~=2.0",
    "pyyaml~=5.0",
    "scikit-learn~=0.24",
    "scipy~=1.6",
    "seaborn~=0.11.1",
    "sqlalchemy~=1.3",
    "tableschema~=1.12",
    "tableschema-sql~=1.3",
    "timezonefinder~=5.0",
    "tqdm~=4.0",
    "xlsxwriter~=1.3",
]

# We are installing the PUDL module to build the docs, but the C libraries
# required to build snappy aren"t available on RTD, so we need to exclude it
# from the installed dependencies here, and mock it for import in docs/conf.py
# using the autodoc_mock_imports parameter:
if not os.getenv("READTHEDOCS"):
    install_requires.append("python-snappy~=0.5.4")

doc_requires = [
    "doc8~=0.8.0",
    "sphinx~=3.0",
    "sphinx-issues~=1.2",
    "sphinx_rtd_theme~=0.5.0",
]

test_requires = [
    "bandit~=1.6",
    "coverage~=5.3",
    "doc8~=0.8.0",
    "flake8~=3.8",
    "flake8-builtins~=1.5",
    "flake8-colors~=0.1.0",
    "flake8-docstrings~=1.5",
    "flake8-rst-docstrings~=0.0.14",
    "flake8-use-fstring~=1.0",
    "mccabe~=0.6.0",
    "nbval~=0.9",
    "pep8-naming~=0.11.0",
    "pre-commit~=2.9",
    "pydocstyle~=5.1",
    "pytest~=6.2",
    "pytest-cov~=2.10",
    "responses~=0.12.1",
]


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
        "electricity", "energy", "data", "analysis", "mcoe", "climate change",
        "finance", "eia 923", "eia 860", "ferc", "form 1", "epa ampd",
        "epa cems", "coal", "natural gas", "eia 861", "ferc 714"],
    python_requires=">=3.8",
    setup_requires=["setuptools_scm"],
    install_requires=install_requires,
    extras_require={
        "doc": doc_requires,
        "test": test_requires,
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
    ],
    packages=find_packages("src"),
    package_dir={"": "src"},
    # package_data is data that is deployed within the python package on the
    # user"s system. setuptools will get whatever is listed in MANIFEST.in
    include_package_data=True,
    # This defines the interfaces to the command line scripts we"re including:
    entry_points={
        'console_scripts': [
            'pudl_datastore = pudl.workspace.datastore:main',
            'pudl_setup = pudl.workspace.setup_cli:main',
            'pudl_etl = pudl.cli:main',
            'datapkg_to_sqlite = pudl.convert.datapkg_to_sqlite:main',
            'ferc1_to_sqlite = pudl.convert.ferc1_to_sqlite:main',
            'epacems_to_parquet = pudl.convert.epacems_to_parquet:main',
            'pudl_territories = pudl.analysis.service_territory:main',
        ]
    },
)
