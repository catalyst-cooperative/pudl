#!/usr/bin/env python
"""Setup script to make PUDL directly installable with pip."""

from setuptools import setup, find_packages

setup(
    name='pudl',
    description='Tools for liberating public US electric utility data.',
    use_scm_version=True,
    author='Catalyst Cooperative',
    author_email='pudl@catalyst.coop',
    maintainer='Zane A. Selvans',
    maintainer_email='zane.selvans@catalyst.coop',
    url='https://github.com/catalyst-cooperative/pudl',
    project_urls={
        "Background": "https://catalyst.coop/pudl",
        "Documentation": "https://catalyst-cooperative-pudl.readthedocs.io",
        "Source": "https://github.com/catalyst-cooperative/pudl",
        "Issue Tracker": "https://github.com/catalyst-cooperative/pudl/issues",
        "Gitter Chat": "https://gitter.im/catalyst-cooperative/pudl",
        "Slack": "https://catalystcooperative.slack.com",
    },
    license='MIT',
    keywords=[
        'electricity', 'energy', 'data', 'analysis', 'mcoe', 'climate change',
        'finance', 'eia 923', 'eia 860', 'ferc', 'form 1', 'epa ampd',
        'epa cems', 'coal', 'natural gas', ],
    python_requires='>=3.6, <4',
    setup_requires=['setuptools_scm'],
    install_requires=[
        'datapackage',
        'dbfread',
        'fastparquet',
        'goodtables',
        'jupyter',
        'jupyterlab',
        'matplotlib',
        'nbval',
        'networkx',
        'numpy',
        'pandas>=0.21',
        'psycopg2',
        'pyarrow',
        'pyyaml',
        'scikit-learn>=0.20',
        'scipy',
        'sqlalchemy>=1.3',
        'sqlalchemy-postgres-copy',
        'tableschema',
        'timezonefinder',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    # package_data is data that is deployed within the python package on the
    # user's system. setuptools will get whatever is listed in MANIFEST.in
    include_package_data=True,
    # The "right way" to deploy scripts so that they work on Windows as well is
    # with entry_points and console_scripts, but that will require some
    # additional re-organization. See issue #327:
    # https://github.com/catalyst-cooperative/pudl/issues/327
    scripts=[
        'scripts/update_datastore.py',
        'scripts/ferc1_to_sqlite.py',
        'scripts/init_pudl.py',
        'scripts/epacems_to_parquet.py',
    ],
)
