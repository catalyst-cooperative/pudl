#!/usr/bin/env python
"""Setup script to make PUDL directly installable with pip."""

from setuptools import setup, find_packages

setup(
    name='pudl',
    description='Tools for liberating public US electric utility data.',
    version='0.1.0',
    author='Catalyst Cooperative',
    author_email='pudl@catalyst.coop',
    url='https://github.com/catalyst-cooperative/pudl',
    packages=find_packages(where='.', include=['pudl*']),
    scripts=[
        'scripts/init_pudl.py',
        'scripts/update_datastore.py',
        'scripts/epacems_to_parquet.py',
    ],
    include_package_data=True,
    python_requires='~=3.6',
    install_requires=[
        'datapackage',
        'dbfread',
        'fastparquet',
        'goodtables',
        'networkx',
        'numpy',
        'pandas>=0.21',
        'psycopg2',
        'pyarrow',
        'python-snappy',
        'pyyaml',
        'scikit-learn>=0.20',
        'scipy',
        'sqlalchemy',
        'sqlalchemy-postgres-copy',
        'tableschema',
        'xlsxwriter',
    ],
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
