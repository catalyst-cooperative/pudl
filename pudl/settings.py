"""
Store and make available global settings pertaining to the entire PUDL project.

This module specifies the location of the data stores for the various sources
we rely upon to populate the PUDL DB. It also describes the database connection
parameters.
"""
from pudl import __file__ as pudl_pkg_file
import os.path

# ALL_CAPS indicates global variables that pertain to the entire project.
PUDL_DIR = os.path.dirname(os.path.dirname(pudl_pkg_file))
DATA_DIR = os.path.join(PUDL_DIR, 'data')
FERC1_DATA_DIR = os.path.join(DATA_DIR, 'ferc', 'form1')
EIA923_DATA_DIR = os.path.join(DATA_DIR, 'eia', 'form923')
EIA860_DATA_DIR = os.path.join(DATA_DIR, 'eia', 'form860')
TEST_DIR = os.path.join(PUDL_DIR, 'test')
DOCS_DIR = os.path.join(PUDL_DIR, 'docs')

# These DB connection dictionaries are used by sqlalchemy.URL()
# (Using 127.0.0.1, the numeric equivalent of localhost, to make postgres use
# the `.pgpass` file without fussing around in the config.)
# sqlalchemy.URL will make a URL missing post (therefore using the default),
# and missing a password (which will make the system look for .pgpass)
DB_FERC1 = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'ferc1'
}

DB_PUDL = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl'
}

DB_FERC1_TEST = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'ferc1_test'
}

DB_PUDL_TEST = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl_test'
}
