from pudl import __file__ as pudl_pkg_file
import os.path

# ALL_CAPS indicates global variables that pertain to the entire project.
PUDL_DIR = os.path.dirname(os.path.dirname(pudl_pkg_file))
DATA_DIR = os.path.join(PUDL_DIR, 'data')
FERC1_DATA_DIR = os.path.join(DATA_DIR, 'ferc', 'form1')
EIA923_DATA_DIR = os.path.join(DATA_DIR, 'eia', 'form923')
TEST_DIR = os.path.join(PUDL_DIR, 'test')
DOCS_DIR = os.path.join(PUDL_DIR, 'docs')

# These DB connection dictionaries are used by sqlalchemy.URL()

DB_FERC1 = {
    'drivername': 'postgresql',
    'host': 'localhost',
    'port': '5432',
    'username': 'catalyst',
    'password': '',
    'database': 'ferc1'
}

DB_PUDL = {
    'drivername': 'postgresql',
    'host': 'localhost',
    'port': '5432',
    'username': 'catalyst',
    'password': '',
    'database': 'pudl_sandbox'
}

DB_FERC1_TEST = {
    'drivername': 'postgresql',
    'host': 'localhost',
    'port': '5432',
    'username': 'catalyst',
    'password': '',
    'database': 'ferc1_test'
}
