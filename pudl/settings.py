"""
This module opens the YAML configuration file.

Whenever using any of the settings in the configuration file in a module,
import this module as `from config import settings`
"""

import os.path
import yaml
from pudl import __file__ as pudl_pkg_file

PUDL_DIR = os.path.dirname(os.path.dirname(pudl_pkg_file))


def settings_init(settings_file="settings_init_pudl_default.yml"):
    with open(os.path.join(PUDL_DIR, 'scripts', settings_file), "r") as f:
        settings_out = yaml.safe_load(f)

    return settings_out


SETTINGS = {}
# ALL_CAPS indicates global variables that pertain to the entire project.
SETTINGS['pudl_dir'] = PUDL_DIR
SETTINGS['data_dir'] = os.path.join(SETTINGS['pudl_dir'], 'data')
SETTINGS['ferc1_data_dir'] = os.path.join(
    SETTINGS['data_dir'], 'ferc', 'form1')
SETTINGS['eia923_data_dir'] = os.path.join(
    SETTINGS['data_dir'], 'eia', 'form923')
SETTINGS['eia860_data_dir'] = os.path.join(
    SETTINGS['data_dir'], 'eia', 'form860')
SETTINGS['epacems_data_dir'] = os.path.join(
    SETTINGS['data_dir'], 'epa', 'cems')
SETTINGS['epaipm_data_dir'] = os.path.join(
    SETTINGS['data_dir'], 'epa', 'ipm')
SETTINGS['test_dir'] = os.path.join(SETTINGS['pudl_dir'], 'test')
SETTINGS['docs_dir'] = os.path.join(SETTINGS['pudl_dir'], 'docs')
SETTINGS['csvdir'] = os.path.join(SETTINGS['pudl_dir'], 'results', 'csvdump')

# Location of small data files that are not easy to download (e.g. from a PDF)
SETTINGS['pudl_data_dir'] = os.path.join(SETTINGS['data_dir'], 'pudl')


# These DB connection dictionaries are used by sqlalchemy.URL()
# (Using 127.0.0.1, the numeric equivalent of localhost, to make postgres use
# the `.pgpass` file without fussing around in the config.)
# sqlalchemy.URL will make a URL missing post (therefore using the default),
# and missing a password (which will make the system look for .pgpass)
SETTINGS['db_pudl'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl'
}

SETTINGS['db_pudl_test'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl_test'
}

SETTINGS['ferc1_sqlite_url'] = "sqlite:///" + os.path.join(
    SETTINGS['pudl_dir'], 'results', 'sqlite', 'ferc1.sqlite'
)

SETTINGS['ferc1_test_sqlite_url'] = "sqlite:///" + os.path.join(
    SETTINGS['pudl_dir'], 'results', 'sqlite', 'ferc1_test.sqlite'
)
