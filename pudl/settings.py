"""
This module opens the YAML configuration file.

Whenever using any of the settings in the configuration file in a module,
import this module as `from config import settings`
"""

import os.path
import yaml
from pudl import __file__ as pudl_pkg_file

PUDL_DIR = os.path.dirname(os.path.dirname(pudl_pkg_file))


def settings_init(settings_file="settings.yml"):
    with open(os.path.join(PUDL_DIR, 'scripts', settings_file), "r") as f:
        settings_out = yaml.safe_load(f)

    # if the refyear is set to none, but there are ferc1_years, set the refyear
    # to be the max of ferc1_years
    if settings_out['ferc1_years']:
        if not settings_out['ferc1_ref_year']:
            settings_out['ferc1_ref_year'] = max(settings_out['ferc1_years'])

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
SETTINGS['test_dir'] = os.path.join(SETTINGS['pudl_dir'], 'test')
SETTINGS['docs_dir'] = os.path.join(SETTINGS['pudl_dir'], 'docs')
SETTINGS['csvdir'] = os.path.join(SETTINGS['pudl_dir'], 'results', 'csvdump')


# These DB connection dictionaries are used by sqlalchemy.URL()
# (Using 127.0.0.1, the numeric equivalent of localhost, to make postgres use
# the `.pgpass` file without fussing around in the config.)
# sqlalchemy.URL will make a URL missing post (therefore using the default),
# and missing a password (which will make the system look for .pgpass)
SETTINGS['db_ferc1'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'ferc1'
}

SETTINGS['db_pudl'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl'
}

SETTINGS['db_ferc1_test'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'ferc1_test'
}

SETTINGS['db_pudl_test'] = {
    'drivername': 'postgresql',
    'host': '127.0.0.1',
    'username': 'catalyst',
    'database': 'pudl_test'
}
