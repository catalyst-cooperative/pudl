"""
This module opens the YAML configuration file.

Whenever using any of the settings in the configuration file in a module,
import this module as `from config import settings`
"""

import os.path
import pathlib
import yaml


def get_user_settings(settings_file=None):
    if settings_file is None:
        settings_file = os.path.join(pathlib.Path.home(), '.pudl.yml')

    if not os.path.exists(settings_file):
        raise FileNotFoundError(settings_file)

    with open(settings_file, "r") as f:
        user_settings = yaml.safe_load(f)

    if not os.path.isdir(user_settings['input_dir']):
        raise FileNotFoundError(user_settings['input_dir'])
    if not os.path.isdir(user_settings['output_dir']):
        raise FileNotFoundError(user_settings['output_dir'])

    return user_settings


SETTINGS = {}
# ALL_CAPS indicates global variables that pertain to the entire project.
SETTINGS['data_dir'] = os.path.join(SETTINGS['pudl_indir'], 'data')
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
