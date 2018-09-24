"""
This module opens the YAML configuration file.

Whenever using any of the settings in the configuration file in a module,
import this module as `from config import settings`
"""

import yaml
from pudl import __file__ as pudl_pkg_file
import os.path

PUDL_DIR = os.path.dirname(os.path.dirname(pudl_pkg_file))
with open(os.path.join(PUDL_DIR, "settings.yml"), "r") as f:
    SETTINGS = yaml.load(f)


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

SETTINGS['csvdir'] = os.path.join(SETTINGS['pudl_dir'],
                                  'results', SETTINGS['csvdir'])
