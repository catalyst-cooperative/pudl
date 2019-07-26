"""
This module opens the YAML configuration file.

Whenever using any of the settings in the configuration file in a module,
import this module as `from config import settings`
"""

import importlib
import logging
import os
import os.path
import pathlib
import shutil

import yaml

import pudl.constants as pc
import pudl.datastore.datastore as datastore

logger = logging.getLogger(__name__)


def read_script_settings(settings_file=None):
    """Read in generic YAML settings for one of the PUDL scripts.

    Args:
        settings_file (path-like): A string or path like object that can be
            used to open a file containing the configuration. Defaults to
            $HOME/.pudl.yml

    Returns:

    Todo:
        Return to (Returns type)
    """
    if settings_file is None:
        settings_file = pathlib.Path.home() / '.pudl.yml'

    # If we don't get a settings file, then read in the extremely basic
    # user settings file, which determines PUDL_IN and PUDL_OUT, and can
    # be used to generate the rest of the pudl_settings paths.
    with pathlib.Path(settings_file).open() as f:
        script_settings = yaml.safe_load(f)

    # Give pudl_in and pudl_out from settings file priority, if present.
    # Otherwise fall back to user default pudl config file
    try:
        script_settings['pudl_in'] = script_settings['pudl_in']
    except KeyError:
        script_settings['pudl_in'] = read_user_settings()['pudl_in']

    try:
        script_settings['pudl_out'] = script_settings['pudl_out']
    except KeyError:
        script_settings['pudl_out'] = read_user_settings()['pudl_out']

    return script_settings


def read_user_settings(settings_file=None):
    """Read the most basic PUDL settings from a user supplied file.

    Args:
        settings_file (path-like): A string or path like object that can be
            used to open a file containing the configuration. Defaults to
            $HOME/.pudl.yml

    Returns:

    Todo:
        Return to (Returns type)
    """
    if settings_file is None:
        settings_file = pathlib.Path.home() / '.pudl.yml'

    with pathlib.Path(settings_file).open() as f:
        user_settings = yaml.safe_load(f)

    return user_settings


def grab_package_settings(pudl_settings, settings_file):
    with open(os.path.join(pudl_settings['settings_dir'], settings_file), "r") as f:
        pkg_settings = yaml.safe_load(f)
    return pkg_settings


def init(pudl_in=None, pudl_out=None, settings_file=None):
    """Generates commonly used PUDL settings based on a user settings file.

    If no configuration file path is provided, attempt to read in the user
    configuration from a file called .pudl.yml in the user's HOME directory.
    Presently the only values we expect are pudl_in and pudl_out, directories
    that store files that PUDL either depends on that rely on PUDL.

    Args:
        pudl_in ():
        pudl_out ():
        settings_file (path-like): A string or path like object that can be
            used to open a file containing the configuration. Defaults to
            $HOME/.pudl.yml

    Returns:
        dict: A dictionary containing common PUDL settings, derived from those
            read out of the YAML file.

    Todo:
        Return to (pudl_in, pudl_out)
    """
    # If we are missing either of the PUDL directories, try and read settings:
    if pudl_in is None or pudl_out is None:
        pudl_settings = read_user_settings(settings_file)
    else:
        pudl_settings = {}

    # If we have either of the inputs... use them to override what we read in:
    if pudl_in is not None:
        pudl_settings['pudl_in'] = pudl_in
    if pudl_out is not None:
        pudl_settings['pudl_out'] = pudl_out

    # Now construct the other settings:
    pudl_settings['data_dir'] = os.path.join(pudl_settings['pudl_in'], 'data')
    pudl_settings['settings_dir'] = \
        os.path.join(pudl_settings['pudl_in'], 'settings')
    pudl_settings['notebook_dir'] = \
        os.path.join(pudl_settings['pudl_out'], 'notebooks')

    # Now the output directories:
    for format in pc.output_formats:
        format_dir = os.path.join(pudl_settings['pudl_out'], format)
        pudl_settings[f'{format}_dir'] = format_dir
    # We don't need to create the other data directories because they are more
    # complicated, and that task is best done by the datastore module.

    # These DB connection dictionaries are used by sqlalchemy.URL() (Using
    # 127.0.0.1, the numeric equivalent of localhost, to make postgres use the
    # `.pgpass` file without fussing around in the config.) sqlalchemy.URL will
    # make a URL missing post (therefore using the default), and missing a
    # password (which will make the system look for .pgpass)
    pudl_settings['db_pudl'] = {
        'drivername': 'postgresql',
        'host': '127.0.0.1',
        'username': 'catalyst',
        'database': 'pudl'
    }

    pudl_settings['db_pudl_test'] = {
        'drivername': 'postgresql',
        'host': '127.0.0.1',
        'username': 'catalyst',
        'database': 'pudl_test'
    }

    pudl_settings['ferc1_sqlite_url'] = "sqlite:///" + os.path.join(
        pudl_settings['pudl_out'], 'sqlite', 'ferc1.sqlite')

    pudl_settings['ferc1_test_sqlite_url'] = "sqlite:///" + os.path.join(
        pudl_settings['pudl_out'], 'sqlite', 'ferc1_test.sqlite')

    return pudl_settings


def setup(pudl_settings=None):
    """Set up a new PUDL working environment based on the user settings.

    Args:
        pudl_settings ():

    Returns:
        None

    Todo:
        Return to (pudl_settings and Returns type)
    """
    # If we aren't given settings, try and generate them.
    if pudl_settings is None:
        pudl_settings = init()

    # First directories for all of the data sources:
    os.makedirs(os.path.join(pudl_settings['data_dir'], 'tmp'), exist_ok=True)
    for source in pc.data_sources:
        src_dir = datastore.path(source, year=None, file=False,
                                 data_dir=pudl_settings['data_dir'])
        os.makedirs(src_dir, exist_ok=True)

    # Now the settings files:
    os.makedirs(pudl_settings['settings_dir'], exist_ok=True)
    settings_files = [
        fn for fn in importlib.resources.contents('pudl.package_data.settings')
        if importlib.resources.is_resource('pudl.package_data.settings', fn)
        and fn != '__init__.py'
    ]
    for fn in settings_files:
        with importlib.resources.path('pudl.package_data.settings', fn) as f:
            dest_file = os.path.join(pudl_settings['settings_dir'], fn)
            if not os.path.exists(dest_file):
                shutil.copy(f, dest_file)
            else:
                logger.warning(
                    f"Found existing settings file at {dest_file}, "
                    f"not clobbering it."
                )

    # Now the output directories:
    for format in pc.output_formats:
        format_dir = os.path.join(pudl_settings['pudl_out'], format)
        os.makedirs(format_dir, exist_ok=True)

    # Copy over the example notebooks:
    os.makedirs(pudl_settings['notebook_dir'], exist_ok=True)
    notebooks = [
        nb for nb in
        importlib.resources.contents('pudl.package_data.notebooks')
        if importlib.resources.is_resource('pudl.package_data.notebooks', nb)
        and nb != '__init__.py'
    ]
    for nb in notebooks:
        with importlib.resources.path('pudl.package_data.notebooks', nb) as f:
            dest_file = os.path.join(pudl_settings['notebook_dir'], nb)
            if not os.path.exists(dest_file):
                shutil.copy(f, dest_file)
            else:
                logger.warning(
                    f"Found existing notebook at {dest_file}, "
                    f"not clobbering it."
                )
