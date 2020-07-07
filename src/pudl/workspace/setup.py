"""Tools for setting up and managing PUDL workspaces."""
import importlib
import logging
import pathlib
import shutil

import yaml

from pudl import constants as pc

logger = logging.getLogger(__name__)


def set_defaults(pudl_in, pudl_out, clobber=False):
    """
    Set default user input and output locations in ``$HOME/.pudl.yml``.

    Create a user settings file for future reference, that defines the default
    PUDL input and output directories. If this file already exists, behavior
    depends on the clobber parameter, which is False by default. If it's True,
    the existing file is replaced. If False, the existing file is not changed.

    Args:
        pudl_in (os.PathLike): Path to be used as the default input directory
            for PUDL -- this is where :mod:`pudl.workspace.datastore` will look
            to find the ``data`` directory, full of data from public agencies.
        pudl_out (os.PathLike): Path to the default output directory for PUDL,
            where results of data processing will be organized.
        clobber (bool): If True and a user settings file exists, overwrite it.
            If False, do not alter the existing file. Defaults to False.

    Returns:
        None

    """
    settings_file = pathlib.Path.home() / '.pudl.yml'
    if settings_file.exists():
        if clobber:
            logger.info(f"{settings_file} exists: clobbering.")
        else:
            logger.info(f"{settings_file} exists: not clobbering.")
            return

    with settings_file.open(mode='w') as f:
        f.write(f"pudl_in: {pudl_in.expanduser().resolve()}\n")
        f.write(f"pudl_out: {pudl_out.expanduser().resolve()}\n")


def get_defaults():
    """
    Read paths to default PUDL input/output dirs from user's $HOME/.pudl.yml.

    Args:
        None

    Returns:
        dict: The contents of the user's PUDL settings file, with keys
        ``pudl_in`` and ``pudl_out`` defining their default PUDL workspace. If
        the ``$HOME/.pudl.yml`` file does not exist, set these paths to None.

    """
    settings_file = pathlib.Path.home() / '.pudl.yml'

    try:
        with pathlib.Path(settings_file).open() as f:
            default_workspace = yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("PUDL user settings file .pudl.yml not found.")
        default_workspace = {"pudl_in": None, "pudl_out": None}
        return default_workspace

    # Ensure that no matter what the user has put in this file, we get fully
    # specified absolute paths out when we read it:
    pudl_in = (
        pathlib.Path(default_workspace["pudl_in"]).
        expanduser().
        resolve()
    )
    pudl_out = (
        pathlib.Path(default_workspace["pudl_out"]).
        expanduser().
        resolve()
    )
    return derive_paths(pudl_in, pudl_out)


def derive_paths(pudl_in, pudl_out):
    """
    Derive PUDL paths based on given input and output paths.

    If no configuration file path is provided, attempt to read in the user
    configuration from a file called .pudl.yml in the user's HOME directory.
    Presently the only values we expect are pudl_in and pudl_out, directories
    that store files that PUDL either depends on that rely on PUDL.

    Args:
        pudl_in (os.PathLike): Path to the directory containing the PUDL input
            files, most notably the ``data`` directory which houses the raw
            data downloaded from public agencies by the
            :mod:`pudl.workspace.datastore` tools. ``pudl_in`` may be the same
            directory as ``pudl_out``.
        pudl_out (os.PathLike): Path to the directory where PUDL should write
            the outputs it generates. These will be organized into directories
            according to the output format (sqlite, datapackage, etc.).

    Returns:
        dict: A dictionary containing common PUDL settings, derived from those
            read out of the YAML file. Mostly paths for inputs & outputs.

    """
    # ps is short for pudl settings -- a dictionary of paths, etc.
    ps = {}

    # The only "inputs" are the datastore and example settings files:
    # Convert from input string to Path and make it absolute w/ resolve()
    pudl_in = pathlib.Path(pudl_in).expanduser().resolve()
    data_dir = pudl_in / "data"
    settings_dir = pudl_in / "settings"
    # Store these as strings... since we aren't using Paths everywhere yet:
    ps["pudl_in"] = str(pudl_in)
    ps["data_dir"] = str(data_dir)
    ps["settings_dir"] = str(settings_dir)

    # Everything else goes into outputs, generally organized by type of file:
    pudl_out = pathlib.Path(pudl_out).expanduser().resolve()
    ps["pudl_out"] = str(pudl_out)
    # One directory per output format, datapackage, sqlite, etc.:
    for fmt in pc.output_formats:
        ps[f"{fmt}_dir"] = str(pudl_out / fmt)

    ferc1_db_file = pathlib.Path(ps['sqlite_dir'], 'ferc1.sqlite')
    ps['ferc1_db'] = "sqlite:///" + str(ferc1_db_file.resolve())

    ps['pudl_db'] = "sqlite:///" + str(pathlib.Path(
        ps['sqlite_dir'], 'pudl.sqlite'))

    return ps


def init(pudl_in, pudl_out, clobber=False):
    """
    Set up a new PUDL working environment based on the user settings.

    Args:
        pudl_in (os.PathLike): Path to the directory containing the PUDL input
            files, most notably the ``data`` directory which houses the raw
            data downloaded from public agencies by the
            :mod:`pudl.workspace.datastore` tools. ``pudl_in`` may be the same
            directory as ``pudl_out``.
        pudl_out (os.PathLike): Path to the directory where PUDL should write
            the outputs it generates. These will be organized into directories
            according to the output format (sqlite, datapackage, etc.).
        clobber (bool): if True, replace existing files. If False (the default)
            do not replace existing files.

    Returns:
        None

    """
    # Generate paths for the workspace:
    ps = derive_paths(pudl_in, pudl_out)

    # Create tmp directory
    tmp_dir = pathlib.Path(ps["data_dir"], "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # These are files that may exist in the package_data directory, but that
    # we do not want to deploy into a user workspace:
    ignore_files = ['__init__.py', '.gitignore']

    # Make a settings directory in the workspace, and deploy settings files:
    settings_dir = pathlib.Path(ps['settings_dir'])
    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_pkg = "pudl.package_data.settings"
    deploy(settings_pkg, settings_dir, ignore_files, clobber=clobber)

    # Make several output directories, and deploy example notebooks:
    for fmt in pc.output_formats:
        format_dir = pathlib.Path(ps["pudl_out"], fmt)
        format_dir.mkdir(parents=True, exist_ok=True)
    notebook_dir = pathlib.Path(ps["notebook_dir"])
    notebook_pkg = "pudl.package_data.notebooks"
    deploy(notebook_pkg, notebook_dir, ignore_files, clobber=clobber)

    # Deploy the pudl user environment file.
    environment_pkg = "pudl.package_data"
    deploy(environment_pkg, ps["pudl_out"], ignore_files, clobber=clobber)


def deploy(pkg_path, deploy_dir, ignore_files, clobber=False):
    """
    Deploy all files from a package_data directory into a workspace.

    Args:
        pkg_path (str): Dotted module path to the subpackage inside of
            package_data containing the resources to be deployed.
        deploy_dir (os.PathLike): Directory on the filesystem to which the
            files within pkg_path should be deployed.
        ignore_files (iterable): List of filenames (strings) that may be
            present in the pkg_path subpackage, but that should be ignored.
        clobber (bool): if True, replace existing copies of the files that are
            being deployed from pkg_path to deploy_dir. If False, do not
            replace existing files.

    Returns:
        None

    """
    files = [
        file for file in
        importlib.resources.contents(pkg_path)
        if importlib.resources.is_resource(pkg_path, file)
        and file not in ignore_files
    ]
    for file in files:
        with importlib.resources.path(pkg_path, file) as f:
            dest_file = pathlib.Path(deploy_dir, file)
            if pathlib.Path.exists(dest_file):
                if clobber:
                    logger.info(f"CLOBBERING existing file at {dest_file}.")
                else:
                    logger.info(f"Skipping existing file at {dest_file}")
                    continue
            shutil.copy(f, dest_file)
