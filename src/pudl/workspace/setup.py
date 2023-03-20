"""Tools for setting up and managing PUDL workspaces."""
import importlib
import os
import pathlib
import shutil
from pathlib import Path
from typing import IO, Literal

import yaml

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def get_settings(
    env: Literal["test", "live"], yaml_file: IO | None, live_dbs: bool = False
) -> dict[str, str]:
    """Handle PUDL settings and env vars."""
    if yaml_file is not None:
        settings = yaml.safe_load(yaml_file)
    else:
        settings = {}

    if (pudl_out := os.getenv("PUDL_OUTPUT")) and (pudl_in := os.getenv("PUDL_INPUT")):
        settings = derive_paths(Path(pudl_in).parent, Path(pudl_out).parent)
    elif settings:
        settings = derive_paths(settings["pudl_in"], settings["pudl_out"])

    if (pudl_out := settings.get("pudl_out")) and (pudl_in := settings.get("data_dir")):
        if "PUDL_OUTPUT" not in os.environ:
            os.environ["PUDL_OUTPUT"] = pudl_out
        if "PUDL_INPUT" not in os.environ:
            os.environ["PUDL_INPUT"] = pudl_in

    if not (os.getenv("PUDL_OUTPUT") and os.getenv("PUDL_INPUT")):
        raise RuntimeError(
            "Must set 'PUDL_OUTPUT'/'PUDL_INPUT' environment variables or provide valid yaml config file."
        )

    # if env vars exist:
    # override settings

    # if test and not live_dbs:
    # override output directory to temp dir

    # if env vars don't exist:
    # set env vars

    # raise an error

    return settings


def set_defaults(pudl_in, pudl_out, clobber=False):
    """Set default user input and output locations in ``$HOME/.pudl.yml``.

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
    logger.warning(
        "pudl_settings is being deprecated in favor of environment "
        "variables PUDL_OUTPUT and PUDL_INPUT. For more info "
        "see: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
    )
    settings_file = pathlib.Path.home() / ".pudl.yml"
    if settings_file.exists():
        if clobber:
            logger.info(f"{settings_file} exists: clobbering.")
        else:
            logger.info(f"{settings_file} exists: not clobbering.")
            return

    with settings_file.open(mode="w") as f:
        f.write(f"pudl_in: {pudl_in.expanduser().resolve()}\n")
        f.write(f"pudl_out: {pudl_out.expanduser().resolve()}\n")


def get_defaults():
    """Read paths to default PUDL input/output dirs from user's $HOME/.pudl.yml.

    Args:
        None

    Returns:
        dict: The contents of the user's PUDL settings file, with keys
        ``pudl_in`` and ``pudl_out`` defining their default PUDL workspace. If
        the ``$HOME/.pudl.yml`` file does not exist, set these paths to None.
    """
    logger.warning(
        "pudl_settings is being deprecated in favor of environment variables "
        "variables PUDL_OUTPUT and PUDL_INPUT. For more info "
        "see: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
    )
    settings_file = pathlib.Path.home() / ".pudl.yml"

    try:
        with pathlib.Path(settings_file).open(encoding="utf8") as f:
            default_workspace = yaml.safe_load(f)
    except FileNotFoundError:
        logger.info("PUDL user settings file .pudl.yml not found.")
        default_workspace = {"pudl_in": None, "pudl_out": None}
        return default_workspace

    # Ensure that no matter what the user has put in this file, we get fully
    # specified absolute paths out when we read it:
    pudl_in = pathlib.Path(default_workspace["pudl_in"]).expanduser().resolve()
    pudl_out = pathlib.Path(default_workspace["pudl_out"]).expanduser().resolve()
    return derive_paths(pudl_in, pudl_out)


def derive_paths(pudl_in, pudl_out):
    """Derive PUDL paths based on given input and output paths.

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
            according to the output format (sqlite, parquet, etc.).

    Returns:
        dict: A dictionary containing common PUDL settings, derived from those
            read out of the YAML file. Mostly paths for inputs & outputs.
    """
    logger.warning(
        "pudl_settings is being deprecated in favor of environment variables "
        "PUDL_OUTPUT and PUDL_INPUT. For more info "
        "see: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
    )
    pudl_settings = {}

    # The only "inputs" are the datastore and example settings files:
    # Convert from input string to Path and make it absolute w/ resolve()
    pudl_in = pathlib.Path(pudl_in).expanduser().resolve()
    data_dir = pudl_in / "data"
    settings_dir = pudl_in / "settings"
    # Store these as strings... since we aren't using Paths everywhere yet:
    pudl_settings["pudl_in"] = str(pudl_in)
    pudl_settings["data_dir"] = str(data_dir)
    pudl_settings["settings_dir"] = str(settings_dir)

    # Everything else goes into outputs, generally organized by type of file:
    pudl_out = pathlib.Path(pudl_out).expanduser().resolve()
    pudl_settings["pudl_out"] = f"{str(pudl_out)}/output"
    # One directory per output format:
    logger.warning(
        "sqlite and parquet directories are no longer being used. Make sure there is a single directory named 'output' at the root of your workspace. For more info see: https://catalystcoop-pudl.readthedocs.io/en/dev/dev/dev_setup.html"
    )
    for fmt in ["sqlite", "parquet"]:
        pudl_settings[f"{fmt}_dir"] = str(pudl_out)

    # Mirror dagster env vars for ease of use
    pudl_settings["PUDL_OUTPUT"] = pudl_settings["pudl_out"]
    pudl_settings["PUDL_INPUT"] = pudl_settings["data_dir"]

    ferc1_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc1.sqlite")
    pudl_settings["ferc1_db"] = "sqlite:///" + str(ferc1_db_file.resolve())

    ferc1_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc1_xbrl.sqlite")
    pudl_settings["ferc1_xbrl_db"] = "sqlite:///" + str(ferc1_db_file.resolve())
    pudl_settings["ferc1_xbrl_datapackage"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc1_xbrl_datapackage.json"
    )
    pudl_settings["ferc1_xbrl_taxonomy_metadata"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc1_xbrl_taxonomy_metadata.json"
    )

    ferc2_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc2_xbrl.sqlite")
    pudl_settings["ferc2_xbrl_db"] = "sqlite:///" + str(ferc2_db_file.resolve())
    pudl_settings["ferc2_xbrl_datapackage"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc2_xbrl_datapackage.json"
    )
    pudl_settings["ferc2_xbrl_taxonomy_metadata"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc2_xbrl_taxonomy_metadata.json"
    )

    ferc6_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc6_xbrl.sqlite")
    pudl_settings["ferc6_xbrl_db"] = "sqlite:///" + str(ferc6_db_file.resolve())
    pudl_settings["ferc6_xbrl_datapackage"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc6_xbrl_datapackage.json"
    )
    pudl_settings["ferc6_xbrl_taxonomy_metadata"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc6_xbrl_taxonomy_metadata.json"
    )

    ferc60_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc60_xbrl.sqlite")
    pudl_settings["ferc60_xbrl_db"] = "sqlite:///" + str(ferc60_db_file.resolve())
    pudl_settings["ferc60_xbrl_datapackage"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc60_xbrl_datapackage.json"
    )
    pudl_settings["ferc60_xbrl_taxonomy_metadata"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc60_xbrl_taxonomy_metadata.json"
    )

    ferc714_db_file = pathlib.Path(pudl_settings["pudl_out"], "ferc714_xbrl.sqlite")
    pudl_settings["ferc714_xbrl_db"] = "sqlite:///" + str(ferc714_db_file.resolve())
    pudl_settings["ferc714_xbrl_datapackage"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc714_xbrl_datapackage.json"
    )
    pudl_settings["ferc714_xbrl_taxonomy_metadata"] = pathlib.Path(
        pudl_settings["pudl_out"], "ferc714_xbrl_taxonomy_metadata.json"
    )

    pudl_settings["pudl_db"] = "sqlite:///" + str(
        pathlib.Path(pudl_settings["pudl_out"], "pudl.sqlite")
    )

    pudl_settings["censusdp1tract_db"] = "sqlite:///" + str(
        pathlib.Path(pudl_settings["pudl_out"], "censusdp1tract.sqlite")
    )
    return pudl_settings


def init(pudl_in, pudl_out, clobber=False):
    """Set up a new PUDL working environment based on the user settings.

    Args:
        pudl_in (os.PathLike): Path to the directory containing the PUDL input
            files, most notably the ``data`` directory which houses the raw
            data downloaded from public agencies by the
            :mod:`pudl.workspace.datastore` tools. ``pudl_in`` may be the same
            directory as ``pudl_out``.
        pudl_out (os.PathLike): Path to the directory where PUDL should write
            the outputs it generates. These will be organized into directories
            according to the output format (sqlite, parquet, etc.).
        clobber (bool): if True, replace existing files. If False (the default)
            do not replace existing files.

    Returns:
        None
    """
    # Generate paths for the workspace:
    pudl_settings = derive_paths(pudl_in, pudl_out)

    # Create tmp directory
    tmp_dir = pathlib.Path(pudl_settings["data_dir"], "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # These are files that may exist in the package_data directory, but that
    # we do not want to deploy into a user workspace:
    ignore_files = ["__init__.py", ".gitignore"]

    # Make a settings directory in the workspace, and deploy settings files:
    settings_dir = pathlib.Path(pudl_settings["settings_dir"])
    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_pkg = "pudl.package_data.settings"
    deploy(settings_pkg, settings_dir, ignore_files, clobber=clobber)

    # Make output directory:
    pudl_out = pathlib.Path(pudl_settings["pudl_out"])
    pudl_out.mkdir(parents=True, exist_ok=True)


def deploy(pkg_path, deploy_dir, ignore_files, clobber=False):
    """Deploy all files from a package_data directory into a workspace.

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
        file
        for file in importlib.resources.contents(pkg_path)
        if importlib.resources.is_resource(pkg_path, file) and file not in ignore_files
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
