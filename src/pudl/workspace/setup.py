"""Tools for setting up and managing PUDL workspaces."""
import importlib.resources
import os
import pathlib
import shutil
from pathlib import Path

from dotenv import load_dotenv

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def set_path_overrides(
    input_dir: str | None = None,
    output_dir: str | None = None,
) -> None:
    """Set PUDL_INPUT and/or PUDL_OUTPUT env variables.

    Args:
        input_dir: if set, overrides PUDL_INPUT env variable.
        output_dir: if set, overrides PUDL_OUTPUT env variable.
    """
    if input_dir:
        os.environ["PUDL_INPUT"] = input_dir
    if output_dir:
        os.environ["PUDL_OUTPUT"] = input_dir


def get_defaults() -> dict[str, str]:
    """Derive PUDL workspace paths from env variables.

    Reads the PUDL_INPUT and PUDL_OUTPUT environment variables, and derives
    all relevant paths that will be set in the config dictionary.

    Returns:
        dictionary with a variety of different paths where inputs/outputs are
        to be found.
    """
    load_dotenv()

    # Workaround for not having PUDL_* env vars in ReadTheDocs builds.
    #
    # They don't let you set env var through config files, and I'd rather
    # have this in source control than go through some sort of web UI
    #
    # I don't like this any more than you do.
    if os.getenv("READTHEDOCS"):
        set_path_overrides(
            input_dir="~/pudl-work/data",
            output_dir="~/pudl-work/output",
        )
    for env_var in ["PUDL_INPUT", "PUDL_OUTPUT"]:
        if env_var not in os.environ:
            raise RuntimeError(f"{env_var} environment variable must be set.")

    pudl_settings = {}

    # The only "inputs" are the datastore and example settings files:
    # Convert from input string to Path and make it absolute w/ resolve()
    pudl_in = pathlib.Path(os.getenv("PUDL_INPUT")).expanduser().resolve()
    data_dir = pudl_in
    pudl_workspace_legacy = pudl_in.parent
    settings_dir = pudl_workspace_legacy / "settings"

    # Store these as strings... since we aren't using Paths everywhere yet:
    pudl_settings["pudl_in"] = str(pudl_workspace_legacy)
    pudl_settings["data_dir"] = str(data_dir)
    pudl_settings["settings_dir"] = str(settings_dir)

    # Everything else goes into outputs, generally organized by type of file:
    pudl_out = pathlib.Path(os.getenv("PUDL_OUTPUT")).expanduser().resolve()
    pudl_settings["pudl_out"] = str(pudl_out)

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

    if not os.getenv("DAGSTER_HOME"):
        os.environ["DAGSTER_HOME"] = str(
            Path(pudl_settings["pudl_in"]) / "dagster_home"
        )

    return pudl_settings


def init(pudl_settings: dict[str, str], clobber=False):
    """Set up a new PUDL working environment based on the user settings.

    Args:
        pudl_settings (os.PathLike): Paths to data inputs & outputs. See
            get_defaults() for how to get these.
        clobber (bool): if True, replace existing files. If False (the default)
            do not replace existing files.

    Returns:
        None
    """
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
        dest_file = pathlib.Path(deploy_dir, file)
        if pathlib.Path.exists(dest_file):
            if clobber:
                logger.info(f"CLOBBERING existing file at {dest_file}.")
            else:
                logger.info(f"Skipping existing file at {dest_file}")
                continue

            pkg_source = importlib.resources.files(pkg_path).joinpath(file)
            with importlib.resources.as_file(pkg_source) as f:
                shutil.copy(f, dest_file)
