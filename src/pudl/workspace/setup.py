"""Tools for setting up and managing PUDL workspaces."""
import importlib.resources
import os
import pathlib
import shutil
from pathlib import Path

from pydantic import DirectoryPath, NewPath
from pydantic_settings import BaseSettings, SettingsConfigDict

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)

PotentialDirectoryPath = DirectoryPath | NewPath


class PudlPaths(BaseSettings):
    """These settings provide access to various PUDL directories.

    It is primarily configured via PUDL_INPUT and PUDL_OUTPUT environment
    variables. Other paths of relevance are derived from these.
    """

    pudl_input: PotentialDirectoryPath
    pudl_output: PotentialDirectoryPath
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def input_dir(self) -> Path:
        """Path to PUDL input directory."""
        return Path(self.pudl_input).absolute()

    @property
    def output_dir(self) -> Path:
        """Path to PUDL output directory."""
        return Path(self.pudl_output).absolute()

    @property
    def settings_dir(self) -> Path:
        """Path to directory containing settings files."""
        return self.input_dir.parent / "settings"

    @property
    def data_dir(self) -> Path:
        """Path to PUDL data directory."""
        # TODO(janrous): possibly deprecate this in favor of input_dir
        return self.input_dir

    @property
    def pudl_db(self) -> str:
        """Returns url of locally stored pudl sqlite database."""
        return self.sqlite_db_uri("pudl")

    def sqlite_db_uri(self, name: str) -> str:
        """Returns url of locally stored pudl sqlite database with given name.

        The name is expected to be the name of the database without the .sqlite
        suffix. E.g. pudl, ferc1 and so on.
        """
        # SQLite URI has 3 slashes - 2 to separate URI scheme, 1 to separate creds
        # sqlite://{credentials}/{db_path}
        return f"sqlite:///{self.sqlite_db_path(name)}"

    def sqlite_db_path(self, name: str) -> Path:
        """Return path to locally stored SQLite DB file."""
        return self.output_dir / f"{name}.sqlite"

    def output_file(self, filename: str) -> Path:
        """Path to file in PUDL output directory."""
        return self.output_dir / filename

    @staticmethod
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
            os.environ["PUDL_OUTPUT"] = output_dir


def init(clobber=False):
    """Set up a new PUDL working environment based on the user settings.

    Args:
        clobber (bool): if True, replace existing files. If False (the default)
            do not replace existing files.

    Returns:
        None
    """
    # Create tmp directory
    tmp_dir = PudlPaths().data_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # These are files that may exist in the package_data directory, but that
    # we do not want to deploy into a user workspace:
    ignore_files = ["__init__.py", ".gitignore"]

    # TODO(janrous): perhaps we don't need to do this?
    # Make a settings directory in the workspace, and deploy settings files:
    settings_dir = PudlPaths().settings_dir
    settings_dir.mkdir(parents=True, exist_ok=True)
    settings_pkg = "pudl.package_data.settings"
    deploy(settings_pkg, settings_dir, ignore_files, clobber=clobber)

    # Make output directory:
    PudlPaths().output_dir.mkdir(parents=True, exist_ok=True)
    # TODO(rousik): it might make sense to turn this into a method of
    # PudlPaths object and to move this to settings.py from this module.
    # Unclear whether deployment of settings files makes much sense.


def deploy(
    pkg_path: str,
    deploy_dir: pathlib.Path,
    ignore_files: list[str],
    clobber: bool = False,
) -> None:
    """Deploy all files from a package_data directory into a workspace.

    Args:
        pkg_path: Dotted module path to the subpackage inside of package_data containing
            the resources to be deployed.
        deploy_dir: Directory on the filesystem to which the files within pkg_path
            should be deployed.
        ignore_files: List of filenames (strings) that may be present in the pkg_path
            subpackage, but that should be ignored.
        clobber: if True, replace existing copies of the files that are being deployed
            from pkg_path to deploy_dir. If False, do not replace existing files.

    Returns:
        None
    """
    files = [
        path
        for path in importlib.resources.files(pkg_path).iterdir()
        if path.is_file() and path.name not in ignore_files
    ]
    for file in files:
        dest_file = pathlib.Path(deploy_dir, file)
        if pathlib.Path.exists(dest_file):
            if clobber:
                logger.info(f"CLOBBERING existing file at {dest_file}.")
            else:
                logger.info(f"Skipping existing file at {dest_file}")
                continue

            pkg_source = importlib.resources.files(pkg_path) / file
            with importlib.resources.as_file(pkg_source) as f:
                shutil.copy(f, dest_file)
