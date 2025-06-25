"""Tools for setting up and managing PUDL workspaces."""

import os
from pathlib import Path
from typing import Self

from pydantic import DirectoryPath, NewPath, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)

PotentialDirectoryPath = DirectoryPath | NewPath

PUDL_ROOT_PATH = Path(__file__).parent.parent.parent.parent


class PudlPaths(BaseSettings):
    """These settings provide access to various PUDL directories.

    It is primarily configured via PUDL_INPUT and PUDL_OUTPUT environment
    variables. Other paths of relevance are derived from these.
    """

    pudl_input: PotentialDirectoryPath
    pudl_output: PotentialDirectoryPath
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def create_directories(self: Self):
        """Create PUDL input and output directories if they don't already exist."""
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self

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

    def parquet_path(self, table_name: str | None = None) -> Path:
        """Return path to parquet file for given database and table."""
        if table_name is None:
            return self.output_dir / "parquet"
        return self.output_dir / "parquet" / f"{table_name}.parquet"

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
