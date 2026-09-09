"""Tools for setting up and managing PUDL workspaces."""

import os
from pathlib import Path
from typing import Any, Self

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class PudlPaths(BaseSettings):
    """These settings provide access to various PUDL directories.

    It is primarily configured via PUDL_INPUT and PUDL_OUTPUT environment
    variables. Other paths of relevance are derived from these.
    """

    # Annotated as plain ``Path`` (not ``Path | str``) because ``normalize_paths``
    # below runs in ``mode="before"`` and coerces whatever is passed -- string,
    # relative path, etc. -- into an absolute ``Path`` before validation. Callers
    # can still pass strings; downstream code always sees a ``Path``.
    pudl_input: Path
    pudl_output: Path
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("pudl_input", "pudl_output", mode="before")
    @classmethod
    def normalize_paths(cls, value: Any) -> Path:
        """Normalize configured paths to absolute ``Path`` objects."""
        return Path(value).absolute()

    @model_validator(mode="after")
    def create_directories(self: Self):
        """Create PUDL input and output directories if they don't already exist."""
        for path_name, path in [
            ("PUDL_INPUT", self.pudl_input),
            ("PUDL_OUTPUT", self.pudl_output),
        ]:
            if path.is_symlink() and not path.exists():
                raise FileExistsError(
                    f"{path_name} path {path} is a broken symlink. "
                    f"If it points to an external drive, ensure the drive is mounted. "
                    f"Otherwise, remove the symlink and try again."
                )

            if path.exists() and not path.is_dir():
                raise FileExistsError(
                    f"{path_name} path {path} exists but is not a directory. "
                    f"Please remove or relocate this file."
                )

            if path.exists():
                continue

            if not path.parent.exists() or not path.parent.is_dir():
                raise FileNotFoundError(
                    f"{path_name} parent directory {path.parent} does not exist. "
                    "Please create the parent directory first."
                )

            # If we've gotten this far, the path doesn't exist but the parent directory
            # does, so we can create it.
            path.mkdir()
        return self

    def parquet_path(self, table_name: str | None = None) -> Path:
        """Return path to parquet file for given database and table."""
        if table_name is None:
            return self.pudl_output / "parquet"
        return self.pudl_output / "parquet" / f"{table_name}.parquet"

    def sqlite_path(self, name: str) -> Path:
        """Return path to locally stored SQLite DB file."""
        return self.pudl_output / f"{name}.sqlite"

    def duckdb_path(self, name: str) -> Path:
        """Return path to locally stored DuckDB file."""
        return self.pudl_output / f"{name}.duckdb"

    def output_file(self, filename: str) -> Path:
        """Path to file in PUDL output directory."""
        return self.pudl_output / filename

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
