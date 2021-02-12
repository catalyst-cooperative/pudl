"""Implements fsspec based Result storage that can be either local or remote."""
import os
from typing import Any

import fsspec
from prefect.engine.result import Result

from pudl.helpers import metafs


class FSSpecResult(Result):
    """Implements Result that uses fsspec to access local or remote filesystem.

    This behaves similarly to LocalResult in that you specify dir that points
    to the root_dir where the results should be stored and fsspec picks the appropriate
    filesystem to use.
    """

    def __init__(self, root_dir: str = None, **kwargs: Any) -> None:  # noqa: A002
        """Initialize fsspec backed Result that stores files under root_dir."""
        self.root_dir = root_dir
        super().__init__(**kwargs)

    def read(self, location: str) -> Result:
        """Deserializes result from a given location."""
        new = self.copy()
        new.location = location
        with fsspec.open(os.path.join(self.root_dir, location), "rb") as f:
            new.value = self.serializer.deserialize(f.read())
        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        """Checks if the result is stored at a given location."""
        full_path = os.path.join(self.root_dir, location.format(**kwargs))  # noqa: FS002
        return metafs.exists(full_path)

    def write(self, value_: Any, **kwargs: Any) -> Result:
        """Serializes result to a given location."""
        new = self.format(**kwargs)  # noqa: FS002
        new.value = value_
        assert new.location is not None

        full_path = os.path.join(self.root_dir, new.location)
        value = self.serializer.serialize(new.value)

        with fsspec.open(full_path, "wb") as f:
            f.write(value)
        new.location = full_path
        return new
