import pytest

from pudl.workspace.setup import PudlPaths


@pytest.fixture(scope="session", autouse=True)
def configure_paths_for_tests():
    """Disable the root session path bootstrap for the unit-test subtree.

    ``test/conftest.py`` installs a session-scoped autouse fixture with this
    name that configures ``PudlPaths`` for the full test suite.

    This means that if you make a session-scoped fixture, it *could* depend on
    the env setup from the root ``configure_paths_for_tests``.

    Shadowing that fixture here lets us catch that while running unit tests, so
    we don't accidentally introduce environment-aware tests to our unit test suite.

    NOTE (2026-04-03): In *mixed* runs, i.e. integration + unit in one process,
    the root fixture will still run and its side effects will still occur. This
    means that the env *will* be tweaked! But in theory the unit-only runs that
    happen every time you commit will catch any tests that are sneakily relying on
    the environment.
    """
    yield


@pytest.fixture(autouse=True)
def disable_ambient_pudlpaths_config(monkeypatch):
    """Make unit tests fail if they rely on ambient PUDL path configuration.

    Unit tests should not pick up `PUDL_INPUT` / `PUDL_OUTPUT` from process
    environment variables or a local `.env` file. Tests that need explicit path
    configuration should inject it directly or set environment variables locally
    within the test.
    """
    monkeypatch.delenv("PUDL_INPUT", raising=False)
    monkeypatch.delenv("PUDL_OUTPUT", raising=False)

    original_init = PudlPaths.__init__

    def patched_init(self, *args, **kwargs):
        """Disable `.env` loading unless a test opts back into it explicitly."""
        kwargs.setdefault("_env_file", None)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(PudlPaths, "__init__", patched_init)
