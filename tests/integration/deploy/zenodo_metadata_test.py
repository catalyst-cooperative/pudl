"""Integration tests for ``verify_git_tag_checked_out``.

These exercise real ``git`` commands (via ``run_git``) against a throwaway repo
rather than mocking, so they actually test git's own tag resolution
(``rev-parse <tag>^{commit}``). They're fast, but conceptually belong here rather
than in ``tests/unit`` because they shell out to git and build a real repo.
"""

from pathlib import Path

import pytest

from pudl.deploy.zenodo_metadata import verify_git_tag_checked_out
from pudl.helpers import run_git


@pytest.fixture
def git_repo(tmp_path, monkeypatch) -> Path:
    """A throwaway git repo with one commit, isolated under pytest's ``tmp_path``.

    Real ``git`` commands (via ``run_git``), not mocks, so these tests actually
    exercise git's own tag resolution (``rev-parse <tag>^{commit}``) rather than
    just the string-comparison logic downstream of it. ``tmp_path`` living outside
    this repo's own working tree isn't enough to isolate these commands on its own.

    We need to unset some git env vars and redirect git's global/system config to empty
    files so that the throwaway repo's local config is the only config in effect.
    Leaving the global / system env vars unset results in git falling back to the
    default locations for those configs (``~/.gitconfig`` and ``/etc/gitconfig``) which
    may well exist.
    """
    for var in (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
    ):
        monkeypatch.delenv(var, raising=False)

    empty_config = tmp_path / "empty_gitconfig"
    empty_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(empty_config))
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", str(empty_config))

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    run_git(["init"], cwd=repo_root)
    run_git(["config", "user.email", "test@example.com"], cwd=repo_root)
    run_git(["config", "user.name", "Test"], cwd=repo_root)
    (repo_root / "README.md").write_text("test\n")
    run_git(["add", "README.md"], cwd=repo_root)
    run_git(["commit", "-m", "Initial commit"], cwd=repo_root)
    return repo_root


def test_verify_git_tag_checked_out_passes_when_head_matches_tag(git_repo):
    """No error when HEAD is exactly the tagged commit."""
    run_git(["tag", "v1.0.0"], cwd=git_repo)

    verify_git_tag_checked_out("v1.0.0", git_repo)


def test_verify_git_tag_checked_out_raises_when_head_is_ahead_of_tag(git_repo):
    """Raises when HEAD has moved past the tagged commit."""
    run_git(["tag", "v1.0.0"], cwd=git_repo)
    (git_repo / "README.md").write_text("more\n")
    run_git(["add", "README.md"], cwd=git_repo)
    run_git(["commit", "-m", "Second commit"], cwd=git_repo)

    with pytest.raises(ValueError, match="does not match"):
        verify_git_tag_checked_out("v1.0.0", git_repo)


def test_verify_git_tag_checked_out_raises_when_tag_missing(git_repo):
    """Raises a clear error when the tag doesn't exist / can't be resolved."""
    with pytest.raises(ValueError, match="Could not resolve"):
        verify_git_tag_checked_out("v1.0.0", git_repo)
