"""Tests for gathering Zenodo deposition metadata from repo sources."""

from pathlib import Path

import pytest

from pudl import PUDL_ROOT_PATH
from pudl.deploy.zenodo_metadata import (
    build_related_resources,
    get_latest_release_tag,
    load_zenodo_json,
    render_release_notes_html,
    verify_git_tag_checked_out,
)
from pudl.helpers import run_git

_FIXTURE_RELEASE_NOTES_HTML = """\
<!DOCTYPE html>
<html>
<body>
<div class="body">
<section id="pudl-release-notes">
<h1>PUDL Release Notes<a class="headerlink" href="#pudl-release-notes">#</a></h1>
<section id="v2026-8-0-2026-08-07">
<span id="release-v2026-8-0"></span><h2>v2026.8.0 (2026-08-07)\
<a class="headerlink" href="#v2026-8-0-2026-08-07">#</a></h2>
<p>This is a test release. See <a class="reference internal" \
href="data_sources/eia860.html"><span class="doc">EIA-860</span></a> and \
<a class="reference external" href="https://github.com/catalyst-cooperative/pudl/pull/1">\
#1</a>.</p>
<section id="new-data">
<h3>New Data<a class="headerlink" href="#new-data">#</a></h3>
<ul class="simple">
<li><p>Added a thing. See <a class="reference internal" href="#some-anchor">\
elsewhere on this page</a>.</p></li>
</ul>
</section>
</section>
<section id="v2026-7-2-2026-07-14">
<span id="release-v2026-7-2"></span><h2>v2026.7.2 (2026-07-14)\
<a class="headerlink" href="#v2026-7-2-2026-07-14">#</a></h2>
<p>An older test release.</p>
</section>
</section>
</div>
</body>
</html>
"""


@pytest.fixture
def docs_html_dir(tmp_path):
    """A fake built-docs directory containing a fixture release_notes.html."""
    (tmp_path / "release_notes.html").write_text(
        _FIXTURE_RELEASE_NOTES_HTML, encoding="utf-8"
    )
    return tmp_path


def test_load_zenodo_json():
    """load_zenodo_json should pass creators through as-is and return keywords."""
    zane_selvans = {
        "name": "Selvans, Zane",
        "affiliation": "Catalyst Cooperative",
        "orcid": "0000-0002-9961-7208",
    }

    creators, keywords = load_zenodo_json(PUDL_ROOT_PATH / ".zenodo.json")
    assert len(creators) >= 4
    assert "electricity" in keywords
    assert zane_selvans in creators


def test_get_latest_release_tag(mocker):
    """get_latest_release_tag should return git's answer, stripped."""
    mock_run_git = mocker.patch(
        "pudl.deploy.zenodo_metadata.run_git",
        return_value="v2026.8.0\n",
    )

    assert get_latest_release_tag(Path("/fake/repo")) == "v2026.8.0"
    mock_run_git.assert_called_once_with(
        ["describe", "--tags", "--abbrev=0", "--match", "v20*"],
        cwd=Path("/fake/repo"),
    )


def test_render_release_notes_html(docs_html_dir):
    """One version's release notes should render as a clean, self-contained fragment.

    Checked in a single call/test rather than split across several, since these are
    all assertions about one function call's output rather than independent
    behaviors.
    """
    html = render_release_notes_html(docs_html_dir, "v2026.8.0")

    # Only the target version's content is present.
    assert "v2026.8.0 (2026-08-07)" in html
    assert "v2026.7.2" not in html
    assert "test release." in html

    # Sphinx's <section> wrappers and headerlink permalinks are stripped.
    assert "<section" not in html
    assert "headerlink" not in html
    assert "#</a>" not in html

    # Headings are shifted up a level (Sphinx numbers the version heading <h2>,
    # since <h1> is reserved for the page's own "PUDL Release Notes" title) so the
    # version heading becomes the top-level <h1> in this standalone fragment.
    assert "<h1>v2026.8.0 (2026-08-07)</h1>" in html
    assert "<h2>New Data</h2>" in html
    assert "<h3>" not in html

    # Relative and same-page links become absolute docs.catalyst.coop URLs.
    assert (
        'href="https://docs.catalyst.coop/pudl/en/v2026.8.0/data_sources/eia860.html"'
        in html
    )
    assert (
        'href="https://docs.catalyst.coop/pudl/en/v2026.8.0/release_notes.html'
        '#some-anchor"' in html
    )
    # External links are left alone.
    assert 'href="https://github.com/catalyst-cooperative/pudl/pull/1"' in html


def test_render_release_notes_html_missing_version_raises(docs_html_dir):
    """A version with no matching section should raise, not silently return nothing."""
    with pytest.raises(ValueError, match="v9999.1.0"):
        render_release_notes_html(docs_html_dir, "v9999.1.0")


@pytest.mark.parametrize(
    "github_archive_doi_url",
    ["https://doi.org/10.5281/zenodo.21360813", None],
    ids=["production", "sandbox"],
)
def test_build_related_resources(github_archive_doi_url):
    """The software archive link/entry should appear iff a DOI URL was given.

    Production runs pass a real DOI (from the GitHub-repo Zenodo software archive);
    sandbox runs pass None, since GitHub's Zenodo integration never publishes there.
    """
    footer_html, related_identifiers = build_related_resources(
        "v2026.8.0", github_archive_doi_url
    )

    assert "Other PUDL v2026.8.0 Resources" in footer_html
    # The GitHub release link and docs link should always be present.
    assert any(
        entry["identifier"]
        == "https://github.com/catalyst-cooperative/pudl/releases/tag/v2026.8.0"
        for entry in related_identifiers
    )
    assert any(entry["relation"] == "isDocumentedBy" for entry in related_identifiers)

    if github_archive_doi_url is None:
        assert "Zenodo archive of the PUDL GitHub repo" not in footer_html
        assert all(
            entry.get("resource_type") != "software"
            or "doi.org" not in entry["identifier"]
            for entry in related_identifiers
        )
    else:
        assert github_archive_doi_url in footer_html
        assert {
            "identifier": github_archive_doi_url,
            "relation": "isSupplementedBy",
            "resource_type": "software",
        } in related_identifiers


@pytest.fixture
def git_repo(tmp_path, monkeypatch) -> Path:
    """A throwaway git repo with one commit, isolated under pytest's ``tmp_path``.

    Real ``git`` commands (via ``run_git``), not mocks, so these tests actually
    exercise git's own tag resolution (``rev-parse <tag>^{commit}``) rather than
    just the string-comparison logic downstream of it. ``tmp_path`` living outside
    this repo's own working tree isn't enough to isolate these commands on its own:
    ``GIT_DIR``/``GIT_WORK_TREE``/``GIT_INDEX_FILE`` env vars, if set in the calling
    process (as pre-commit/git hooks do), override git's normal cwd-based repo
    discovery and would silently redirect ``run_git`` here into the real PUDL repo's
    index instead of this throwaway one. Clearing them for the test process ensures
    ``run_git``'s ``cwd=repo_root`` is what actually determines the repo.
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

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    run_git(["init"], cwd=repo_root)
    run_git(["config", "user.email", "test@example.com"], cwd=repo_root)
    run_git(["config", "user.name", "Test"], cwd=repo_root)
    # Override any ambient global gpg-signing config, which would otherwise make
    # `git tag` fail here asking for a signing key/message it doesn't have.
    run_git(["config", "tag.gpgSign", "false"], cwd=repo_root)
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
