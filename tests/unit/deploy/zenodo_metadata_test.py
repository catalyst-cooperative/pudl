"""Tests for gathering Zenodo deposition metadata from repo sources."""

import subprocess
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


def test_verify_git_tag_checked_out_passes_when_head_matches_tag(mocker):
    """No error when HEAD resolves to the same commit as the tag.

    These tests mock ``run_git`` rather than spawning a real ``git`` subprocess
    against a throwaway repo, so there's no chance of a leaked ``GIT_DIR``/
    ``GIT_WORK_TREE`` (or any other ambient git config) ever causing test git
    commands to touch anything outside the test process.
    """
    mocker.patch(
        "pudl.deploy.zenodo_metadata.run_git",
        return_value="abc123def\n",
    )

    verify_git_tag_checked_out("v1.0.0", Path("/fake/repo"))


@pytest.mark.parametrize(
    "run_git_side_effect,expected_match",
    [
        (["aaa111\n", "bbb222\n"], "does not match"),
        (
            subprocess.CalledProcessError(128, ["git", "rev-parse"]),
            "Could not resolve",
        ),
    ],
    ids=["head-ahead-of-tag", "tag-missing"],
)
def test_verify_git_tag_checked_out_raises(mocker, run_git_side_effect, expected_match):
    """A HEAD/tag mismatch or an unresolvable tag should both raise a clear ValueError."""
    mocker.patch(
        "pudl.deploy.zenodo_metadata.run_git",
        side_effect=run_git_side_effect,
    )

    with pytest.raises(ValueError, match=expected_match):
        verify_git_tag_checked_out("v1.0.0", Path("/fake/repo"))
