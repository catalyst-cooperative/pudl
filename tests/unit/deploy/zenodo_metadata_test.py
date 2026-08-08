"""Tests for gathering Zenodo deposition metadata from repo sources."""

import json
import subprocess
from pathlib import Path

import pytest
import yaml

from pudl.deploy.zenodo_metadata import (
    CONTACT_US_HTML,
    build_related_resources,
    get_data_license_id,
    load_citation_cff_version,
    load_zenodo_json,
    render_release_notes_html,
    verify_git_tag_checked_out,
)
from pudl.metadata.constants import LICENSES

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


def test_load_zenodo_json(tmp_path):
    """load_zenodo_json should pass creators through as-is and return keywords."""
    path = tmp_path / ".zenodo.json"
    path.write_text(
        json.dumps(
            {
                "creators": [
                    {
                        "name": "Selvans, Zane",
                        "affiliation": "Catalyst Cooperative",
                        "orcid": "0000-0002-9961-7208",
                    }
                ],
                "keywords": ["electricity", "energy"],
            }
        ),
        encoding="utf-8",
    )

    creators, keywords = load_zenodo_json(path)

    assert creators == [
        {
            "name": "Selvans, Zane",
            "affiliation": "Catalyst Cooperative",
            "orcid": "0000-0002-9961-7208",
        }
    ]
    assert keywords == ["electricity", "energy"]


def test_load_citation_cff_version(tmp_path):
    """load_citation_cff_version should prefix the CFF version with 'v'."""
    path = tmp_path / "CITATION.cff"
    path.write_text(yaml.dump({"version": "2026.8.0"}), encoding="utf-8")

    assert load_citation_cff_version(path) == "v2026.8.0"


def test_render_release_notes_html_extracts_only_target_version(docs_html_dir):
    """Only the requested version's fragment should be returned."""
    html = render_release_notes_html(docs_html_dir, "v2026.8.0")

    assert "v2026.8.0 (2026-08-07)" in html
    assert "v2026.7.2" not in html
    assert "test release." in html


def test_render_release_notes_html_strips_sphinx_cruft(docs_html_dir):
    """Section wrappers and headerlink permalinks should be stripped out."""
    html = render_release_notes_html(docs_html_dir, "v2026.8.0")

    assert "<section" not in html
    assert "headerlink" not in html
    assert "#</a>" not in html


def test_render_release_notes_html_decrements_heading_levels(docs_html_dir):
    """Headings should shift up one level so the version heading becomes <h1>.

    Sphinx numbers the version heading <h2> (since <h1> is reserved for the page's
    own "PUDL Release Notes" title), but the description is its own standalone
    document, so every heading is bumped up a level.
    """
    html = render_release_notes_html(docs_html_dir, "v2026.8.0")

    assert "<h1>v2026.8.0 (2026-08-07)</h1>" in html
    assert "<h2>New Data</h2>" in html
    assert "<h3>" not in html


def test_render_release_notes_html_rewrites_relative_links(docs_html_dir):
    """Relative and same-page links should become absolute docs.catalyst.coop URLs."""
    html = render_release_notes_html(docs_html_dir, "v2026.8.0")

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


def test_build_related_resources_production_includes_archive_link():
    """When a software archive DOI is available, it should appear in both outputs."""
    footer_html, related_identifiers = build_related_resources(
        "v2026.8.0", "https://doi.org/10.5281/zenodo.21360813"
    )

    assert "Other PUDL v2026.8.0 Resources" in footer_html
    assert "https://doi.org/10.5281/zenodo.21360813" in footer_html
    assert {
        "identifier": "https://doi.org/10.5281/zenodo.21360813",
        "relation": "isSupplementedBy",
        "resource_type": "software",
    } in related_identifiers


def test_build_related_resources_sandbox_omits_archive_link():
    """On sandbox, with no software archive DOI, that bullet/entry should be absent."""
    footer_html, related_identifiers = build_related_resources("v2026.8.0", None)

    assert "Zenodo archive of the PUDL GitHub repo" not in footer_html
    assert all(
        entry.get("resource_type") != "software" or "doi.org" not in entry["identifier"]
        for entry in related_identifiers
    )
    # The GitHub release link and docs link should still be present.
    assert any(
        entry["identifier"]
        == "https://github.com/catalyst-cooperative/pudl/releases/tag/v2026.8.0"
        for entry in related_identifiers
    )
    assert any(entry["relation"] == "isDocumentedBy" for entry in related_identifiers)


def test_get_data_license_id_matches_authoritative_source():
    """The returned ID should be a real LICENSES key, matching SOURCES['pudl']."""
    license_id = get_data_license_id()

    assert license_id in LICENSES
    assert license_id == "cc-by-4.0"


def test_contact_us_html_is_static_and_nonempty():
    """A basic guard against CONTACT_US_HTML being accidentally emptied out."""
    assert "Contact Us" in CONTACT_US_HTML
    assert "hello@catalyst.coop" in CONTACT_US_HTML


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


def test_verify_git_tag_checked_out_raises_when_head_is_ahead_of_tag(mocker):
    """A working tree whose HEAD doesn't match the tag's commit should raise."""
    mocker.patch(
        "pudl.deploy.zenodo_metadata.run_git",
        side_effect=["aaa111\n", "bbb222\n"],
    )

    with pytest.raises(ValueError, match="does not match"):
        verify_git_tag_checked_out("v1.0.0", Path("/fake/repo"))


def test_verify_git_tag_checked_out_raises_when_tag_missing(mocker):
    """An unresolvable tag should raise a clear error rather than a raw CalledProcessError."""
    mocker.patch(
        "pudl.deploy.zenodo_metadata.run_git",
        side_effect=subprocess.CalledProcessError(128, ["git", "rev-parse"]),
    )

    with pytest.raises(ValueError, match="Could not resolve"):
        verify_git_tag_checked_out("v9.9.9", Path("/fake/repo"))
