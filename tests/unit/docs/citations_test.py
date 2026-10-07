"""Unit tests for :mod:`pudl.docs.citations`."""

import shutil

import pytest
from pybtex.database import parse_string

from pudl import PUDL_DOCS_PATH
from pudl.docs import citations

BIB = """
@article{new_a, author={Zed, A.}, title={Newer}, year={2024}}
@article{old_a, author={Abe, B.}, title={Older}, year={2020}}
@techreport{wp, title={WP}, year={2022}, type={Working Paper}}
@misc{other, title={Other}, year={2021}}
@dataset{novel, title={Novel}, year={2023}}
"""


@pytest.fixture
def entries():
    """Parsed pybtex entries keyed by citation key."""
    return parse_string(BIB, "bibtex").entries


def test_heading_prefers_type_field_then_kind_label(entries):
    """Free-text ``type`` wins; known kinds use labels; unknown kinds are titled."""
    assert citations._bibtex_entry_heading(entries["wp"]) == "Working Paper"
    assert (
        citations._bibtex_entry_heading(entries["new_a"]) == "Journal or News Article"
    )
    assert citations._bibtex_entry_heading(entries["novel"]) == "Dataset"


def test_sorting_key_descends_by_year(entries):
    """Newer entries sort first."""
    style = citations.YearDescendingSortingStyle()
    keys = sorted(entries, key=lambda k: style.sorting_key(entries[k]))
    assert keys.index("new_a") < keys.index("old_a")


def test_citations_media_to_rst(tmp_path):
    """Pages get one section per heading, alphabetical, with 'Other' last."""
    (tmp_path / "templates").mkdir()
    shutil.copy(
        PUDL_DOCS_PATH / "templates" / "citations_media_page.rst.jinja",
        tmp_path / "templates",
    )
    (tmp_path / "citations_media").mkdir()
    for bibfile in citations.BIBTEX_FILES:
        (tmp_path / bibfile).write_text(BIB)

    citations.citations_media_to_rst(tmp_path)

    for path in citations.citations_media_page_paths(tmp_path):
        text = path.read_text()
        headings = [
            line
            for line in text.splitlines()
            if line in {"Dataset", "Journal or News Article", "Other", "Working Paper"}
        ]
        assert headings == [
            "Dataset",
            "Journal or News Article",
            "Working Paper",
            "Other",
        ]
        assert "new_a" in text
