"""Unit tests for :mod:`pudl.docs.data_dictionary`."""

import shutil

import pytest

from pudl import PUDL_DOCS_PATH
from pudl.docs import data_dictionary
from pudl.metadata.warnings import USAGE_WARNINGS, UsageWarning


@pytest.fixture
def docs_dir(tmp_path):
    """A minimal docs directory with the usage warnings template."""
    (tmp_path / "templates").mkdir()
    shutil.copy(
        PUDL_DOCS_PATH / "templates" / "usage_warnings.rst.jinja",
        tmp_path / "templates",
    )
    (tmp_path / "data_dictionaries").mkdir()
    return tmp_path


def test_usage_warnings_to_rst(docs_dir, mocker):
    """Titled warnings are listed alphabetically with labels, details and no escaping."""
    mocker.patch.object(
        data_dictionary,
        "USAGE_WARNINGS",
        {
            "zebra": UsageWarning(title="Zebra", description="Stripes & `links <u>`_."),
            "apple": UsageWarning(
                title="apple", description="Short.", details="Line one.\n\nLine two."
            ),
            "untitled": UsageWarning(description="Not on the page."),
        },
    )

    data_dictionary.usage_warnings_to_rst(docs_dir)

    text = (docs_dir / data_dictionary.USAGE_WARNINGS_RST).read_text()
    # Sorted case-insensitively by title, and untitled warnings are omitted.
    assert text.index(".. _apple:") < text.index(".. _zebra:")
    assert "untitled" not in text
    assert "Not on the page." not in text
    # Details are indented under the entry, with the blank line preserved.
    assert "apple:\n  Short.\n\n  Line one.\n\n  Line two.\n" in text
    # RST markup is passed through unescaped.
    assert "Stripes & `links <u>`_." in text


def test_usage_warnings_page_covers_real_warnings(docs_dir):
    """Every titled warning appears on the generated page."""
    data_dictionary.usage_warnings_to_rst(docs_dir)

    text = (docs_dir / data_dictionary.USAGE_WARNINGS_RST).read_text()
    titled = {k: w for k, w in USAGE_WARNINGS.items() if w.title}
    assert titled
    for key, warning in titled.items():
        assert f".. _{key}:\n" in text
        assert f"{warning.title}:\n" in text
        assert warning.description in text
    # Other documentation links to this anchor, so it must not change.
    assert ".. _harvested:\n" in text


def test_usage_warnings_have_descriptions():
    """Every warning has the description embedded in table descriptions."""
    assert all(w.description.strip() for w in USAGE_WARNINGS.values())
