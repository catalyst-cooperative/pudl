"""Unit tests for :mod:`pudl.docs.data_dictionary`."""

import shutil
from pathlib import Path

import pandas as pd
import pytest

from pudl import PUDL_DOCS_PATH
from pudl.docs import data_dictionary
from pudl.metadata.classes import Encoder, Package
from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.warnings import USAGE_WARNINGS, UsageWarning

# Real code tables to render: one with no code fixes, and one with.
PLAIN_CODES = "core_eia__codes_averaging_periods"
FIXED_CODES = "core_eia__codes_boiler_status"


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


@pytest.fixture
def full_docs_dir(tmp_path):
    """A docs directory with all the real templates and the output directories."""
    shutil.copytree(PUDL_DOCS_PATH / "templates", tmp_path / "templates")
    (tmp_path / data_dictionary.CODES_CSV_SUBDIR).mkdir(parents=True)
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


def test_encoder_to_rst_writes_csv_and_section(full_docs_dir):
    """A code table's CSV is written and its section links to that file."""
    encoder = Encoder.from_code_id(PLAIN_CODES)

    text = data_dictionary.encoder_to_rst(
        encoder,
        top_dir=full_docs_dir,
        csv_subdir=data_dictionary.CODES_CSV_SUBDIR,
        is_header=False,
    )

    csv_path = full_docs_dir / data_dictionary.CODES_CSV_SUBDIR / f"{PLAIN_CODES}.csv"
    pd.testing.assert_frame_equal(
        pd.read_csv(csv_path, keep_default_na=False).astype(str),
        encoder.df.reset_index(drop=True).astype(str),
        check_dtype=False,
    )
    assert f".. _{PLAIN_CODES}_:" in text
    assert f":file: /{data_dictionary.CODES_CSV_SUBDIR}/{PLAIN_CODES}.csv" in text
    assert "Fixed Codes" not in text


def test_encoder_to_rst_header_only_when_requested(full_docs_dir):
    """The page header is rendered only for the first encoder."""
    encoder = Encoder.from_code_id(PLAIN_CODES)
    kwargs = {
        "top_dir": full_docs_dir,
        "csv_subdir": data_dictionary.CODES_CSV_SUBDIR,
    }

    with_header = data_dictionary.encoder_to_rst(encoder, is_header=True, **kwargs)
    without_header = data_dictionary.encoder_to_rst(encoder, is_header=False, **kwargs)

    assert "PUDL Code Metadata" in with_header
    assert "PUDL Code Metadata" not in without_header


def test_encoder_to_rst_lists_code_fixes(full_docs_dir):
    """Encoders with non-standard code fixes get a table of them."""
    encoder = Encoder.from_code_id(FIXED_CODES)
    assert encoder.code_fixes

    text = data_dictionary.encoder_to_rst(
        encoder,
        top_dir=full_docs_dir,
        csv_subdir=data_dictionary.CODES_CSV_SUBDIR,
        is_header=False,
    )

    assert "Fixed Codes" in text
    for bad_code, good_code in encoder.code_fixes.items():
        assert f"* - {bad_code}\n    - {good_code}" in text


def test_encoders_to_rst_writes_one_header_and_all_sections(full_docs_dir):
    """All encoders land in one file, in order, under a single page header."""
    encoders = [Encoder.from_code_id(name) for name in (PLAIN_CODES, FIXED_CODES)]
    rst_path = full_docs_dir / data_dictionary.CODES_RST

    data_dictionary.encoders_to_rst(
        encoders,
        top_dir=full_docs_dir,
        csv_subdir=data_dictionary.CODES_CSV_SUBDIR,
        rst_path=str(rst_path),
    )

    text = rst_path.read_text()
    assert text.count("PUDL Code Metadata") == 1
    assert text.index(f".. _{PLAIN_CODES}_:") < text.index(f".. _{FIXED_CODES}_:")
    for name in (PLAIN_CODES, FIXED_CODES):
        csv_path = full_docs_dir / data_dictionary.CODES_CSV_SUBDIR / f"{name}.csv"
        assert csv_path.is_file()


def test_codes_to_rst_covers_every_code_table(full_docs_dir):
    """Each code table in CODE_METADATA gets a section and a CSV file."""
    data_dictionary.codes_to_rst(full_docs_dir)

    text = (full_docs_dir / data_dictionary.CODES_RST).read_text()
    csv_dir = full_docs_dir / data_dictionary.CODES_CSV_SUBDIR
    assert text.count("PUDL Code Metadata") == 1
    for name in CODE_METADATA:
        assert f".. _{name}_:" in text
        assert (csv_dir / f"{name}.csv").is_file()


def test_package_to_rst(full_docs_dir):
    """A package renders every resource under the data dictionary heading."""
    names = (PLAIN_CODES, FIXED_CODES)
    package = Package.from_resource_ids(resource_ids=names)
    path = Path(full_docs_dir / data_dictionary.DATA_DICTIONARY_RST)

    data_dictionary.package_to_rst(package, docs_dir=full_docs_dir, path=str(path))

    text = path.read_text()
    assert "PUDL Data Dictionary" in text
    for resource in package.get_sorted_resources():
        assert f".. _{resource.sphinx_ref_name}:" in text
        assert resource.name in text
