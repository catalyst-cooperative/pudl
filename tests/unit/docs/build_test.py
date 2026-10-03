"""Unit tests for the generated-file registry in :mod:`pudl.docs.build`."""

from pudl import PUDL_DOCS_PATH
from pudl.docs import build
from pudl.docs.data_sources import INCLUDED_SOURCES


def test_every_included_source_has_a_template():
    """Each data source page needs a child template to render from."""
    for name in INCLUDED_SOURCES:
        assert (PUDL_DOCS_PATH / "templates" / f"{name}_child.rst.jinja").is_file()


def test_generated_files_are_under_docs_dir(tmp_path):
    """Registered paths are rooted in the docs dir they were asked about."""
    paths = build.generated_files(tmp_path)
    assert len(paths) == len(set(paths))
    assert all(tmp_path in p.parents for p in paths)


def test_remove_generated_files(tmp_path):
    """Generated files and the CSV dir go away; hand-written files stay."""
    generated = build.generated_files(tmp_path)
    for path in generated:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("generated")
    csv_dir = tmp_path / build.CODES_CSV_SUBDIR
    csv_dir.mkdir(parents=True)
    (csv_dir / "codes.csv").write_text("a,b")
    keeper = tmp_path / "data_sources" / "index.rst"
    keeper.write_text("hand-written")

    build.remove_generated_files(tmp_path)

    assert not any(p.exists() for p in generated)
    assert not csv_dir.exists()
    assert keeper.read_text() == "hand-written"


def test_remove_generated_files_when_nothing_generated(tmp_path):
    """Cleanup is a no-op on a clean docs dir."""
    build.remove_generated_files(tmp_path)
