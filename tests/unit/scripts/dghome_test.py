"""Unit tests for `dghome` date parsing and CLI behavior."""

import os
from datetime import datetime
from pathlib import Path

from click.testing import CliRunner

from pudl.scripts import dghome


def make_dagster_home(
    tmp_path: Path,
    entries: list[tuple[str, int, datetime]],
) -> dict[str, Path]:
    """Build fake DAGSTER_HOME tree for dghome tests.

    Creates `$DAGSTER_HOME/storage/<name>/payload.bin` for each entry tuple.
    Each tuple contains directory name, payload size in bytes, and directory
    modification time as a datetime. Returns paths keyed by directory
    name so tests can assert on expected ordering and deletion behavior.
    """
    storage = tmp_path / "storage"
    storage.mkdir()

    created: dict[str, Path] = {}
    for name, size, mtime in entries:
        run_dir = storage / name
        run_dir.mkdir()
        (run_dir / "payload.bin").write_bytes(b"x" * size)
        os.utime(run_dir, (mtime.timestamp(), mtime.timestamp()))
        created[name] = run_dir

    return created


def test_parse_date_relative() -> None:
    """Verify relative dghome cutoffs resolve from current local time."""
    fake_now = datetime(2026, 5, 12, 9, 15, 0)

    assert dghome._parse_date("10d", fake_now) == datetime(2026, 5, 2, 9, 15, 0)
    assert dghome._parse_date("1w", fake_now) == datetime(2026, 5, 5, 9, 15, 0)
    assert dghome._parse_date("1m", fake_now) == datetime(2026, 4, 12, 9, 15, 0)


def test_ls_lists_matching_directories(tmp_path: Path, monkeypatch) -> None:
    """CLI `ls` should show matching directories and cumulative size."""
    make_dagster_home(
        tmp_path,
        [
            (
                "11111111-1111-1111-1111-111111111111",
                2048,
                datetime(2026, 5, 1, 12, 0, 0),
            ),
            (
                "22222222-2222-2222-2222-222222222222",
                4096,
                datetime(2026, 5, 2, 12, 0, 0),
            ),
            (
                "33333333-3333-3333-3333-333333333333",
                8192,
                datetime(2026, 5, 3, 12, 0, 0),
            ),
        ],
    )
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))

    result = CliRunner().invoke(dghome.dghome, ["ls", "2026-05-02 18:00:00"])

    assert result.exit_code == 0
    assert "11111111-1111-1111-1111-111111111111" in result.output
    assert "22222222-2222-2222-2222-222222222222" in result.output
    assert "33333333-3333-3333-3333-333333333333" not in result.output
    assert "2K" in result.output
    assert "6K" in result.output


def test_rm_aborts_on_confirmation_failure(tmp_path: Path, monkeypatch) -> None:
    """CLI `rm` should abort cleanly and leave directories in place."""
    created = make_dagster_home(
        tmp_path,
        [
            (
                "11111111-1111-1111-1111-111111111111",
                2048,
                datetime(2026, 5, 1, 12, 0, 0),
            )
        ],
    )
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))

    result = CliRunner().invoke(
        dghome.dghome,
        ["rm", "2026-05-01 18:00:00"],
        input="n\n",
    )

    assert result.exit_code == 1
    assert "About to remove these directories:" in result.output
    assert str(created["11111111-1111-1111-1111-111111111111"]) in result.output
    assert "Remove these directories? [y/N]: n" in result.output
    assert "Aborted!" in result.output
    assert created["11111111-1111-1111-1111-111111111111"].exists()
