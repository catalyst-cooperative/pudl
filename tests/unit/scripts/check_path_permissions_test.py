"""Unit tests for the generic check_path_permissions CLI."""

import json
import logging
from pathlib import Path

from click.testing import CliRunner

from pudl.scripts import check_path_permissions


def test_check_write_access_writes_and_removes_canary(tmp_path: Path) -> None:
    """The write check should leave no residue behind on local paths."""
    check_path_permissions.check_write_access(
        check_path_permissions._build_upath(str(tmp_path), anon=False)
    )

    assert list(tmp_path.iterdir()) == []


def test_check_read_access_raises_for_missing_path(tmp_path: Path) -> None:
    """Readable-path checks should fail cleanly for missing paths."""
    missing_path = check_path_permissions._build_upath(
        str(tmp_path / "missing"), anon=False
    )

    try:
        check_path_permissions.check_read_access(missing_path)
    except Exception as exc:  # noqa: BLE001
        assert "not readable" in str(exc) or "does not exist" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected check_read_access to fail for missing path")


def test_main_defaults_to_read_and_write(tmp_path: Path) -> None:
    """With no flags, the CLI should perform both read and write checks."""
    result = CliRunner().invoke(check_path_permissions.main, [str(tmp_path)])

    assert result.exit_code == 0
    assert "Checking read access" in result.output
    assert "Checking write/delete access" in result.output
    assert list(tmp_path.iterdir()) == []


def test_main_json_reports_failures(tmp_path: Path, script_runner) -> None:
    """JSON output should summarize a failing read check for agents."""
    missing_path = tmp_path / "missing"

    result = script_runner.run(
        ["check_path_permissions", "--read", "--json", str(missing_path)],
        print_result=False,
    )

    assert not result.success
    payload = json.loads(result.stdout)
    assert payload["success"] is False
    assert payload["results"][0]["checks"]["read"]["requested"] is True
    assert payload["results"][0]["checks"]["read"]["success"] is False
    assert payload["results"][0]["checks"]["read"]["errors"]


def test_main_json_distinguishes_delete_failures(monkeypatch, tmp_path: Path) -> None:
    """JSON output should report delete failures after a successful write."""

    def fake_check_write_access(_path):
        raise check_path_permissions.PathPermissionError(
            check=check_path_permissions.PermissionCheck.DELETE,
            message="Unable to delete written canary",
        )

    monkeypatch.setattr(
        check_path_permissions,
        "check_write_access",
        fake_check_write_access,
    )

    result = CliRunner().invoke(
        check_path_permissions.main,
        ["--write", "--json", str(tmp_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["success"] is False
    assert payload["results"][0]["checks"]["write"]["success"] is True
    assert payload["results"][0]["checks"]["delete"]["success"] is False
    assert payload["results"][0]["checks"]["delete"]["errors"] == [
        "Unable to delete written canary"
    ]


def test_main_accepts_multiple_paths(tmp_path: Path) -> None:
    """The CLI should apply the same checks to each provided path."""
    other_path = tmp_path / "other"
    other_path.mkdir()

    result = CliRunner().invoke(
        check_path_permissions.main,
        ["--read", str(tmp_path), str(other_path)],
    )

    assert result.exit_code == 0
    assert result.output.count("Checking read access for:") == 2


def test_main_json_reports_multiple_path_results(tmp_path: Path) -> None:
    """JSON output should include one result object per requested path."""
    other_path = tmp_path / "other"
    other_path.mkdir()

    result = CliRunner().invoke(
        check_path_permissions.main,
        ["--read", "--json", str(tmp_path), str(other_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["success"] is True
    assert payload["paths"] == [str(tmp_path), str(other_path)]
    assert len(payload["results"]) == 2
    assert all(result_summary["success"] for result_summary in payload["results"])


def test_main_json_includes_expanded_ferceqr_deployment_paths(
    tmp_path: Path, monkeypatch
) -> None:
    """JSON output should include any FERCEQR deployment paths added by the flag."""
    other_path = tmp_path / "other"
    other_path.mkdir()
    monkeypatch.setattr(
        check_path_permissions,
        "_get_ferceqr_deployment_paths",
        lambda: [str(other_path)],
    )

    result = CliRunner().invoke(
        check_path_permissions.main,
        [
            "--read",
            "--json",
            "--check-ferceqr-deployment-paths",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["paths"] == [str(tmp_path), str(other_path)]
    assert len(payload["results"]) == 2


def test_main_json_handles_non_os_permission_exceptions(monkeypatch) -> None:
    """Cloud permission failures should be reported cleanly even if they are not OSErrors."""

    class FakePath:
        def __str__(self) -> str:
            return "gs://test.catalyst.coop"

        def exists(self) -> bool:
            raise RuntimeError(
                "Anonymous caller does not have storage.buckets.get access"
            )

    monkeypatch.setattr(
        check_path_permissions,
        "_build_upath",
        lambda path, anon: FakePath(),
    )

    result = CliRunner().invoke(
        check_path_permissions.main,
        ["--json", "--anon", "gs://test.catalyst.coop"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["success"] is False
    assert payload["results"][0]["success"] is False
    assert (
        "storage.buckets.get access"
        in payload["results"][0]["checks"]["read"]["errors"][0]
    )


def test_suppress_backend_tracebacks_restores_logger_levels() -> None:
    """Backend traceback suppression should be temporary and reversible."""
    logger = logging.getLogger("gcsfs.retry")
    original_level = logger.level
    logger.setLevel(logging.ERROR)

    try:
        with check_path_permissions._suppress_backend_tracebacks():
            assert logging.getLogger("gcsfs.retry").level == logging.CRITICAL
            assert logging.getLogger("gcsfs").level == logging.CRITICAL
    finally:
        logger.setLevel(original_level)

    assert logging.getLogger("gcsfs.retry").level == original_level


def test_build_upath_supports_anonymous_cloud_access(monkeypatch) -> None:
    """Anonymous mode should map to the appropriate cloud storage options."""
    captured_calls: list[tuple[str, dict[str, object]]] = []

    def fake_upath(path: str, **kwargs):
        captured_calls.append((path, kwargs))
        return path

    monkeypatch.setattr(check_path_permissions, "UPath", fake_upath)

    assert (
        check_path_permissions._build_upath("gs://bucket/prefix", anon=True)
        == "gs://bucket/prefix"
    )
    assert (
        check_path_permissions._build_upath("s3://bucket/prefix", anon=True)
        == "s3://bucket/prefix"
    )
    assert captured_calls == [
        ("gs://bucket/prefix", {"token": "anon"}),
        ("s3://bucket/prefix", {"anon": True}),
    ]
