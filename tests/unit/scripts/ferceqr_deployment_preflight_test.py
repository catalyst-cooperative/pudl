"""Unit tests for the FERCEQR deployment preflight CLI."""

from pathlib import Path

from click.testing import CliRunner
from upath import UPath

from pudl.scripts import ferceqr_deployment_preflight


def test_verify_path_is_writable_writes_and_removes_canary(tmp_path: Path) -> None:
    """The canary write check should leave no residue behind on local paths."""
    target = UPath(tmp_path)

    ferceqr_deployment_preflight.verify_path_is_writable(
        target,
        description="test target",
        build_id="build-123",
    )

    assert list(tmp_path.iterdir()) == []


def test_ensure_path_exists_raises_for_missing_path(tmp_path: Path) -> None:
    """Readable-path checks should fail cleanly for missing paths."""
    missing_path = UPath(tmp_path / "missing")

    try:
        ferceqr_deployment_preflight.ensure_path_exists(
            missing_path,
            description="missing archive",
        )
    except Exception as exc:  # noqa: BLE001
        assert "missing archive" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ensure_path_exists to fail for missing path")


def test_main_skips_deployment_checks_when_no_targets(
    monkeypatch, tmp_path: Path
) -> None:
    """The CLI should still validate archive and logs paths when deployment is disabled."""
    archive_path = tmp_path / "archive"
    logs_path = tmp_path / "logs"
    archive_path.mkdir()
    logs_path.mkdir()

    checked_paths: list[tuple[str, str]] = []

    def fake_load_deployment_targets(
        _deployment_config_path: str | None,
    ) -> list[UPath]:
        return []

    def fake_verify_path_is_writable(
        path: UPath, description: str, build_id: str
    ) -> None:
        checked_paths.append((description, str(path)))
        assert build_id == "build-123"

    monkeypatch.setattr(
        ferceqr_deployment_preflight,
        "load_deployment_targets",
        fake_load_deployment_targets,
    )
    monkeypatch.setattr(
        ferceqr_deployment_preflight,
        "verify_path_is_writable",
        fake_verify_path_is_writable,
    )

    result = CliRunner().invoke(
        ferceqr_deployment_preflight.main,
        [
            "--archive-path",
            str(archive_path),
            "--logs-path",
            str(logs_path),
            "--build-id",
            "build-123",
        ],
    )

    assert result.exit_code == 0
    assert checked_paths == [("FERC EQR logs destination", str(logs_path))]
    assert "skipping deployment checks" in result.output


def test_main_checks_each_deployment_target(monkeypatch, tmp_path: Path) -> None:
    """The CLI should run the same canary check for each resolved deployment target."""
    archive_path = tmp_path / "archive"
    logs_path = tmp_path / "logs"
    deploy_a = tmp_path / "deploy_a"
    deploy_b = tmp_path / "deploy_b"
    archive_path.mkdir()
    logs_path.mkdir()
    deploy_a.mkdir()
    deploy_b.mkdir()

    checked_paths: list[tuple[str, str]] = []

    monkeypatch.setattr(
        ferceqr_deployment_preflight,
        "load_deployment_targets",
        lambda _deployment_config_path: [UPath(deploy_a), UPath(deploy_b)],
    )

    def fake_verify_path_is_writable(
        path: UPath, description: str, build_id: str
    ) -> None:
        checked_paths.append((description, str(path)))
        assert build_id == "build-456"

    monkeypatch.setattr(
        ferceqr_deployment_preflight,
        "verify_path_is_writable",
        fake_verify_path_is_writable,
    )

    result = CliRunner().invoke(
        ferceqr_deployment_preflight.main,
        [
            "--archive-path",
            str(archive_path),
            "--logs-path",
            str(logs_path),
            "--build-id",
            "build-456",
        ],
    )

    assert result.exit_code == 0
    assert checked_paths == [
        ("FERC EQR logs destination", str(logs_path)),
        ("FERC EQR deployment target", str(deploy_a)),
        ("FERC EQR deployment target", str(deploy_b)),
    ]
