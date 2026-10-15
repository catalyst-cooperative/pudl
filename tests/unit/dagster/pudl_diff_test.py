"""Unit tests for the pudl_diff Dagster asset."""

import dagster as dg
import pytest

from pudl.dagster.assets.core import pudl_diff as asset_module
from pudl.dagster.assets.core.pudl_diff import PudlDiffConfig, pudl_diff
from pudl.deploy.pudl_diff import DiffPlan
from pudl.workspace.setup import PudlPaths


@pytest.fixture
def context(tmp_path):
    paths = PudlPaths(pudl_input=tmp_path / "in", pudl_output=tmp_path / "out")
    return dg.build_asset_context(resources={"pudl_paths": paths})


def _invoke(context, **config) -> dg.MaterializeResult:
    result = pudl_diff(context, PudlDiffConfig(**config))
    assert isinstance(result, dg.MaterializeResult)
    return result


def test_pudl_diff_is_skipped_by_default(context, mocker, monkeypatch):
    monkeypatch.delenv("PUDL_DIFF_RUN", raising=False)
    plan = mocker.patch.object(asset_module, "plan_build_diffs")

    result = _invoke(context)

    assert result.metadata == {"skipped": True}
    plan.assert_not_called()


def test_pudl_diff_runs_when_asked_by_environment(context, mocker, monkeypatch):
    monkeypatch.setenv("PUDL_DIFF_RUN", "true")
    monkeypatch.delenv("GIT_TAG", raising=False)
    monkeypatch.setenv("PUDL_DIFF_LEFT_ROOT", "/my/baseline")
    plan = mocker.patch.object(
        asset_module,
        "plan_build_diffs",
        return_value=[DiffPlan("/my/baseline", "local")],
    )
    run = mocker.patch.object(asset_module, "run_diff_plan", return_value=None)

    result = _invoke(context)

    assert plan.call_args.kwargs["left_root"] == "/my/baseline"
    run.assert_called_once()
    assert result.metadata == {
        "/my/baseline skipped": dg.MetadataValue.bool(True),
    }


def test_pudl_diff_runs_when_asked_by_config(context, mocker, monkeypatch):
    monkeypatch.delenv("PUDL_DIFF_RUN", raising=False)
    monkeypatch.delenv("PUDL_DIFF_LEFT_ROOT", raising=False)
    plan = mocker.patch.object(asset_module, "plan_build_diffs", return_value=[])

    result = _invoke(context, run=True, left_root="/from/config")

    assert plan.call_args.kwargs["left_root"] == "/from/config"
    assert result.metadata == {"skipped": True}


def test_pudl_diff_never_fails_the_build(context, mocker, monkeypatch):
    monkeypatch.setenv("PUDL_DIFF_RUN", "true")
    mocker.patch.object(
        asset_module,
        "plan_build_diffs",
        return_value=[DiffPlan("/a", "r"), DiffPlan("/b", "r")],
    )
    mocker.patch.object(asset_module, "run_diff_plan", side_effect=OSError("boom"))

    result = _invoke(context)

    assert result.metadata is not None
    assert set(result.metadata) == {"/a error", "/b error"}
