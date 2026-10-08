"""Unit tests for pudl.deploy.pudl_diff."""

import json
import zipfile
from pathlib import Path

import polars as pl
import pytest
from pudl_diff.dataset import PudlDiffDataset

from pudl.deploy import pudl_diff
from pudl.deploy.pudl import DeploymentPlan
from pudl.deploy.pudl_diff import (
    NIGHTLY_ROOT,
    STABLE_ROOT,
    DiffPlan,
    dataset_label,
    plan_build_diffs,
    prepare_diff_reports,
    previous_stable_tag,
    report_dir_name,
    run_diff_plan,
    zip_diff_reports,
)

BRANCH_TAG = "branch-2026-10-31-1939-3e5887d46-my-branch"


@pytest.mark.parametrize(
    ("git_tag", "expected_lefts", "expected_right"),
    [
        ("nightly-2026-09-16", [NIGHTLY_ROOT, STABLE_ROOT], "nightly-2026-09-16"),
        (BRANCH_TAG, [NIGHTLY_ROOT, STABLE_ROOT], "the-build-id"),
        ("v2026.10.0", [], "v2026.10.0"),
        (None, [NIGHTLY_ROOT], "local"),
    ],
)
def test_plan_build_diffs_by_build_type(git_tag, expected_lefts, expected_right):
    plans = plan_build_diffs(
        git_tag=git_tag,
        build_id="the-build-id",
        gcs_output="gs://builds.catalyst.coop/the-build-id",
    )

    assert [p.left_root for p in plans] == expected_lefts
    assert {p.right_label for p in plans} <= {expected_right}
    assert {p.right_display_root for p in plans} <= {
        "gs://builds.catalyst.coop/the-build-id/parquet"
    }


@pytest.mark.parametrize("git_tag", ["nightly-2026-09-16", "v2026.10.0", None])
def test_plan_build_diffs_left_root_override_is_the_only_comparison(git_tag):
    plans = plan_build_diffs(git_tag=git_tag, build_id=None, left_root="/my/baseline")

    assert [p.left_root for p in plans] == ["/my/baseline"]
    assert plans[0].right_display_root is None


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        (["v2026.9.0"], "v2026.9.0"),
        (["something", "nightly-2026-09-15"], "nightly-2026-09-15"),
        (["odd tag/1"], "odd-tag-1"),
        (None, "abcdef12"),
    ],
)
def test_dataset_label(tmp_path: Path, tags, expected):
    descriptor = {"id": "abcdef12-3456", "resources": []}
    if tags:
        descriptor["git_tags"] = tags
    (tmp_path / "datapackage.json").write_text(json.dumps(descriptor))

    assert dataset_label(PudlDiffDataset(tmp_path)) == expected


def test_report_dir_name():
    assert report_dir_name("v2026.9.0", "v2026.10.0") == "v2026.9.0-vs-v2026.10.0"
    assert report_dir_name("a b", "c/d") == "a-b-vs-c-d"


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        ("v2026.10.0", "v2026.9.1"),
        ("v2026.9.1", "v2026.9.0"),
        ("v2026.9.0", "v2025.12.0"),
        ("v2025.12.0", None),
    ],
)
def test_previous_stable_tag(current, expected):
    released = ["v2026.9.0", "v2025.12.0", "v2026.9.1", "v2026.10.0"]
    assert previous_stable_tag(current, released) == expected


def test_list_stable_release_tags(mocker):
    fs = mocker.MagicMock()
    fs.ls.return_value = [
        "pudl.catalyst.coop/nightly",
        "pudl.catalyst.coop/v2026.9.0",
        "pudl.catalyst.coop/v2025.12.0/",
        "pudl.catalyst.coop/stable",
    ]

    assert pudl_diff.list_stable_release_tags(fs) == ["v2026.9.0", "v2025.12.0"]


def _write_dataset(
    root: Path, ys: list[str], git_tags: list[str] | None = None
) -> PudlDiffDataset:
    root.mkdir(parents=True, exist_ok=True)
    resource = {
        "name": "t",
        "schema": {
            "fields": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "string"},
            ],
            "primaryKey": ["x"],
        },
    }
    descriptor = {"resources": [resource], "git_tags": git_tags}
    descriptor_name = "pudl_parquet_datapackage.json"
    (root / descriptor_name).write_text(json.dumps(descriptor))
    pl.DataFrame({"x": list(range(len(ys))), "y": ys}).write_parquet(root / "t.parquet")
    return PudlDiffDataset(root, descriptor_name=descriptor_name)


def _patch_public_dataset(mocker, baseline: PudlDiffDataset):
    return mocker.patch.object(pudl_diff, "public_dataset", return_value=baseline)


def test_run_diff_plan_writes_a_report_named_for_both_datasets(tmp_path: Path, mocker):
    baseline = _write_dataset(tmp_path / "left", ["a", "b"], ["nightly-2026-09-15"])
    right = _write_dataset(tmp_path / "right", ["a", "c"])
    _patch_public_dataset(mocker, baseline)
    plan = DiffPlan(left_root="s3://pudl.catalyst.coop/nightly/", right_label="mine")

    report = run_diff_plan(plan, right, tmp_path / "reports")

    assert report is not None
    assert report.summary.changed_table_count == 1
    report_dir = tmp_path / "reports" / "nightly-2026-09-15-vs-mine"
    assert (report_dir / "pudl_diff_report.json").exists()


def test_run_diff_plan_compares_the_rows_of_tables_of_any_size(tmp_path: Path, mocker):
    baseline = _write_dataset(tmp_path / "left", ["a"], ["nightly-2026-09-15"])
    right = _write_dataset(tmp_path / "right", ["a"])
    _patch_public_dataset(mocker, baseline)
    plan = DiffPlan(left_root="s3://pudl.catalyst.coop/nightly/", right_label="mine")

    report = run_diff_plan(plan, right, tmp_path / "reports")

    assert report is not None
    assert report.options.max_compare_rows > 10**12


def test_run_diff_plan_skips_an_unreadable_baseline(tmp_path: Path, mocker):
    right = _write_dataset(tmp_path / "right", ["a"])
    _patch_public_dataset(mocker, PudlDiffDataset(tmp_path / "nowhere"))
    plan = DiffPlan(left_root="s3://pudl.catalyst.coop/stable/", right_label="mine")

    assert run_diff_plan(plan, right, tmp_path / "reports") is None
    assert not (tmp_path / "reports").exists()


def test_zip_diff_reports(tmp_path: Path):
    report_dir = tmp_path / "pudl_diff" / "a-vs-b"
    report_dir.mkdir(parents=True)
    (report_dir / "pudl_diff_report.json").write_text("{}")

    zip_path = zip_diff_reports(tmp_path)

    assert zip_path == tmp_path / "pudl_diff.zip"
    assert zip_path is not None
    with zipfile.ZipFile(zip_path) as zf:
        assert zf.namelist() == ["pudl_diff/a-vs-b/pudl_diff_report.json"]


def test_zip_diff_reports_without_reports(tmp_path: Path):
    assert zip_diff_reports(tmp_path) is None
    assert not (tmp_path / "pudl_diff.zip").exists()


def test_prepare_diff_reports_keeps_a_nightly_builds_reports(tmp_path: Path):
    (tmp_path / "pudl_diff" / "x-vs-y").mkdir(parents=True)
    (tmp_path / "pudl_diff" / "x-vs-y" / "pudl_diff_report.json").write_text("{}")
    plan = DeploymentPlan(git_tag="nightly-2026-09-16", environment="production")

    prepare_diff_reports(tmp_path, plan)

    assert (tmp_path / "pudl_diff" / "x-vs-y" / "pudl_diff_report.json").exists()
    assert (tmp_path / "pudl_diff.zip").exists()


@pytest.fixture
def stable_plan() -> DeploymentPlan:
    return DeploymentPlan(git_tag="v2026.10.0", environment="production")


def test_prepare_diff_reports_replaces_a_stable_releases_reports(
    tmp_path: Path, mocker, stable_plan: DeploymentPlan
):
    # The build being deployed was a nightly, so it carries reports against it.
    stale = tmp_path / "outputs" / "pudl_diff" / "nightly-x-vs-nightly-y"
    stale.mkdir(parents=True)
    (stale / "pudl_diff_report.json").write_text("{}")
    (stale.parents[1] / "pudl_diff.zip").write_text("stale")
    _write_dataset(tmp_path / "outputs", ["a", "c"])
    baseline = _write_dataset(tmp_path / "previous", ["a", "b"])
    mocker.patch.object(
        pudl_diff, "list_stable_release_tags", return_value=["v2026.9.0", "v2026.10.0"]
    )
    public = _patch_public_dataset(mocker, baseline)

    prepare_diff_reports(tmp_path / "outputs", stable_plan)

    public.assert_called_once_with("s3://pudl.catalyst.coop/v2026.9.0/")
    reports = tmp_path / "outputs" / "pudl_diff"
    assert [p.name for p in reports.iterdir()] == ["v2026.9.0-vs-v2026.10.0"]
    report = json.loads(
        (reports / "v2026.9.0-vs-v2026.10.0" / "pudl_diff_report.json").read_text()
    )
    assert report["right_dataset"]["root"] == "s3://pudl.catalyst.coop/v2026.10.0/"
    with zipfile.ZipFile(tmp_path / "outputs" / "pudl_diff.zip") as zf:
        assert (
            "pudl_diff/v2026.9.0-vs-v2026.10.0/pudl_diff_report.json" in zf.namelist()
        )
        assert not any("nightly" in name for name in zf.namelist())


def test_prepare_diff_reports_stable_without_a_previous_release(
    tmp_path: Path, mocker, stable_plan: DeploymentPlan
):
    mocker.patch.object(pudl_diff, "list_stable_release_tags", return_value=[])

    prepare_diff_reports(tmp_path, stable_plan)

    assert not (tmp_path / "pudl_diff").exists()


def test_prepare_diff_reports_fails_a_stable_release_if_the_baseline_is_unreadable(
    tmp_path: Path, mocker, stable_plan: DeploymentPlan
):
    _write_dataset(tmp_path / "outputs", ["a"])
    mocker.patch.object(
        pudl_diff, "list_stable_release_tags", return_value=["v2026.9.0"]
    )
    _patch_public_dataset(mocker, PudlDiffDataset(tmp_path / "nowhere"))

    with pytest.raises(RuntimeError, match=r"v2026\.9\.0"):
        prepare_diff_reports(tmp_path / "outputs", stable_plan)


def test_prepare_diff_reports_fails_a_stable_release_if_the_diff_fails(
    tmp_path: Path, mocker, stable_plan: DeploymentPlan
):
    # No tables in common, so the run as a whole fails.
    _write_dataset(tmp_path / "outputs", ["a"])
    baseline = _write_dataset(tmp_path / "previous", ["a"])
    (tmp_path / "previous" / "t.parquet").rename(tmp_path / "previous" / "u.parquet")
    mocker.patch.object(
        pudl_diff, "list_stable_release_tags", return_value=["v2026.9.0"]
    )
    _patch_public_dataset(mocker, baseline)

    with pytest.raises(RuntimeError, match="failed"):
        prepare_diff_reports(tmp_path / "outputs", stable_plan)


def test_primary_keys_fall_back_to_pudls_own_metadata(tmp_path: Path):
    """A dataset with no datapackage of its own gets its primary keys from PUDL."""
    dataset = PudlDiffDataset(tmp_path)

    assert dataset.primary_key("core_eia__codes_wet_dry_bottom") == ["code"]
