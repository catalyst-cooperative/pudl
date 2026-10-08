"""Decide which PUDL Diff reports a build produces, and produce them.

Branch and nightly builds compare their outputs against the last nightly build and the
last stable release from inside the ETL (see ``pudl.dagster.assets.core.pudl_diff``).
A stable release instead makes its report at deploy time, against the previous stable
release, so that the roots recorded in the report are the permanent versioned paths
rather than the ephemeral build outputs. That report is the durable record of how the
data changed from one release to the next.
"""

import re
import shutil
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import s3fs
from pudl_diff.dataset import PudlDiffDataset
from pudl_diff.dataset_report import REPORT_FILENAME, PudlDiffReport
from pudl_diff.runner import run_dataset_diff
from pudl_diff.table_report import DiffOptions
from upath import UPath

from pudl.deploy.pudl import (
    PUDL_DIFF_DIRNAME,
    PUDL_DIFF_ZIP_NAME,
    DeploymentPlan,
    DeploymentType,
    get_deployment_type_from_tag,
)
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

PUBLIC_BUCKET = "pudl.catalyst.coop"
NIGHTLY_ROOT = f"s3://{PUBLIC_BUCKET}/nightly/"
STABLE_ROOT = f"s3://{PUBLIC_BUCKET}/stable/"
_STABLE_TAG_REGEX = re.compile(r"v(\d{4})\.(\d{1,2})\.(\d{1,2})")
ALL_ROWS = DiffOptions(max_compare_rows=sys.maxsize)
"""Never skip the row-level comparison of a table for having too many rows."""
_PREFERRED_TAG_PREFIXES = ("v20", "nightly-", "branch-")
"""Prefixes of the tags that name a dataset, in decreasing order of preference."""
_LOCAL_RIGHT_LABEL = "local"


@dataclass(frozen=True)
class DiffPlan:
    """One comparison of a build's outputs against a baseline dataset."""

    left_root: str
    """Root of the baseline (left) dataset."""
    right_label: str
    """Names the build being compared in the report's directory name, unless its
    outputs have git tags of their own to name it with."""
    right_display_root: str | None = None
    """Root to record in the report for the build's outputs, if not where they
    are actually read from."""
    left_label: str | None = None
    """Names the baseline in the report's directory name. If not given, it is
    taken from the baseline's own git tags."""


def public_dataset(root: str, **kwargs) -> PudlDiffDataset:
    """A dataset at ``root``, read anonymously if it's in PUDL's public S3 bucket."""
    location = UPath(root, anon=True) if root.startswith("s3://") else UPath(root)
    return PudlDiffDataset(location, **kwargs)


def plan_build_diffs(
    *,
    git_tag: str | None,
    build_id: str | None,
    gcs_output: str | None = None,
    left_root: str | None = None,
) -> list[DiffPlan]:
    """Decide which reports the ETL should produce for a build.

    Args:
        git_tag: The build's tag, e.g. ``nightly-2026-09-16`` or ``v2026.10.0``, or
            ``None`` if this isn't a tagged build (e.g. a local run).
        build_id: The unique ID of the build.
        gcs_output: Where the build's outputs are saved, if they are; the report
            records this as the location of the right dataset.
        left_root: A baseline to compare against instead of the defaults. This is
            always the only comparison, whatever the build type.

    Returns:
        A build against a tag other than stable's compares against the last
        nightly build and the last stable release. A stable build has none, since
        it makes its report when it's deployed. With no tag it's the last nightly
        only, like the CLI.
    """
    deploy_type = get_deployment_type_from_tag(git_tag) if git_tag else None
    if deploy_type == DeploymentType.STABLE and left_root is None:
        return []

    if deploy_type is None:
        right_label = _LOCAL_RIGHT_LABEL
    elif deploy_type == DeploymentType.BRANCH:
        right_label = build_id or git_tag or _LOCAL_RIGHT_LABEL
    else:
        right_label = git_tag or _LOCAL_RIGHT_LABEL
    right_display_root = f"{gcs_output.rstrip('/')}/parquet" if gcs_output else None

    if left_root is not None:
        left_roots = [left_root]
    elif deploy_type is None:
        left_roots = [NIGHTLY_ROOT]
    else:
        left_roots = [NIGHTLY_ROOT, STABLE_ROOT]
    return [
        DiffPlan(
            left_root=root,
            right_label=right_label,
            right_display_root=right_display_root,
        )
        for root in left_roots
    ]


def _sanitize(label: str) -> str:
    return re.sub(r"[^\w.\-]+", "-", label).strip("-")


def _tag_preference(tag: str) -> int:
    """Rank a tag for naming a dataset: lower is more legible, so preferred."""
    for rank, prefix in enumerate(_PREFERRED_TAG_PREFIXES):
        if tag.startswith(prefix):
            return rank
    return len(_PREFERRED_TAG_PREFIXES)


def _preferred_tag(dataset: PudlDiffDataset) -> str | None:
    """The most legible of a dataset's git tags, if it has any.

    A dataset can have several tags on its git commit. Prefer versioned release tags,
    then nightly build tags, then branch build tags, then any other tag, and otherwise
    take the first of those in the order the dataset lists them.
    """
    tags = dataset.provenance().git_tags or []
    return min(tags, key=_tag_preference) if tags else None


def dataset_label(dataset: PudlDiffDataset, default: str | None = None) -> str:
    """Name a dataset by its preferred git tag.

    Failing that, use ``default``, and failing that a short form of its ID.
    """
    if label := _preferred_tag(dataset) or default:
        return _sanitize(label)
    if dataset_id := dataset.provenance().id:
        return _sanitize(dataset_id[:8])
    return "baseline"


def report_dir_name(left_label: str, right_label: str) -> str:
    """Name the directory holding the report on ``left_label`` vs ``right_label``."""
    return f"{_sanitize(left_label)}-vs-{_sanitize(right_label)}"


def run_diff_plan(
    plan: DiffPlan,
    right: PudlDiffDataset,
    reports_dir: Path,
) -> PudlDiffReport | None:
    """Compare ``right`` against a plan's baseline, and save the report.

    The report and its Parquet outputs are written to a directory named for the two
    datasets, within ``reports_dir``. Each is named by its preferred git tag (see :func:`dataset_label`),
    or for the build being compared, by the plan's ``right_label`` if it has none.
    Every table is compared row by row, however large, since streaming keeps the
    memory needed bounded.

    Returns:
        The report, or ``None`` if the baseline couldn't be found or read, which is
        logged as a warning.
    """
    left = public_dataset(plan.left_root)
    try:
        left_label = plan.left_label or dataset_label(left)
    except (OSError, ValueError) as e:
        logger.warning(
            f"Skipping the PUDL Diff against {plan.left_root}: couldn't read its "
            f"datapackage descriptor: {e!r}"
        )
        return None
    right_label = dataset_label(right, default=plan.right_label)
    output_path = reports_dir / report_dir_name(left_label, right_label)
    logger.info(f"Comparing against {plan.left_root}; writing to {output_path}.")
    report = run_dataset_diff(left, right, output_path, options=ALL_ROWS)
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / REPORT_FILENAME).write_text(report.model_dump_json(indent=2))
    return report


def list_stable_release_tags(fs: s3fs.S3FileSystem | None = None) -> list[str]:
    """The versions of all stable releases in PUDL's public S3 bucket."""
    fs = fs or s3fs.S3FileSystem(anon=True)
    names = (path.rstrip("/").rsplit("/", 1)[-1] for path in fs.ls(PUBLIC_BUCKET))
    return [name for name in names if _STABLE_TAG_REGEX.fullmatch(name)]


def previous_stable_tag(git_tag: str, released_tags: list[str]) -> str | None:
    """The latest release before ``git_tag``, if any, from among ``released_tags``."""

    def version(tag: str) -> tuple[int, ...]:
        match = _STABLE_TAG_REGEX.fullmatch(tag)
        assert match is not None  # noqa: S101
        return tuple(int(part) for part in match.groups())

    earlier = [tag for tag in released_tags if version(tag) < version(git_tag)]
    return max(earlier, key=version, default=None)


def zip_diff_reports(local_path: Path) -> Path | None:
    """Archive the ``pudl_diff`` directory in ``local_path``, if there is one.

    The archive is small, and it gets uploaded with the release to Zenodo.
    """
    reports_dir = local_path / PUDL_DIFF_DIRNAME
    if not reports_dir.is_dir():
        return None
    zip_path = local_path / PUDL_DIFF_ZIP_NAME
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(reports_dir.rglob("*")):
            if path.is_file():
                zf.write(path, arcname=path.relative_to(local_path))
    logger.info(f"Created PUDL Diff report archive: {zip_path}")
    return zip_path


def prepare_diff_reports(local_path: Path, plan: DeploymentPlan) -> None:
    """Get the PUDL Diff reports in prepared outputs ready to be distributed.

    A stable release replaces whatever reports its build carried with a comparison
    against the previous stable release, which fails the deployment if it can't be
    made, since a release's permanent record can't be added to later. Reports from a
    branch or nightly build are kept as they are. Either way they are archived.

    Args:
        local_path: The prepared outputs, as left by
            :func:`~pudl.deploy.pudl.prepare_outputs_for_distribution`.
        plan: What's being deployed.

    Raises:
        RuntimeError: If a stable release's report can't be made.
    """
    if plan.deploy_type == DeploymentType.STABLE:
        _replace_with_stable_report(local_path, plan)
    zip_diff_reports(local_path)


def _replace_with_stable_report(local_path: Path, plan: DeploymentPlan) -> None:
    shutil.rmtree(local_path / PUDL_DIFF_DIRNAME, ignore_errors=True)
    (local_path / PUDL_DIFF_ZIP_NAME).unlink(missing_ok=True)

    previous = previous_stable_tag(plan.git_tag, list_stable_release_tags())
    if previous is None:
        logger.warning(f"No stable release precedes {plan.git_tag}; no PUDL Diff.")
        return

    right = PudlDiffDataset(
        local_path,
        descriptor_name="pudl_parquet_datapackage.json",
        display_root=f"s3://{PUBLIC_BUCKET}/{plan.path_suffixes[0]}/",
    )
    diff_plan = DiffPlan(
        left_root=f"s3://{PUBLIC_BUCKET}/{previous}/",
        left_label=previous,
        right_label=plan.git_tag,
    )
    report = run_diff_plan(diff_plan, right, local_path / PUDL_DIFF_DIRNAME)
    if report is None:
        raise RuntimeError(f"Couldn't read the previous release, {previous}.")
    if report.error is not None:
        raise RuntimeError(f"The PUDL Diff against {previous} failed: {report.error}")
    logger.info(
        f"PUDL Diff against {previous}: {report.summary.changed_table_count} tables "
        f"changed, {report.summary.failed_table_count} failed."
    )
