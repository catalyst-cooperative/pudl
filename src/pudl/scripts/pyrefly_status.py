"""Report on the current status of pyrefly typing in the repo.

Meant for the weekly baseline-ratcheting workflow: pick the module or error kind
with the biggest count, fix it, then shrink :file:`.pyrefly-baseline.json`.
"""

import json
import subprocess
from collections import Counter
from pathlib import Path

import click


def _run_pyrefly(args: list[str]) -> dict:
    """Run a pyrefly subcommand and return its JSON stdout, parsed.

    Raises ``click.ClickException`` if pyrefly can't be invoked or produces
    unparsable output.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ["pyrefly", *args],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise click.ClickException(
            "Could not find the `pyrefly` binary on PATH."
        ) from exc
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise click.ClickException(
            f"Could not parse pyrefly's JSON output: {exc}"
        ) from exc


def _collect_live_errors() -> list[dict]:
    """Run pyrefly directly and return its full, per-occurrence error list."""
    return _run_pyrefly(["check", "--output-format", "json"])["errors"]


def _collect_coverage_report() -> dict:
    """Run ``pyrefly coverage report`` on ``src/`` and return its parsed JSON.

    Restricted to ``src/`` since test modules aren't expected to be fully typed. The
    returned dict has ``module_reports`` (a list, one entry per module) and
    ``summary`` (aggregate counts and coverage across all of ``src/``).
    """
    return _run_pyrefly(["coverage", "report", "src"])


def _print_counts(errors: list[dict], key: str, header: str, top: int | None) -> None:
    counts = Counter(error[key] for error in errors)
    rows = counts.most_common(top)
    width = max(len(name) for name, _ in rows)
    click.echo(f"{header} ({len(counts)} distinct, {sum(counts.values())} total):")
    for name, count in rows:
        click.echo(f"  {name:<{width}}  {count}")
    click.echo()


def _relative_path(path: str) -> str:
    """Display a coverage report path relative to the repo root when possible."""
    try:
        return str(Path(path).resolve().relative_to(Path.cwd()))
    except ValueError:
        return path


_COVERAGE_COLUMNS = ["typable", "typed", "any", "untyped", "coverage"]


def _print_coverage_table(report: dict, sort_by: str, top: int | None) -> None:
    rows = [
        {
            "module": _relative_path(module_report["path"]),
            "typable": module_report["n_typable"],
            "typed": module_report["n_typed"],
            "any": module_report["n_any"],
            "untyped": module_report["n_untyped"],
            "coverage": module_report["coverage"],
        }
        for module_report in report["module_reports"]
    ]
    n_total = len(rows)
    # Nothing to fix in a fully-typed module, so skip it.
    rows = [row for row in rows if row["untyped"] > 0]
    click.echo(f"Overall src/ type coverage: {report['summary']['coverage']:.1f}%")
    click.echo(
        f"{len(rows)} of {n_total} src/ modules have incomplete type coverage:\n"
    )
    if not rows:
        return
    # Worst coverage (or biggest untyped count) first, since that's what you'd fix.
    reverse = sort_by != "coverage"
    rows.sort(key=lambda row: row[sort_by], reverse=reverse)
    rows = rows[:top]

    module_width = max(len(row["module"]) for row in rows)
    header = f"{'module':<{module_width}}  {'typable':>7}  {'typed':>7}  {'any':>5}  {'untyped':>7}  {'coverage':>8}"
    click.echo(header)
    click.echo("-" * len(header))
    for row in rows:
        click.echo(
            f"{row['module']:<{module_width}}  {row['typable']:>7}  {row['typed']:>7}  "
            f"{row['any']:>5}  {row['untyped']:>7}  {row['coverage']:>7.1f}%"
        )
    click.echo()


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main() -> None:
    """Report on the current status of pyrefly typing in the repo.

    Meant for the weekly baseline-ratcheting workflow: pick the module or error kind
    with the biggest count, fix it, then shrink .pyrefly-baseline.json.
    """


@main.command()
@click.option(
    "--by",
    type=click.Choice(["module", "kind", "both"]),
    default="both",
    show_default=True,
    help="Group the summary by module (file path), error kind, or both.",
)
@click.option(
    "--top",
    default=None,
    type=int,
    help="Only show the top N rows of each table. Shows all rows by default.",
)
def errors(by: str, top: int | None) -> None:
    """Summarize remaining pyrefly type errors by module and/or error kind.

    Runs pyrefly itself to get an exact, per-occurrence error count.
    """
    remaining_errors = _collect_live_errors()
    click.echo(f"Summarizing {len(remaining_errors)} pyrefly errors.\n")

    if by in ("module", "both"):
        _print_counts(remaining_errors, key="path", header="Errors per module", top=top)
    if by in ("kind", "both"):
        _print_counts(remaining_errors, key="name", header="Errors per kind", top=top)


@main.command()
@click.option(
    "--sort-by",
    type=click.Choice(["coverage", *_COVERAGE_COLUMNS]),
    default="coverage",
    show_default=True,
    help="Column to sort modules by. Non-coverage columns sort biggest first.",
)
@click.option(
    "--top",
    default=None,
    type=int,
    help="Only show the top N modules. Shows all modules by default.",
)
def coverage(sort_by: str, top: int | None) -> None:
    """Summarize `pyrefly coverage report`'s per-module type coverage as a table.

    Sorted by ascending coverage (worst first) by default, so the least-typed
    modules -- the best ratcheting targets -- show up at the top.
    """
    report = _collect_coverage_report()
    _print_coverage_table(report, sort_by=sort_by, top=top)


if __name__ == "__main__":
    main()
