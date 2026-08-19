"""Diff the pyrefly baseline against a git ref by content, not by line number.

Raw JSON diffs of ``.pyrefly-baseline.json`` are dominated by line/column churn from
ordinary code changes and pyrefly version bumps -- a single ``--update-baseline`` run
can touch hundreds of lines without any error actually appearing or disappearing. This
diffs baseline entries by ``(file, error code, description)`` instead, so the output
only shows errors that were genuinely fixed or newly introduced.
"""

import json
import subprocess
from pathlib import Path

import click


def _load_baseline_entries(
    ref: str | None, baseline_path: Path
) -> set[tuple[str, str, str]]:
    """Load baseline error entries from a git ref, or the working tree if ``ref`` is None."""
    if ref is None:
        text = baseline_path.read_text()
    else:
        result = subprocess.run(  # noqa: S603
            ["git", "show", f"{ref}:{baseline_path}"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
        text = result.stdout
    errors = json.loads(text)["errors"]
    return {(e["path"], e["name"], e["description"]) for e in errors}


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--ref",
    default="HEAD",
    show_default=True,
    help="Git ref to diff the working-tree baseline against.",
)
@click.option(
    "--baseline-path",
    default=".pyrefly-baseline.json",
    show_default=True,
    type=click.Path(path_type=Path),
    help="Path to the pyrefly baseline file, relative to the repo root.",
)
def main(ref: str, baseline_path: Path) -> None:
    """Diff a pyrefly baseline against a git ref by (file, code, description).

    Run this after ``pyrefly check --baseline .pyrefly-baseline.json --update-baseline``
    to sanity-check the regenerated baseline before committing it: the "fixed" list
    should match what you intentionally fixed, and the "newly baselined" list should
    only contain pre-existing issues you're deliberately deferring -- not something
    your own change introduced.
    """
    old = _load_baseline_entries(ref, baseline_path)
    new = _load_baseline_entries(None, baseline_path)

    removed = sorted(old - new)
    added = sorted(new - old)

    click.echo(f"{len(old)} entries at {ref!r} -> {len(new)} entries in working tree")

    click.echo(f"\n{len(removed)} fixed (present at {ref!r}, gone now):")
    for path, name, description in removed:
        click.echo(f"  - {path} [{name}] {description[:100]}")

    click.echo(f"\n{len(added)} newly baselined (absent at {ref!r}, present now):")
    for path, name, description in added:
        click.echo(f"  + {path} [{name}] {description[:100]}")

    if added:
        click.echo(
            "\nReview the 'newly baselined' entries above carefully -- confirm "
            "they're pre-existing issues you're deliberately deferring, not "
            "something your own change introduced."
        )


if __name__ == "__main__":
    main()
