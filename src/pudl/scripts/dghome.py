"""Manage UUID-named directories in a Dagster storage directory.

This CLI helps manage the Dagster storage directory to reclaim disk space by
listing and removing run artifacts (represented as UUID-named directories)
based on modification date.

Examples:
    List all UUID-named directories:
        dghome ls

    List directories modified on or before 2026-01-15:
        dghome ls 2026-01-15

    List directories modified 10 days or more ago:
        dghome ls 10d

    Remove directories modified 1 month or more ago:
        dghome rm 1m
"""

import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import click

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _parse_date(date_str: str) -> datetime:
    """Parse a cutoff datetime from one of several formats.

    Accepted formats:
      YYYY-MM-DD HH:MM  — exact datetime; seconds set to 59
      YYYY-MM-DD        — date only; cutoff is 23:59:59 on that day
      <N>d / <N>w / <N>m — relative; cutoff is 23:59:59 N days/weeks/months ago
    """
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(second=59)
        except ValueError as e:
            raise click.BadParameter(str(e)) from e

    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
        except ValueError as e:
            raise click.BadParameter(str(e)) from e

    m = re.match(r"^(\d+)([dwm])$", date_str, re.IGNORECASE)
    if not m:
        raise click.BadParameter(
            f"Use YYYY-MM-DD, 'YYYY-MM-DD HH:MM', or a duration like 10d, 4w, 1m — got '{date_str}'."
        )
    num, unit = int(m.group(1)), m.group(2).lower()
    delta = {
        "d": timedelta(days=num),
        "w": timedelta(weeks=num),
        "m": timedelta(days=num * 30),
    }[unit]
    return (datetime.now() - delta).replace(hour=23, minute=59, second=59)


def _human_readable(kb: int) -> str:
    """Format a kilobyte count as a human-readable string (e.g. 1.2M, 3.4G)."""
    if kb < 1024:
        return f"{kb}K"
    if kb < 1_048_576:
        return f"{kb / 1024:.1f}M"
    return f"{kb / 1_048_576:.1f}G"


# Color thresholds matching the bash script:
#   green < 100 MB, yellow < 1 GB, orange < 5 GB, red >= 5 GB
def _row_color(size_kb: int) -> str | tuple[int, int, int]:
    if size_kb < 102_400:
        return "green"
    if size_kb < 1_048_576:
        return "yellow"
    if size_kb < 5_242_880:
        return (255, 175, 95)  # ANSI 256-color 215 (orange)
    return "red"


def _collect(cutoff_ts: float | None) -> list[dict]:
    """Return UUID dirs from $DAGSTER_HOME/storage, filtered to mtime <= cutoff_ts.

    Raises click.ClickException if DAGSTER_HOME is unset or the storage dir is missing.
    Results are sorted by modification time (oldest first).
    """
    dagster_home = os.environ.get("DAGSTER_HOME")
    if not dagster_home:
        raise click.ClickException("DAGSTER_HOME is not set.")
    storage = Path(dagster_home) / "storage"
    if not storage.is_dir():
        raise click.ClickException(f"{storage} does not exist.")

    entries = []
    for d in storage.iterdir():
        if not (d.is_dir() and UUID_PATTERN.match(d.name)):
            continue
        mtime = d.stat().st_mtime
        if cutoff_ts is not None and mtime > cutoff_ts:
            continue
        size_kb = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) // 1024
        entries.append(
            {
                "path": d,
                "mtime": mtime,
                "size_kb": size_kb,
                "size_human": _human_readable(size_kb),
                "mdatetime": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
            }
        )
    entries.sort(key=lambda e: e["mtime"])
    return entries


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def dghome() -> None:
    """Manage UUID-named directories in Dagster storage."""


@dghome.command()
@click.argument("date", required=False, default=None)
def ls(date: str | None) -> None:  # noqa: D301
    """List UUID directories, optionally filtered by modification date.

    Without DATE, lists all directories. With DATE, lists only those
    last modified on or before DATE.

    
    DATE formats:
      YYYY-MM-DD HH:MM  exact datetime, e.g. '2026-01-15 14:30'
      YYYY-MM-DD        end of that day, e.g. 2026-01-15
      <N>d              N days ago,  e.g. 10d
      <N>w              N weeks ago, e.g. 4w
      <N>m              N months ago, e.g. 1m
    """
    cutoff_ts = _parse_date(date).timestamp() if date else None
    entries = _collect(cutoff_ts)

    if not entries:
        msg = (
            f"No directories found on or before {date}."
            if date
            else "No directories found."
        )
        click.echo(msg, err=True)
        return

    click.echo(
        click.style(
            f"{'#':<4} {'SIZE':<7} {'TOTAL':<7} {'DATETIME':<16} PATH", bold=True
        )
    )
    total_kb = 0
    for i, e in enumerate(entries, 1):
        total_kb += e["size_kb"]
        line = f"{i:<4} {e['size_human']:<7} {_human_readable(total_kb):<7} {e['mdatetime']:<16} {e['path'].name}"
        click.echo(click.style(line, fg=_row_color(e["size_kb"])))


@dghome.command()
@click.argument("date", required=False, default=None)
def rm(date: str | None) -> None:  # noqa: D301
    """Remove UUID directories last modified on or before DATE.

    Without DATE, reports what would be removed but does nothing.

    
    DATE formats:
      YYYY-MM-DD HH:MM  exact datetime, e.g. '2026-01-15 14:30'
      YYYY-MM-DD        end of that day, e.g. 2026-01-15
      <N>d              N days ago,  e.g. 10d
      <N>w              N weeks ago, e.g. 4w
      <N>m              N months ago, e.g. 1m
    """
    cutoff_ts = _parse_date(date).timestamp() if date else None
    entries = _collect(cutoff_ts)

    if not entries:
        msg = (
            f"No directories found on or before {date}."
            if date
            else "No directories found."
        )
        click.echo(msg, err=True)
        return

    total = _human_readable(sum(e["size_kb"] for e in entries))

    if cutoff_ts is None:
        click.echo(
            f"Found {len(entries)} directories totalling {total}. Provide a date to remove them."
        )
        return

    for e in entries:
        try:
            shutil.rmtree(e["path"])
        except OSError as exc:
            click.echo(f"Warning: failed to remove {e['path']}: {exc}", err=True)
    click.echo(f"Removed {len(entries)} directories totalling {total}.")


def main() -> None:
    """Entry point for the dghome CLI."""
    dghome()


if __name__ == "__main__":
    main()
