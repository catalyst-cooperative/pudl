"""List every upstream ancestor of an asset and its ``dagster/priority`` tag.

This is a debugging aid for rebalancing the PUDL Dagster DAG. Given one of the
last-to-materialize assets, it prints the full set of assets on that asset's
"hot path" (all transitive upstream inputs) along with the ``dagster/priority``
value in each asset's ``op_tags``. Assets with no explicit priority are reported
with a ``null`` value.

The output is a JSON array, sorted by descending priority then asset name, so
it's easy to spot ancestors that still need :data:`HOT_PATH_OP_TAGS
<pudl.dagster.op_tags.HOT_PATH_OP_TAGS>` to keep them ahead of unrelated
"island" assets that merely fill open slots.

Caveat: ``dagster/priority`` lives on the op, so graph-backed assets (the raw
``*__all_dfs`` extraction assets, the record-linkage ``@graph`` models) have no
single op and always report ``null`` here even when their underlying ops are
tagged. Check those definitions directly.

Example::

    pixi run hot_path out_ferc714__summarized_demand
"""

import difflib
import json
import sys

import click

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

PRIORITY_TAG = "dagster/priority"


def _op_priority(node) -> int | None:
    """Return the ``dagster/priority`` op tag for an asset node, if any."""
    assets_def = getattr(node, "assets_def", None)
    try:
        op = assets_def.op if assets_def is not None else None
    except Exception:  # noqa: BLE001 - external/source assets have no op
        op = None
    tags = getattr(op, "tags", {}) or {}
    value = tags.get(PRIORITY_TAG)
    return None if value is None else int(value)


Row = dict[str, str | int | None]


def _sort_key(row: Row) -> tuple[int, str]:
    """Sort rows by descending priority, then ancestor name.

    Ancestors with no explicit ``dagster/priority`` sort after any that have one.
    """
    priority = row[PRIORITY_TAG]
    rank = -int(priority) if priority is not None else 1
    return (rank, str(row["ancestor"]))


def hot_path(asset_name: str) -> list[Row]:
    """Collect every ancestor of ``asset_name`` and its priority tag."""
    from pudl.definitions import defs

    asset_graph = defs.resolve_asset_graph()
    keys_by_name = {
        key.to_user_string(): key for key in asset_graph.get_all_asset_keys()
    }

    if asset_name not in keys_by_name:
        suggestions = difflib.get_close_matches(
            asset_name, keys_by_name, n=20, cutoff=0.5
        ) or [
            name for name in sorted(keys_by_name) if asset_name.lower() in name.lower()
        ]
        message = (
            f"No asset named {asset_name!r} exists in the PUDL Dagster definitions."
        )
        if suggestions:
            listed = "\n  ".join(suggestions[:20])
            message += f"\n\nDid you mean one of these?\n  {listed}"
        else:
            message += "\n\nRun `pixi run dg list defs` to see the available assets."
        raise click.ClickException(message)

    target = keys_by_name[asset_name]

    ancestors: set = set()
    stack = list(asset_graph.get(target).parent_keys)
    while stack:
        key = stack.pop()
        if key in ancestors:
            continue
        ancestors.add(key)
        stack.extend(asset_graph.get(key).parent_keys)

    rows: list[Row] = [
        {
            "ancestor": key.to_user_string(),
            PRIORITY_TAG: _op_priority(asset_graph.get(key)),
        }
        for key in ancestors
    ]
    rows.sort(key=_sort_key)
    return rows


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("asset_name")
def main(asset_name: str) -> None:
    """Print upstream ancestors of ASSET_NAME and their dagster/priority tags."""
    rows = hot_path(asset_name)
    click.echo(json.dumps(rows, indent=2))


if __name__ == "__main__":
    sys.exit(main())
