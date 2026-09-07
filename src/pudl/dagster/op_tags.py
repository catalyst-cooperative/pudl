"""Shared Dagster ``op_tags`` constants for PUDL assets.

Keep op-tag dictionaries that are applied across many asset modules here, so they
have a single canonical definition to import rather than being redefined in each
module. This is deliberately a dependency-free leaf module: ``pudl/__init__.py``
eagerly imports the whole package tree, so anything imported here risks a circular
import when an ``extract``/``transform`` module imports it back.
"""

# Assets for datasets that are extracted and transformed but not yet integrated
# into the rest of PUDL: nothing downstream depends on them. Give every asset in
# such a dataset a low scheduling priority so the whole chain (extract -> _core ->
# core -> out) acts as late-DAG filler -- backfilling idle executor slots during
# the serial tail of the run instead of contending for CPU with the critical path
# at startup. `dagster/priority` is a soft tiebreaker for the ready-step queue,
# not a concurrency cap, so a deprioritized asset still runs early whenever a slot
# would otherwise sit idle.
ISLAND_OP_TAGS = {"dagster/priority": -10}
