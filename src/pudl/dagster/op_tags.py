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

# Assets on the "hot path": the transitive upstream inputs of the handful of
# assets that consistently determine the wall-clock runtime of the whole PUDL
# DAG (currently ``out_ferc714__hourly_planning_area_demand``,
# ``out_eia__monthly_generators`` and
# ``out_pudl__yearly_assn_eia_ferc1_plant_parts``). Give every asset on that path
# a high scheduling priority so the ready-step queue always favours it over
# unrelated work -- in particular over the deprioritised ISLAND_OP_TAGS chains and
# the default-priority datasets that are integrated but not on the critical path.
# Like ``dagster/priority`` generally this is a soft tiebreaker for the ready
# queue, not a concurrency reservation: a hot-path asset still yields its slot
# once it has run, and still queues behind hard limits such as the
# ``memory-use: high`` tag_concurrency_limit.
#
# Regenerate the hot-path membership with ``pixi run hot_path <asset>`` after
# integrating a new data year or otherwise reshaping the DAG.
HOT_PATH_OP_TAGS = {"dagster/priority": 10}
