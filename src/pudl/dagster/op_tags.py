"""Shared Dagster ``op_tags`` constants for PUDL assets.

Keep op-tag dictionaries that are applied across many asset modules here, so they have a
single canonical definition to import rather than being redefined in each module. This
is deliberately a dependency-free leaf module to avoid the risk of circular import when
an ``extract``/``transform`` module imports it back.

Scheduling priority
-------------------

The ``dagster/priority`` op tag is used as a *soft tiebreaker* when more steps are
runnable than there are executor slots. Higher values run first. It is not a concurrency
cap and does not preempt running work: a low-priority step still starts immediately
whenever a slot would otherwise sit idle, and any step still queues behind hard limits
such as the ``memory-use: high`` ``tag_concurrency_limit``.

PUDL uses three tiers:

* :data:`HOT_PATH_OP_TAGS` (``+10``) -- assets on the critical path.
* untagged (``0``) -- integrated datasets that are not on the critical path.
* :data:`COLD_PATH_OP_TAGS` (``-10``) -- isolated datasets with no downstream
  dependencies which can soak up idle CPU later in the run.

Prefer the named constants. Only reach for :func:`scheduling_priority` directly
when a single op genuinely needs a bespoke value (and add a comment saying why).
"""

PRIORITY_TAG = "dagster/priority"


def scheduling_priority(priority: int) -> dict[str, int]:
    """Build an ``op_tags`` dict that sets a step's ready-queue priority.

    Pass the result as ``op_tags=`` on ``@asset`` / ``@multi_asset`` (or ``tags=``
    on a bare ``@op``). Combine with other op tags using ``|``::

        @asset(op_tags={"memory-use": "high"} | scheduling_priority(10))
    """
    return {PRIORITY_TAG: priority}


# Assets on the "hot path" / critical path: the transitive upstream inputs of the
# handful of assets that consistently determine the runtime of the whole PUDL DAG
# (currently ``out_ferc714__hourly_planning_area_demand``,
# ``out_eia__monthly_generators`` and ``out_pudl__yearly_assn_eia_ferc1_plant_parts``).
# Give every asset on that path a high scheduling priority so the ready-step queue
# always favours it over lower priority work
#
# Regenerate the hot-path membership with ``pixi run hot_path <asset>`` after
# significantly reshaping the DAG.
HOT_PATH_OP_TAGS = scheduling_priority(10)

# Assets for datasets that are not yet deeply integrated into the rest of PUDL and have
# no downstrea dependencies. Give every asset in such a dataset a low scheduling
# priority so that they act as late-DAG filler -- taking up idle slots during the serial
# tail of the run instead of contending for CPU with the critical path at startup. A
# deprioritised asset still runs early whenever a slot would otherwise sit idle.
COLD_PATH_OP_TAGS = scheduling_priority(-10)
