"""Unit tests for the :mod:`pudl.transform.rus7` module."""

from pudl.metadata.enums import SERVICE_STATUS_RENAME_RUS7, SERVICE_STATUS_RUS7


def test_service_status_rename_matches_enum():
    """The rename map must be a bijection onto the ``service_status`` enum.

    ``_core_rus7__transmission_and_distribution`` maps the raw Form 7 Part B column
    suffixes through ``SERVICE_STATUS_RENAME_RUS7`` to produce the ``service_status``
    values, which are constrained by ``SERVICE_STATUS_RUS7``. If the two ever drift
    apart, every renamed value would silently fail the enum constraint, so guard the
    invariant directly.
    """
    # Every renamed value is a valid enum member, and every enum member is produced.
    assert sorted(SERVICE_STATUS_RENAME_RUS7.values()) == sorted(SERVICE_STATUS_RUS7)
    # The mapping has no collisions (distinct keys map to distinct values).
    assert len(set(SERVICE_STATUS_RENAME_RUS7.values())) == len(
        SERVICE_STATUS_RENAME_RUS7
    )
    # The raw suffixes the stacking regex matches are the map's keys.
    assert set(SERVICE_STATUS_RENAME_RUS7) == {"connected", "idle", "retired", "total"}
