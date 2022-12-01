"""Validate database integrity checks."""
from dagster import build_init_resource_context

from pudl.io_managers import pudl_sqlite_io_manager


def test_foreign_key_constraints():
    """Check foreign key constraints of current database."""
    # TODO (bendnorman): Replace this with a fixture like the live_dbs fixtures
    init_context = build_init_resource_context()
    manager = pudl_sqlite_io_manager(init_context)
    manager.check_foreign_keys()
