"""Integration tests for settings that require real datastore configuration."""

from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


def test_partitions_for_datasource_table(pudl_etl_settings) -> None:
    """Test that a Datastore with *real local cache dir* generates a non-empty
    datasources table.

    NOTE (2026-04-03): Probably worth beefing up the assertions to confirm more
    of the metadata fallback behavior (i.e., look in local cache, *and* Zenodo,
    etc.). And probably worth testing out with populated & unpopulated local
    cache.
    """
    ds = Datastore(local_cache_path=PudlPaths().data_dir)
    datasource = pudl_etl_settings.make_datasources_table(ds)
    datasets = pudl_etl_settings.get_datasets().keys()
    if datasource.empty and datasets:
        raise AssertionError(
            "Datasource table is empty with the following datasets in the settings: "
            f"{datasets}"
        )
