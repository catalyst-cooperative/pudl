import dagster as dg
import pytest

from pudl.dbt_wrapper import dagster_to_dbt_selection


@pytest.fixture
def dummy_dagster():
    @dg.asset
    def dna():
        return "dna"

    @dg.asset
    def cell(dna):
        return "cell"

    @dg.asset
    def animal(cell):
        return "animal"

    defs = dg.Definitions(assets=[dna, cell, animal])
    return defs


@pytest.fixture
def dummy_dbt():
    # make some yamls
    # write them to a temp dir
    #
    # run dbt parse
    # grab the manifest
    return None


@pytest.mark.parametrize(
    "dagster_selection,dbt_selection",
    [
        (
            "key:out_eia__yearly_generators",
            "source:pudl_dbt.pudl.out_eia__yearly_generators",
        )
    ],
)
@pytest.mark.xfail
def test_dagster_to_dbt_selection(dagster_selection, dbt_selection, dummy_dagster):
    observed = dagster_to_dbt_selection(
        dagster_selection, defs=dummy_dagster, manifest=dummy_dbt
    )
    expected = dbt_selection
    assert observed == expected
