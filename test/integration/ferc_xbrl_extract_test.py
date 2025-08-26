"""PyTest based testing of the FERC DBF Extraction logic."""

import logging
from itertools import chain
from typing import Any

import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.etl import defs
from pudl.extract.ferc1 import TABLE_NAME_MAP_FERC1
from pudl.transform.ferc import filter_for_freshest_data_xbrl, get_primary_key_raw_xbrl

logger = logging.getLogger(__name__)


@pytest.mark.order(1)
def test_ferc1_xbrl2sqlite(ferc1_engine_xbrl: sa.Engine, ferc1_xbrl_taxonomy_metadata):
    """Attempt to access the XBRL based FERC 1 SQLite DB & XBRL taxonomy metadata.

    We're testing both the SQLite & JSON taxonomy here because they are generated
    together by the FERC 1 XBRL ETL.

    This test is marked with order(1) to ensure that it is explicitly run before the
    main PUDL ETL test, and is the first attempt to make use of the conceptually related
    FERC Form 1 XBRL DB engine & taxonomy fixtures. This means that if they fail, the
    failure will be more clearly associated with the fixture, and not some random
    downstream test that just happened to run first.
    """
    # Does the database exist, and contain a table we expect it to contain?
    assert isinstance(ferc1_engine_xbrl, sa.Engine)
    assert (
        "identification_001_duration" in sa.inspect(ferc1_engine_xbrl).get_table_names()
    )

    # Has the metadata we've read in from JSON contain a long list of entities?
    assert isinstance(ferc1_xbrl_taxonomy_metadata, dict)
    assert "core_ferc1__yearly_steam_plants_sched402" in ferc1_xbrl_taxonomy_metadata
    assert len(ferc1_xbrl_taxonomy_metadata) > 10
    assert len(ferc1_xbrl_taxonomy_metadata) < 100

    # Can we normalize that list of entities and find data in it that we expect?
    df = pd.json_normalize(
        ferc1_xbrl_taxonomy_metadata["core_ferc1__yearly_plant_in_service_sched204"][
            "instant"
        ]
    )
    assert (
        df.loc[
            df.name == "reactor_plant_equipment_nuclear_production", "balance"
        ].to_numpy()
        == "debit"
    )
    assert (
        df.loc[
            df.name == "reactor_plant_equipment_nuclear_production",
            "references.account",
        ].to_numpy()
        == "322"
    )


@pytest.mark.order(1)
def test_ferc714_xbrl2sqlite(
    ferc714_engine_xbrl: sa.Engine, ferc714_xbrl_taxonomy_metadata: dict[str, Any]
):
    """Attempt to access the XBRL based FERC 714 SQLite DB & XBRL taxonomy metadata.

    This test is marked with order(1) to ensure that it is explicitly run before the
    main PUDL ETL test, and is the first attempt to make use of the conceptually related
    FERC-714 XBRL DB engine & taxonomy fixtures. This means that if they fail, the
    failure will be more clearly associated with the fixture, and not some random
    downstream test that just happened to run first.
    """
    assert isinstance(ferc714_engine_xbrl, sa.Engine)
    assert (
        "identification_and_certification_01_1_duration"
        in sa.inspect(ferc714_engine_xbrl).get_table_names()
    )

    assert isinstance(ferc714_xbrl_taxonomy_metadata, dict)
    assert "core_ferc714__hourly_planning_area_demand" in ferc714_xbrl_taxonomy_metadata
    assert len(ferc714_xbrl_taxonomy_metadata) > 1
    assert len(ferc714_xbrl_taxonomy_metadata) < 10


@pytest.mark.parametrize(
    "table_name",
    [  # some sample wide guys
        "core_ferc1__yearly_sales_by_rate_schedules_sched304",
        "core_ferc1__yearly_pumped_storage_plants_sched408",
        "core_ferc1__yearly_steam_plants_sched402",
        "core_ferc1__yearly_hydroelectric_plants_sched406",
        "core_ferc1__yearly_transmission_lines_sched422",
        # some sample guys found to have higher filtering diffs
        "core_ferc1__yearly_utility_plant_summary_sched200",
        "core_ferc1__yearly_plant_in_service_sched204",
        "core_ferc1__yearly_operating_expenses_sched320",
        "core_ferc1__yearly_income_statements_sched114",
    ],
)
def test_filter_for_freshest_data(ferc1_engine_xbrl: sa.Engine, table_name: str):
    """Test if we are unexpectedly replacing records during filter_for_freshest_data."""

    raw_table_names = TABLE_NAME_MAP_FERC1[table_name]["xbrl"]
    # sometimes there are many raw tables that go into one
    # core table, but usually its a string.
    if isinstance(raw_table_names, str):
        raw_table_names = [raw_table_names]
    xbrls_with_periods = chain.from_iterable(
        (f"raw_ferc1_xbrl__{tn}_instant", f"raw_ferc1_xbrl__{tn}_duration")
        for tn in raw_table_names
    )
    for raw_table_name in xbrls_with_periods:
        logger.info(f"Checking if our filtering methodology works for {raw_table_name}")
        xbrl_table: pd.DataFrame = defs.load_asset_value(raw_table_name)
        if not xbrl_table.empty:
            primary_keys = get_primary_key_raw_xbrl(
                raw_table_name.removeprefix("raw_ferc1_xbrl__"), "ferc1"
            )
            filter_for_freshest_data_xbrl(
                xbrl_table, primary_keys, compare_methods=True
            )
