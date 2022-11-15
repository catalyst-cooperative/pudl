"""Unit tests specific to the FERC Form 1 table transformations."""

import pytest

from pudl.transform.ferc1 import fill_dbf_to_xbrl_map, read_dbf_to_xbrl_map


@pytest.mark.parametrize(
    "dbf_table_name",
    [
        "f1_plant_in_srvce",
        "f1_elctrc_erg_acct",
    ],
)
def test_dbf_to_xbrl_map(dbf_table_name):
    """Test our DBF to XBRL row alignment tools & manually compiled mapping."""
    dbf_xbrl_map = fill_dbf_to_xbrl_map(
        read_dbf_to_xbrl_map(dbf_table_name=dbf_table_name)
    )
    dbf_to_xbrl_mapping_is_unique = (
        dbf_xbrl_map.groupby(["report_year", "xbrl_column_stem"])[
            "row_number"
        ].nunique()
        <= 1
    ).any()

    assert dbf_to_xbrl_mapping_is_unique  # nosec: B101
